package bpmn

import (
	"context"
	"fmt"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"reflect"
	"runtime"
)

type EngineBatch struct {
	b                storage.Batch
	engine           *Engine
	touchedInstances []int64
	preFlushActions  []func() error
	postFlushActions []func()
}

// NewEngineBatch TODO: optimize usage of FindProcessInstanceByKey
// NewEngineBatch - Use this method only in public engine methods in _api files
func (e *Engine) NewEngineBatch(ctx context.Context, instance bpmnruntime.ProcessInstance) (EngineBatch, error) {
	e.runningInstances.lockInstance(instance.ProcessInstance().Key)
	instance, err := e.persistence.FindProcessInstanceByKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		e.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return EngineBatch{}, fmt.Errorf("failed refresh process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if instance.ProcessInstance().State == bpmnruntime.ActivityStateCompleted {
		e.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return EngineBatch{}, fmt.Errorf("process instance %d is already completed", instance.ProcessInstance().Key)
	}
	return EngineBatch{
		b:                e.persistence.NewBatch(),
		engine:           e,
		touchedInstances: []int64{instance.ProcessInstance().Key},
		postFlushActions: []func(){},
		preFlushActions:  []func() error{},
	}, nil
}

func (e *Engine) NewEngineBatchClean() (EngineBatch, error) {
	return EngineBatch{
		b:                e.persistence.NewBatch(),
		engine:           e,
		touchedInstances: []int64{},
		postFlushActions: []func(){},
		preFlushActions:  []func() error{},
	}, nil
}

// AddParentLockedInstance only refreshes the input instances. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddParentLockedInstance(ctx context.Context, currentInstance bpmnruntime.ProcessInstance, parentInstance bpmnruntime.ProcessInstance) error {
	//This does the same thing as AddLockedInstance because I havent found better way yet
	b.engine.runningInstances.lockInstance(parentInstance.ProcessInstance().Key)
	parentInstance, err := b.engine.persistence.FindProcessInstanceByKey(ctx, parentInstance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to find process instance %d: %w", parentInstance.ProcessInstance().Key, err)
	}
	if parentInstance.ProcessInstance().State == bpmnruntime.ActivityStateCompleted {
		b.engine.runningInstances.unlockInstance(parentInstance.ProcessInstance().Key)
		return fmt.Errorf("process instance %d is already completed", parentInstance.ProcessInstance().Key)
	}
	b.touchedInstances = append(b.touchedInstances, parentInstance.ProcessInstance().Key)
	return nil
}

// AddLockedInstance only refreshes the input instance. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddLockedInstance(ctx context.Context, instance bpmnruntime.ProcessInstance) error {
	b.engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
	instance, err := b.engine.persistence.FindProcessInstanceByKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to find process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if instance.ProcessInstance().State == bpmnruntime.ActivityStateCompleted {
		b.engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return fmt.Errorf("process instance %d is already completed", instance.ProcessInstance().Key)
	}
	b.touchedInstances = append(b.touchedInstances, instance.ProcessInstance().Key)
	return nil
}

// Flush - only use in methods that initialized EngineBatch
func (b *EngineBatch) Flush(ctx context.Context) (err error) {
	defer func() {
		for _, key := range b.touchedInstances {
			b.engine.runningInstances.unlockInstance(key)
		}
		if err == nil {
			for _, action := range b.postFlushActions {
				action()
			}
		}
	}()
	for _, action := range b.preFlushActions {
		err := action()
		if err != nil {
			funcName := runtime.FuncForPC(reflect.ValueOf(action).Pointer()).Name()
			return fmt.Errorf("failed pre-flush action %s: %w", funcName, err)
		}
	}
	err = b.b.Flush(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (b *EngineBatch) Clear(ctx context.Context) {
	for _, key := range b.touchedInstances {
		b.engine.runningInstances.unlockInstance(key)
	}
	b.b = b.engine.persistence.NewBatch()
	b.touchedInstances = []int64{}
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){}
}

func (b *EngineBatch) WriteTokenIncident(ctx context.Context, token bpmnruntime.ExecutionToken, instance bpmnruntime.ProcessInstance, err error) {
	b.b = b.engine.persistence.NewBatch()
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){}
	token.State = bpmnruntime.TokenStateFailed
	instance.ProcessInstance().State = bpmnruntime.ActivityStateFailed
	b.b.SaveToken(ctx, token)
	b.b.SaveProcessInstance(ctx, instance)
	b.b.SaveIncident(ctx, createNewIncidentFromToken(err, token, b.engine))
}

func (b *EngineBatch) WriteMessageIncident(ctx context.Context, message bpmnruntime.MessageSubscription, instance bpmnruntime.ProcessInstance, err error) {
	b.b = b.engine.persistence.NewBatch()
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){}
	b.b.SaveMessageSubscription(ctx, message)
	b.b.SaveProcessInstance(ctx, instance)
}

func (b *EngineBatch) AddPreFlushAction(ctx context.Context, f func() error) {
	b.preFlushActions = append(b.preFlushActions, f)
}

func (b *EngineBatch) AddPostFlushAction(ctx context.Context, f func()) {
	b.postFlushActions = append(b.postFlushActions, f)
}

func (b *EngineBatch) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	return b.b.SaveProcessDefinition(ctx, definition)
}

func (b *EngineBatch) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	return b.b.SaveProcessInstance(ctx, processInstance)
}

func (b *EngineBatch) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	return b.b.SaveTimer(ctx, timer)
}

func (b *EngineBatch) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	return b.b.SaveJob(ctx, job)
}

func (b *EngineBatch) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	return b.b.SaveMessageSubscription(ctx, subscription)
}

func (b *EngineBatch) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	return b.b.SaveToken(ctx, token)
}

func (b *EngineBatch) SaveFlowElementInstance(ctx context.Context, historyItem bpmnruntime.FlowElementInstance) error {
	return b.b.SaveFlowElementInstance(ctx, historyItem)
}

func (b *EngineBatch) UpdateOutputFlowElementInstance(ctx context.Context, historyItem bpmnruntime.FlowElementInstance) error {
	return b.b.UpdateOutputFlowElementInstance(ctx, historyItem)
}

func (b *EngineBatch) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	return b.b.SaveIncident(ctx, incident)
}
