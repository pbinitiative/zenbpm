package bpmn

import (
	"context"
	"fmt"
	"reflect"
	"runtime"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
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
func (engine *Engine) NewEngineBatch(ctx context.Context, instance bpmnruntime.ProcessInstance) (EngineBatch, error) {
	engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
	err := engine.persistence.RefreshProcessInstance(ctx, instance)
	if err != nil {
		engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return EngineBatch{}, fmt.Errorf("failed refresh process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if instance.ProcessInstance().State == bpmnruntime.ActivityStateCompleted || instance.ProcessInstance().State == bpmnruntime.ActivityStateTerminated {
		engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return EngineBatch{}, fmt.Errorf("process instance %d is already completed", instance.ProcessInstance().Key)
	}
	return EngineBatch{
		b:                engine.persistence.NewBatch(),
		engine:           engine,
		touchedInstances: []int64{instance.ProcessInstance().Key},
		postFlushActions: []func(){},
		preFlushActions:  []func() error{},
	}, nil
}

func (engine *Engine) NewEngineBatchClean() (EngineBatch, error) {
	return EngineBatch{
		b:                engine.persistence.NewBatch(),
		engine:           engine,
		touchedInstances: []int64{},
		postFlushActions: []func(){},
		preFlushActions:  []func() error{},
	}, nil
}

func (b *EngineBatch) hasLockedInstance(instanceKey int64) bool {
	for _, touchedKey := range b.touchedInstances {
		if touchedKey == instanceKey {
			return true
		}
	}
	return false
}

// AddParentLockedInstance only refreshes the input instances. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddParentLockedInstance(ctx context.Context, parentInstance bpmnruntime.ProcessInstance) error {
	if b.hasLockedInstance(parentInstance.ProcessInstance().Key) {
		return b.engine.persistence.RefreshProcessInstance(ctx, parentInstance)
	}

	//This does the same thing as AddLockedInstance because I havent found better way yet
	//TODO: do this better
	err := b.engine.runningInstances.tryLockInstance(ctx, parentInstance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed locking parent instance %d: %w", parentInstance.ProcessInstance().Key, err)
	}
	err = b.engine.persistence.RefreshProcessInstance(ctx, parentInstance)
	if err != nil {
		b.engine.runningInstances.unlockInstance(parentInstance.ProcessInstance().Key)
		return fmt.Errorf("failed to find process instance %d: %w", parentInstance.ProcessInstance().Key, err)
	}
	b.touchedInstances = append(b.touchedInstances, parentInstance.ProcessInstance().Key)
	return nil
}

// AddLockedInstance only refreshes the input instance. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddLockedInstance(ctx context.Context, instance bpmnruntime.ProcessInstance) error {
	if b.hasLockedInstance(instance.ProcessInstance().Key) {
		return b.engine.persistence.RefreshProcessInstance(ctx, instance)
	}

	b.engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
	err := b.engine.persistence.RefreshProcessInstance(ctx, instance)
	if err != nil {
		b.engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return fmt.Errorf("failed to find process instance %d: %w", instance.ProcessInstance().Key, err)
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
		b.b = b.engine.persistence.NewBatch()
		b.touchedInstances = []int64{}
		b.preFlushActions = []func() error{}
		b.postFlushActions = []func(){}
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

func (b *EngineBatch) DeleteProcessDefinitionsTimers(ctx context.Context, processDefinitionKeys []int64) error {
	return b.b.DeleteProcessDefinitionsTimers(ctx, processDefinitionKeys)
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

func (b *EngineBatch) SaveErrorSubscription(ctx context.Context, subscription bpmnruntime.ErrorSubscription) error {
	return b.b.SaveErrorSubscription(ctx, subscription)
}
