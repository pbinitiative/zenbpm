package bpmn

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type EngineBatch struct {
	b                storage.Batch
	engine           *Engine
	touchedInstances []int64
	preFlushActions  []func() error
	postFlushActions []func()
}

// ErrInstanceAlreadyTerminal is returned by NewEngineBatch when the process instance being batched against
// has already reached a terminal state (Completed or Terminated). Callers that race against a concurrent
// cancellation/completion (e.g. multiple interrupting event-subprocess publishes on the same parent)
// can detect this with errors.Is and treat the situation as a clean no-op.
var ErrInstanceAlreadyTerminal = fmt.Errorf("process instance is already in a terminal state")

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
		return EngineBatch{}, fmt.Errorf("process instance %d is already completed: %w", instance.ProcessInstance().Key, ErrInstanceAlreadyTerminal)
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
	return slices.Contains(b.touchedInstances, instanceKey)
}

// AddParentLockedInstance only refreshes the input instances. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddParentLockedInstance(ctx context.Context, parentInstance bpmnruntime.ProcessInstance) error {
	if b.hasLockedInstance(parentInstance.ProcessInstance().Key) {
		return markTechnicalFailure(b.engine.persistence.RefreshProcessInstance(ctx, parentInstance))
	}

	//This does the same thing as AddLockedInstance because I havent found better way yet
	//TODO: do this better
	err := b.engine.runningInstances.tryLockInstance(ctx, parentInstance.ProcessInstance().Key)
	if err != nil {
		return markTechnicalFailure(fmt.Errorf("failed locking parent instance %d: %w", parentInstance.ProcessInstance().Key, err))
	}
	err = b.engine.persistence.RefreshProcessInstance(ctx, parentInstance)
	if err != nil {
		b.engine.runningInstances.unlockInstance(parentInstance.ProcessInstance().Key)
		return markTechnicalFailure(fmt.Errorf("failed to find process instance %d: %w", parentInstance.ProcessInstance().Key, err))
	}
	b.touchedInstances = append(b.touchedInstances, parentInstance.ProcessInstance().Key)
	return nil
}

// AddLockedInstance only refreshes the input instance. State of tokens, job, variables has to be refreshed manually
func (b *EngineBatch) AddLockedInstance(ctx context.Context, instance bpmnruntime.ProcessInstance) error {
	if b.hasLockedInstance(instance.ProcessInstance().Key) {
		return markTechnicalFailure(b.engine.persistence.RefreshProcessInstance(ctx, instance))
	}

	b.engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
	err := b.engine.persistence.RefreshProcessInstance(ctx, instance)
	if err != nil {
		b.engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
		return markTechnicalFailure(fmt.Errorf("failed to find process instance %d: %w", instance.ProcessInstance().Key, err))
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

// discardWrites drops all buffered writes and pre/post flush actions while keeping the instance locks held by the batch.
// Incident paths use it either to replace partially prepared mutations or defensively ensure that only incident
// bookkeeping and trigger-consumption writes are flushed.
func (b *EngineBatch) discardWrites() {
	b.b = b.engine.persistence.NewBatch()
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){}
}

func (b *EngineBatch) WriteTokenIncident(ctx context.Context, token bpmnruntime.ExecutionToken, instance bpmnruntime.ProcessInstance, cause error) error {
	incidentBatch := b.engine.persistence.NewBatch()
	token.State = bpmnruntime.TokenStateFailed
	failedInstance, err := processInstanceWithState(instance, bpmnruntime.ActivityStateFailed)
	if err != nil {
		b.Clear(ctx)
		return err
	}
	if err := incidentBatch.SaveToken(ctx, token); err != nil {
		b.Clear(ctx)
		return fmt.Errorf("failed to queue failed token %d: %w", token.Key, err)
	}
	if err := incidentBatch.SaveProcessInstance(ctx, failedInstance); err != nil {
		b.Clear(ctx)
		return fmt.Errorf("failed to queue failed process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	incident := createNewIncidentFromToken(cause, token, b.engine)
	if err := incidentBatch.SaveIncident(ctx, incident); err != nil {
		b.Clear(ctx)
		return fmt.Errorf("failed to queue incident %d for token %d: %w", incident.Key, token.Key, err)
	}

	b.b = incidentBatch
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){func() {
		instance.ProcessInstance().State = bpmnruntime.ActivityStateFailed
		b.engine.recordIncidentMetric(ctx, incident)
	}}
	return nil
}

func processInstanceWithState(instance bpmnruntime.ProcessInstance, state bpmnruntime.ActivityState) (bpmnruntime.ProcessInstance, error) {
	var copied bpmnruntime.ProcessInstance
	switch instance := instance.(type) {
	case *bpmnruntime.DefaultProcessInstance:
		value := *instance
		copied = &value
	case *bpmnruntime.SubProcessInstance:
		value := *instance
		copied = &value
	case *bpmnruntime.CallActivityInstance:
		value := *instance
		copied = &value
	case *bpmnruntime.MultiInstanceInstance:
		value := *instance
		copied = &value
	default:
		return nil, fmt.Errorf("unsupported process instance type %T", instance)
	}
	copied.ProcessInstance().State = state
	return copied, nil
}

func (b *EngineBatch) writeAndFlushTokenIncident(ctx context.Context, token bpmnruntime.ExecutionToken, instance bpmnruntime.ProcessInstance, cause error) error {
	if err := b.WriteTokenIncident(ctx, token, instance, cause); err != nil {
		return err
	}
	return b.Flush(ctx)
}

// writeAndFlushTokenIncidentWithCounterInvalidation keeps the per-run counter cache aligned
// with persistence when WriteTokenIncident replaces and discards the token-processing batch.
// It is applied uniformly at every token-incident site for consistency: on paths that continue
// the run the invalidation is load-bearing (the next guard check re-reads storage); on paths
// that end the run immediately, it is a harmless no-op.
func (b *EngineBatch) writeAndFlushTokenIncidentWithCounterInvalidation(
	ctx context.Context,
	token bpmnruntime.ExecutionToken,
	instance bpmnruntime.ProcessInstance,
	cause error,
	runCount *flowNodeRunCount,
) error {
	runCount.cached = false
	return b.writeAndFlushTokenIncident(ctx, token, instance, cause)
}

func (b *EngineBatch) WriteMessageIncident(ctx context.Context, message bpmnruntime.MessageSubscription, instance bpmnruntime.ProcessInstance, err error) error {
	b.b = b.engine.persistence.NewBatch()
	b.preFlushActions = []func() error{}
	b.postFlushActions = []func(){}
	if saveErr := b.b.SaveMessageSubscription(ctx, message); saveErr != nil {
		return fmt.Errorf("failed to save message subscription for incident: %w", saveErr)
	}
	instance.ProcessInstance().State = bpmnruntime.ActivityStateFailed
	if saveErr := b.b.SaveProcessInstance(ctx, instance); saveErr != nil {
		return fmt.Errorf("failed to save process instance for incident: %w", saveErr)
	}
	var incident bpmnruntime.Incident
	if tokenSub, ok := message.(*bpmnruntime.TokenMessageSubscription); ok {
		tokenSub.Token.State = bpmnruntime.TokenStateFailed
		if saveErr := b.b.SaveToken(ctx, tokenSub.Token); saveErr != nil {
			return fmt.Errorf("failed to save token for message incident: %w", saveErr)
		}
		incident = createNewIncidentFromToken(err, tokenSub.Token, b.engine)
	} else {
		data := message.MessageSubscription()
		incident = bpmnruntime.Incident{
			Key:                b.engine.generateKey(),
			ElementId:          data.ElementId,
			ProcessInstanceKey: instance.ProcessInstance().Key,
			Message:            err.Error(),
			CreatedAt:          time.Now(),
		}
	}
	if saveErr := b.b.SaveIncident(ctx, incident); saveErr != nil {
		return fmt.Errorf("failed to save message incident: %w", saveErr)
	}
	b.postFlushActions = append(b.postFlushActions, func() {
		b.engine.recordIncidentMetric(ctx, incident)
	})
	return nil
}

func (b *EngineBatch) AddPreFlushAction(ctx context.Context, f func() error) {
	b.preFlushActions = append(b.preFlushActions, f)
}

func (b *EngineBatch) AddPostFlushAction(ctx context.Context, f func()) {
	b.postFlushActions = append(b.postFlushActions, f)
}

func (b *EngineBatch) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	return markTechnicalFailure(b.b.SaveProcessDefinition(ctx, definition))
}

func (b *EngineBatch) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	return markTechnicalFailure(b.b.SaveProcessInstance(ctx, processInstance))
}

func (b *EngineBatch) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	if err := b.b.SaveTimer(ctx, timer); err != nil {
		return markTechnicalFailure(err)
	}
	b.postFlushActions = append(b.postFlushActions, func() {
		b.engine.recordTimerMetric(ctx, timer)
	})
	return nil
}

func (b *EngineBatch) DeleteProcessDefinitionsTimers(ctx context.Context, processDefinitionKeys []int64) error {
	return markTechnicalFailure(b.b.DeleteProcessDefinitionsTimers(ctx, processDefinitionKeys))
}

func (b *EngineBatch) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	return markTechnicalFailure(b.b.SaveJob(ctx, job))
}

func (b *EngineBatch) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	return markTechnicalFailure(b.b.SaveMessageSubscription(ctx, subscription))
}

func (b *EngineBatch) DeleteProcessDefinitionsMessageSubscriptions(ctx context.Context, processDefinitionKeys []int64) error {
	return markTechnicalFailure(b.b.DeleteProcessDefinitionsMessageSubscriptions(ctx, processDefinitionKeys))
}

func (b *EngineBatch) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	return markTechnicalFailure(b.b.SaveToken(ctx, token))
}

func (b *EngineBatch) saveTokens(ctx context.Context, tokens []bpmnruntime.ExecutionToken) error {
	for _, token := range tokens {
		if err := b.b.SaveToken(ctx, token); err != nil {
			b.Clear(ctx)
			return fmt.Errorf("failed to save token %d: %w", token.Key, err)
		}
	}
	return nil
}

func (b *EngineBatch) SaveFlowElementInstance(ctx context.Context, historyItem bpmnruntime.FlowElementInstance) error {
	return markTechnicalFailure(b.b.SaveFlowElementInstance(ctx, historyItem))
}

func (b *EngineBatch) UpdateOutputFlowElementInstance(ctx context.Context, historyItem bpmnruntime.FlowElementInstance) error {
	return markTechnicalFailure(b.b.UpdateOutputFlowElementInstance(ctx, historyItem))
}

func (b *EngineBatch) CompleteFlowElementInstance(ctx context.Context, key int64, completedAt time.Time) error {
	return markTechnicalFailure(b.b.CompleteFlowElementInstance(ctx, key, completedAt))
}

func (b *EngineBatch) IncrementFlowNodeCount(ctx context.Context, processInstanceKey int64) error {
	return markTechnicalFailure(b.b.IncrementFlowNodeCount(ctx, processInstanceKey))
}

func (b *EngineBatch) ResetProcessInstanceFlowNodeCount(ctx context.Context, processInstanceKey int64) error {
	return markTechnicalFailure(b.b.ResetProcessInstanceFlowNodeCount(ctx, processInstanceKey))
}

func (b *EngineBatch) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	if err := b.b.SaveIncident(ctx, incident); err != nil {
		return markTechnicalFailure(err)
	}
	b.postFlushActions = append(b.postFlushActions, func() {
		b.engine.recordIncidentMetric(ctx, incident)
	})
	return nil
}

// recordIncidentMetric increments the incident engine metrics. Incidents with
// ResolvedAt set are counted as resolved, others as created.
func (engine *Engine) recordIncidentMetric(ctx context.Context, incident bpmnruntime.Incident) {
	if engine == nil || engine.metrics == nil {
		return
	}
	if incident.ResolvedAt != nil {
		if engine.metrics.IncidentsResolved != nil {
			engine.metrics.IncidentsResolved.Add(ctx, 1, metric.WithAttributes(
				attribute.String("element_id", incident.ElementId),
			))
		}
		return
	}
	if engine.metrics.IncidentsCreated == nil {
		return
	}
	engine.metrics.IncidentsCreated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("element_id", incident.ElementId),
	))
}

// recordTimerMetric increments timer engine metrics based on the state the
// timer is being persisted with. It is shared by EngineBatch and the raw
// storage.Batch code paths (definition-level timer start events) so every
// durable timer state transition is counted exactly once.
func (engine *Engine) recordTimerMetric(ctx context.Context, timer bpmnruntime.Timer) {
	if engine == nil || engine.metrics == nil {
		return
	}
	// NewMetrics may return a partially initialized struct on error, so each
	// instrument must be checked individually.
	switch timer.TimerState {
	case bpmnruntime.TimerStateCreated:
		if engine.metrics.TimersScheduled != nil {
			engine.metrics.TimersScheduled.Add(ctx, 1)
		}
	case bpmnruntime.TimerStateTriggered:
		if engine.metrics.TimersFired != nil {
			engine.metrics.TimersFired.Add(ctx, 1)
		}
	case bpmnruntime.TimerStateCancelled:
		if engine.metrics.TimersCancelled != nil {
			engine.metrics.TimersCancelled.Add(ctx, 1)
		}
	}
}

func (b *EngineBatch) SaveErrorSubscription(ctx context.Context, subscription bpmnruntime.ErrorSubscription) error {
	return markTechnicalFailure(b.b.SaveErrorSubscription(ctx, subscription))
}
