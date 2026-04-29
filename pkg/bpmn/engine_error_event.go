package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

type boundaryErrorContext struct {
	instance      runtime.ProcessInstance
	token         runtime.ExecutionToken
	attachedToRef string
}

type boundaryErrorMatch struct {
	context *boundaryErrorContext
	event   *bpmn20.TBoundaryEvent
}

func (engine *Engine) findMatchingBoundaryErrorEvent(
	ctx context.Context,
	instance runtime.ProcessInstance,
	token runtime.ExecutionToken,
	errorCode *string,
) (*boundaryErrorMatch, error) {
	currentBoundaryErrorContext := &boundaryErrorContext{
		instance:      instance,
		token:         token,
		attachedToRef: token.ElementId,
	}

	for currentBoundaryErrorContext != nil {
		event := engine.findMatchingBoundaryErrorEventInCurrentProcessInstance(currentBoundaryErrorContext.instance, currentBoundaryErrorContext.attachedToRef, errorCode)
		if event != nil {
			return &boundaryErrorMatch{
				context: currentBoundaryErrorContext,
				event:   event,
			}, nil
		}

		parentContext, err := engine.getParentBoundaryErrorContext(ctx, currentBoundaryErrorContext.instance)
		if err != nil {
			return nil, err
		}

		currentBoundaryErrorContext = parentContext
	}

	return nil, nil
}

func (engine *Engine) findMatchingBoundaryErrorEventInCurrentProcessInstance(instance runtime.ProcessInstance, attachedToRef string, errorCode *string) *bpmn20.TBoundaryEvent {
	if !ownsBoundaryEventScope(instance) {
		return nil
	}

	definitions := &instance.ProcessInstance().Definition.Definitions
	processScope := findBoundaryErrorScopeForAttachedToRef(&definitions.Process, attachedToRef)
	if processScope == nil {
		return nil
	}

	return findMatchingBoundaryErrorEventInProcessScope(processScope, definitions, attachedToRef, errorCode)
}

// Reports whether the instance carries its own boundary
// event definitions. Multi-instance children delegate to the parent scope.
func ownsBoundaryEventScope(instance runtime.ProcessInstance) bool {
	_, isMultiInstanceChild := instance.(*runtime.MultiInstanceInstance)
	return !isMultiInstanceChild
}

func findBoundaryErrorScopeForAttachedToRef(process *bpmn20.TProcess, attachedToRef string) *bpmn20.TProcess {
	if containsDirectBoundaryTarget(process, attachedToRef) {
		return process
	}

	for i := range process.SubProcess {
		if processScope := findBoundaryErrorScopeForAttachedToRef(&process.SubProcess[i].TProcess, attachedToRef); processScope != nil {
			return processScope
		}
	}

	return nil
}

func findMatchingBoundaryErrorEventInProcessScope(process *bpmn20.TProcess, definitions *bpmn20.TDefinitions, attachedToRef string, errorCode *string) *bpmn20.TBoundaryEvent {
	var catchAllBoundary *bpmn20.TBoundaryEvent

	for i := range process.BoundaryEvent {
		boundaryEvent := &process.BoundaryEvent[i]
		if boundaryEvent.AttachedToRef != attachedToRef {
			continue
		}

		errorDefinition, ok := boundaryEvent.EventDefinition.(bpmn20.TErrorEventDefinition)
		if !ok {
			continue
		}

		if errorDefinition.ErrorRef == nil {
			if catchAllBoundary == nil {
				catchAllBoundary = boundaryEvent
			}
			continue
		}

		bpmnError, err := definitions.GetErrorByRef(*errorDefinition.ErrorRef)
		if err != nil {
			return nil
		}

		if errorCode != nil && bpmnError.ErrorCode == *errorCode {
			return boundaryEvent
		}
	}

	return catchAllBoundary
}

func containsDirectBoundaryTarget(process *bpmn20.TProcess, elementId string) bool {
	for i := range process.ServiceTasks {
		if process.ServiceTasks[i].GetId() == elementId {
			return true
		}
	}
	for i := range process.UserTasks {
		if process.UserTasks[i].GetId() == elementId {
			return true
		}
	}
	for i := range process.BusinessRuleTask {
		if process.BusinessRuleTask[i].GetId() == elementId {
			return true
		}
	}
	for i := range process.SendTask {
		if process.SendTask[i].GetId() == elementId {
			return true
		}
	}
	for i := range process.CallActivity {
		if process.CallActivity[i].GetId() == elementId {
			return true
		}
	}
	for i := range process.SubProcess {
		if process.SubProcess[i].GetId() == elementId {
			return true
		}
	}

	return false
}

func (engine *Engine) getParentBoundaryErrorContext(ctx context.Context, instance runtime.ProcessInstance) (*boundaryErrorContext, error) {
	parentProcessInstanceKey := instance.GetParentProcessInstanceKey()
	if parentProcessInstanceKey == nil {
		return nil, nil
	}

	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, *parentProcessInstanceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find parent process instance %d: %w", *parentProcessInstanceKey, err)
	}
	if parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted || parentInstance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return nil, nil
	}

	var (
		parentTokenKey             int64
		parentAttachedToRef        string
		expectedElementInstanceKey int64
	)

	switch inst := instance.(type) {
	case *runtime.CallActivityInstance:
		parentTokenKey = inst.ParentProcessExecutionToken.Key
		parentAttachedToRef = inst.ParentProcessExecutionToken.ElementId
		expectedElementInstanceKey = inst.ParentProcessTargetElementInstanceKey
	case *runtime.SubProcessInstance:
		parentTokenKey = inst.ParentProcessExecutionToken.Key
		parentAttachedToRef = inst.ParentProcessTargetElementId
		expectedElementInstanceKey = inst.ParentProcessTargetElementInstanceKey
	case *runtime.MultiInstanceInstance:
		parentTokenKey = inst.ParentProcessExecutionToken.Key
		parentAttachedToRef = inst.ParentProcessTargetElementId
		expectedElementInstanceKey = inst.ParentProcessTargetElementInstanceKey
	default:
		return nil, nil
	}

	parentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent token by key %d: %w", parentTokenKey, err)
	}
	if parentToken.ElementInstanceKey != expectedElementInstanceKey {
		return nil, nil
	}

	return &boundaryErrorContext{
		instance:      parentInstance,
		token:         parentToken,
		attachedToRef: parentAttachedToRef,
	}, nil
}

func (engine *Engine) handleBoundaryError(
	ctx context.Context,
	batch *EngineBatch,
	currentInstance runtime.ProcessInstance,
	match *boundaryErrorMatch,
) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	if match == nil || match.context == nil || match.event == nil {
		return nil, nil, fmt.Errorf("boundary error match is required")
	}

	contextToHandle := match.context

	if contextToHandle.instance.ProcessInstance().Key != currentInstance.ProcessInstance().Key {
		if err := batch.AddParentLockedInstance(ctx, contextToHandle.instance); err != nil {
			return nil, nil, err
		}
	}

	if err := engine.cancelBoundarySubscriptions(ctx, batch, contextToHandle.instance.ProcessInstance().Key, &contextToHandle.token); err != nil {
		return nil, nil, fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", contextToHandle.instance.ProcessInstance().Key, err)
	}

	if err := engine.cancelChildProcessInstancesForToken(ctx, batch, contextToHandle.token); err != nil {
		return nil, nil, err
	}

	tokens, err := engine.handleElementTransition(ctx, batch, contextToHandle.instance, match.event, contextToHandle.token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to process boundary error transition %s: %w", match.event.GetId(), err)
	}
	return contextToHandle.instance, tokens, nil
}

func (engine *Engine) cancelChildProcessInstancesForToken(
	ctx context.Context,
	batch *EngineBatch,
	parentToken runtime.ExecutionToken,
) error {
	childInstances, err := engine.persistence.FindProcessInstancesByParentExecutionTokenKey(ctx, parentToken.Key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to find child instances for token %d: %w", parentToken.Key, err)
	}

	for _, childInstance := range childInstances {
		if err := engine.cancelSubProcessInstance(ctx, childInstance, batch); err != nil {
			return fmt.Errorf("failed to cancel child instance for token %d: %w", parentToken.Key, err)
		}
	}

	return nil
}
