package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

type boundaryErrorMatch struct {
	scope *errorEventContext
	event *bpmn20.TBoundaryEvent
}

func (engine *Engine) findMatchingBoundaryErrorEvent(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	token runtime.ExecutionToken,
	errorCode *string,
) (*boundaryErrorMatch, error) {
	currentErrorEventContext := &errorEventContext{
		instance:      instance,
		token:         token,
		attachedToRef: token.ElementId,
	}

	for currentErrorEventContext != nil {
		event := engine.findMatchingBoundaryErrorEventInCurrentProcessInstance(currentErrorEventContext.instance, currentErrorEventContext.attachedToRef, errorCode)
		if event != nil {
			return &boundaryErrorMatch{
				scope: currentErrorEventContext,
				event: event,
			}, nil
		}

		parentScope, err := engine.loadParentErrorEventContext(ctx, batch, currentErrorEventContext.instance, false)
		if err != nil {
			return nil, err
		}

		currentErrorEventContext = parentScope
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

func (engine *Engine) handleBoundaryError(
	ctx context.Context,
	batch *EngineBatch,
	currentInstance runtime.ProcessInstance,
	match *boundaryErrorMatch,
	cancelChildInstances bool,
) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	if match == nil || match.scope == nil || match.event == nil {
		return nil, nil, fmt.Errorf("boundary error match is required")
	}

	scope := match.scope

	if scope.instance.ProcessInstance().Key != currentInstance.ProcessInstance().Key {
		if err := batch.AddParentLockedInstance(ctx, scope.instance); err != nil {
			return nil, nil, err
		}
	}

	if err := engine.cancelBoundarySubscriptions(ctx, batch, scope.instance.ProcessInstance().Key, &scope.token); err != nil {
		return nil, nil, fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", scope.instance.ProcessInstance().Key, err)
	}

	if cancelChildInstances {
		if err := engine.cancelChildProcessInstancesForToken(ctx, batch, scope.token); err != nil {
			return nil, nil, err
		}
	}

	tokens, err := engine.handleElementTransition(ctx, batch, scope.instance, match.event, scope.token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to process boundary error transition %s: %w", match.event.GetId(), err)
	}
	return scope.instance, tokens, nil
}

func (engine *Engine) prepareBoundaryErrorTransition(
	ctx context.Context,
	batch *EngineBatch,
	currentInstance runtime.ProcessInstance,
	match *boundaryErrorMatch,
	variables map[string]interface{},
	cancelChildInstances bool,
) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	boundaryInstance, tokens, err := engine.handleBoundaryError(ctx, batch, currentInstance, match, cancelChildInstances)
	if err != nil {
		return nil, nil, err
	}

	variableHolder := runtime.NewVariableHolder(&boundaryInstance.ProcessInstance().VariableHolder, nil)
	_, err = variableHolder.PropagateOutputVariablesToParent(match.event.GetOutputMapping(), variables, engine.evaluateExpression)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to propagate boundary error variables to process instance %d: %w", boundaryInstance.ProcessInstance().Key, err)
	}

	if err := batch.SaveProcessInstance(ctx, boundaryInstance); err != nil {
		return nil, nil, fmt.Errorf("failed to save process instance %d after boundary error handling: %w", boundaryInstance.ProcessInstance().Key, err)
	}

	for _, token := range tokens {
		if err := batch.SaveToken(ctx, token); err != nil {
			return nil, nil, err
		}
	}

	return boundaryInstance, tokens, nil
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
