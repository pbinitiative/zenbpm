package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func (engine *Engine) validateBoundaryErrorEvent(instance runtime.ProcessInstance, boundaryEvent bpmn20.TBoundaryEvent) error {

	errorDefinition, ok := boundaryEvent.EventDefinition.(bpmn20.TErrorEventDefinition)

	if !ok {
		return fmt.Errorf("boundary event %s is not an error boundary event", boundaryEvent.GetId())
	}

	if !boundaryEvent.CancellActivity {
		return fmt.Errorf("error boundary event %s must be interrupting", boundaryEvent.GetId())
	}

	if errorDefinition.ErrorRef == "" {
		return nil
	}

	_, err := instance.ProcessInstance().Definition.Definitions.GetErrorByRef(errorDefinition.ErrorRef)
	if err != nil {
		return fmt.Errorf("failed to resolve boundary error %s on element %s: %w", errorDefinition.ErrorRef, boundaryEvent.AttachedToRef, err)
	}
	return nil
}

func (engine *Engine) findMatchingBoundaryErrorEvent(instance runtime.ProcessInstance, attachedToRef string, errorCode string) (*bpmn20.TBoundaryEvent, error) {

	definitions := instance.ProcessInstance().Definition.Definitions

	for _, boundaryEvent := range definitions.Process.BoundaryEvent {
		if boundaryEvent.AttachedToRef != attachedToRef {
			continue
		}

		errorDefinition, ok := boundaryEvent.EventDefinition.(bpmn20.TErrorEventDefinition)
		if !ok {
			continue
		}

		bpmnError, err := definitions.GetErrorByRef(errorDefinition.ErrorRef)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve boundary error %s on element %s: %w", errorDefinition.ErrorRef, attachedToRef, err)
		}

		if bpmnError.ErrorCode == errorCode {
			return &boundaryEvent, nil
		}
	}

	return nil, nil
}

func (engine *Engine) handleBoundaryError(ctx context.Context, batch *EngineBatch, listener *bpmn20.TBoundaryEvent, job *runtime.Job, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	token := job.Token

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if _, err := variableHolder.PropagateOutputVariablesToParent(listener.Output, variables, engine.evaluateExpression); err != nil {
		return nil, fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return nil, fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	// BPMN error boundary events are interrupting, so the failing activity is terminated and its boundary path is taken.
	job.State = runtime.ActivityStateTerminated
	if err := batch.SaveJob(ctx, *job); err != nil {
		return nil, fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
	}

	childInstances, err := engine.persistence.FindProcessInstanceByParentExecutionTokenKey(ctx, token.Key)
	if !errors.Is(err, storage.ErrNotFound) || len(childInstances) > 0 {
		if err != nil {
			return nil, fmt.Errorf("failed to find child instances for token %d: %w", token.Key, err)
		}
		for _, childInstance := range childInstances {
			if err := engine.cancelSubProcessInstance(ctx, childInstance, batch); err != nil {
				return nil, fmt.Errorf("failed to cancel child instance for token %d: %w", token.Key, err)
			}
		}
	}

	if err := engine.cancelBoundarySubscriptions(ctx, batch, instance, &token); err != nil {
		return nil, fmt.Errorf("failed to cancel boundary subscriptions: %w", err)
	}

	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process boundary error transition %s: %w", listener.GetId(), err)
	}
	return tokens, nil
}
