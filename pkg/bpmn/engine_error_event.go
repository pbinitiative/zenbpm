package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func (engine *Engine) createErrorCatchEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, errorEventDefinition bpmn20.TErrorEventDefinition, element bpmn20.FlowNode, token runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	errorSubscription, err := engine.createErrorSubscription(instance, errorEventDefinition, element, token)

	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to create error subscription: %w", err)
	}

	err = batch.SaveErrorSubscription(ctx, errorSubscription)
	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to save new error subscription %+v: %w", errorSubscription, err)
	}

	token.State = runtime.TokenStateWaiting
	return token, nil
}

func (engine *Engine) createErrorSubscription(instance runtime.ProcessInstance, errorEventDefinition bpmn20.TErrorEventDefinition, element bpmn20.FlowNode, token runtime.ExecutionToken) (runtime.ErrorSubscription, error) {

	errorSubscription := runtime.ErrorSubscription{
		Key:                  engine.generateKey(),
		ElementId:            element.GetId(),
		ElementInstanceKey:   token.ElementInstanceKey,
		ProcessDefinitionKey: instance.ProcessInstance().Definition.Key,
		ProcessInstanceKey:   instance.ProcessInstance().GetInstanceKey(),
		ErrorCode:            errorEventDefinition.ErrorRef,
		State:                runtime.ErrorStateCreated,
		CreatedAt:            time.Now(),
		Token:                token,
	}

	return errorSubscription, nil
}

func (engine *Engine) findMatchingBoundaryErrorEvent(instance runtime.ProcessInstance, attachedToRef string, errorCode string) (*bpmn20.TBoundaryEvent, error) {

	definitions := instance.ProcessInstance().Definition.Definitions

	var catchAll *bpmn20.TBoundaryEvent

	for _, boundaryEvent := range definitions.Process.BoundaryEvent {
		if boundaryEvent.AttachedToRef != attachedToRef {
			continue
		}

		errorDefinition, ok := boundaryEvent.EventDefinition.(bpmn20.TErrorEventDefinition)
		if !ok {
			continue
		}

		if errorDefinition.ErrorRef == "" {
			catchAll = &boundaryEvent
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

	return catchAll, nil
}

func (engine *Engine) findMatchingParentCallActivityBoundaryErrorEvent(
	ctx context.Context,
	batch *EngineBatch,
	instance *runtime.CallActivityInstance,
	errorCode string,
) (runtime.ProcessInstance, runtime.ExecutionToken, *bpmn20.TBoundaryEvent, error) {
	parentProcessInstanceKey := instance.ParentProcessExecutionToken.ProcessInstanceKey
	parentTokenKey := instance.ParentProcessExecutionToken.Key

	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return nil, runtime.ExecutionToken{}, nil, fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	if err := batch.AddParentLockedInstance(ctx, parentInstance); err != nil {
		return nil, runtime.ExecutionToken{}, nil, err
	}
	if parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted || parentInstance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return parentInstance, runtime.ExecutionToken{}, nil, nil
	}

	parentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return nil, runtime.ExecutionToken{}, nil, fmt.Errorf("failed to get parent token by key %d: %w", parentTokenKey, err)
	}
	if parentToken.ElementInstanceKey != instance.ParentProcessTargetElementInstanceKey {
		return parentInstance, parentToken, nil, nil
	}

	boundaryEvent, err := engine.findMatchingBoundaryErrorEvent(parentInstance, parentToken.ElementId, errorCode)
	if err != nil {
		return nil, runtime.ExecutionToken{}, nil, err
	}
	return parentInstance, parentToken, boundaryEvent, nil
}

func (engine *Engine) handleParentCallActivityBoundaryError(
	ctx context.Context,
	batch *EngineBatch,
	childInstance *runtime.CallActivityInstance,
	parentInstance runtime.ProcessInstance,
	parentToken runtime.ExecutionToken,
	boundaryEvent *bpmn20.TBoundaryEvent,
) ([]runtime.ExecutionToken, error) {
	if err := engine.cancelBoundarySubscriptions(ctx, batch, parentInstance, &parentToken); err != nil {
		return nil, fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", parentInstance.ProcessInstance().Key, err)
	}

	if _, err := engine.handleProcessInstanceInnerCancel(ctx, childInstance, batch); err != nil {
		return nil, fmt.Errorf("failed to cancel child process instance %d: %w", childInstance.ProcessInstance().Key, err)
	}
	childInstance.ProcessInstance().State = runtime.ActivityStateTerminated
	if err := batch.SaveProcessInstance(ctx, childInstance); err != nil {
		return nil, fmt.Errorf("failed to save child process instance %d: %w", childInstance.ProcessInstance().Key, err)
	}

	tokens, err := engine.handleElementTransition(ctx, batch, parentInstance, boundaryEvent, parentToken)
	if err != nil {
		return nil, fmt.Errorf("failed to process boundary error transition %s: %w", boundaryEvent.GetId(), err)
	}
	for _, token := range tokens {
		if err := batch.SaveToken(ctx, token); err != nil {
			return nil, fmt.Errorf("failed to save parent token %d: %w", token.Key, err)
		}
	}
	if err := batch.SaveProcessInstance(ctx, parentInstance); err != nil {
		return nil, fmt.Errorf("failed to save parent process instance %d: %w", parentInstance.ProcessInstance().Key, err)
	}

	return tokens, nil
}

func (engine *Engine) handleBoundaryError(ctx context.Context, batch *EngineBatch, boundaryEvent *bpmn20.TBoundaryEvent, job runtime.Job, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	token := job.Token

	childInstances, err := engine.persistence.FindProcessInstancesByParentExecutionTokenKey(ctx, token.Key)
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

	tokens, err := engine.handleElementTransition(ctx, batch, instance, boundaryEvent, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process boundary error transition %s: %w", boundaryEvent.GetId(), err)
	}
	return tokens, nil
}
