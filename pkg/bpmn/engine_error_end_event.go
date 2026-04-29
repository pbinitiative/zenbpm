package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) handleEndErrorEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, endEvent *bpmn20.TEndEvent, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	errorCode, err := engine.findErrorCode(instance, endEvent)
	if err != nil {
		return nil, err
	}

	parentScope, err := engine.loadParentErrorEventContext(ctx, batch, instance, true)
	if err != nil {
		return nil, err
	}

	currentToken.State = runtime.TokenStateCompleted
	tokens := []runtime.ExecutionToken{currentToken}

	if parentScope == nil {
		return engine.failUnhandledEndError(ctx, batch, tokens, currentToken, endEvent.Id, errorCode)
	}

	return engine.propagateErrorToParentHierarchy(ctx, batch, tokens, instance, currentToken, parentScope, endEvent.Id, errorCode)
}

func (engine *Engine) findErrorCode(instance runtime.ProcessInstance, endEvent *bpmn20.TEndEvent) (*string, error) {
	for _, eventDefinition := range endEvent.EventDefinitions {
		errorDefinition, ok := eventDefinition.(bpmn20.TErrorEventDefinition)
		if !ok {
			continue
		}
		if errorDefinition.ErrorRef == nil {
			return nil, nil
		}

		bpmnError, err := instance.ProcessInstance().Definition.Definitions.GetErrorByRef(*errorDefinition.ErrorRef)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve end error %s on event %s: %w", *errorDefinition.ErrorRef, endEvent.Id, err)
		}

		return &bpmnError.ErrorCode, nil
	}

	return nil, fmt.Errorf("end event %s has no error definition", endEvent.Id)
}

func (engine *Engine) failUnhandledEndError(
	ctx context.Context,
	batch *EngineBatch,
	tokens []runtime.ExecutionToken,
	failToken runtime.ExecutionToken,
	endEventId string,
	errorCode *string,
) ([]runtime.ExecutionToken, error) {

	failToken.State = runtime.TokenStateCompleted
	if err := batch.SaveToken(ctx, failToken); err != nil {
		return nil, err
	}

	incidentErr := unhandledEndError(endEventId, errorCode)
	incident := createNewIncidentFromToken(incidentErr, failToken, engine)

	if err := batch.SaveIncident(ctx, incident); err != nil {
		return nil, err
	}

	tokens[len(tokens)-1] = failToken
	return tokens, nil
}

func (engine *Engine) propagateErrorToParentHierarchy(
	ctx context.Context,
	batch *EngineBatch,
	tokens []runtime.ExecutionToken,
	errorSourceInstance runtime.ProcessInstance,
	errorSourceToken runtime.ExecutionToken,
	parentErrorEventContext *errorEventContext,
	endEventId string,
	errorCode *string,
) ([]runtime.ExecutionToken, error) {
	propagatingScopes := make([]*errorEventContext, 0)

	for parentErrorEventContext != nil {
		boundaryEvent := engine.findMatchingBoundaryErrorEventInCurrentProcessInstance(parentErrorEventContext.instance, parentErrorEventContext.attachedToRef, errorCode)
		if boundaryEvent != nil {
			var err error
			tokens, err = engine.terminateEndErrorThrowingScope(ctx, batch, errorSourceInstance, errorSourceToken, tokens, endEventId)
			if err != nil {
				return nil, err
			}
			for _, propagatingScope := range propagatingScopes {
				if err := engine.terminateEndErrorPropagatingScope(ctx, batch, propagatingScope.instance, propagatingScope.token); err != nil {
					return nil, err
				}
			}
			if err := engine.activateBoundaryErrorHandler(ctx, batch, parentErrorEventContext, boundaryEvent); err != nil {
				return nil, err
			}
			return tokens, nil
		}

		nextParentEventContext, err := engine.loadParentErrorEventContext(ctx, batch, parentErrorEventContext.instance, true)
		if err != nil {
			return nil, err
		}

		if nextParentEventContext == nil {
			tokens, err = engine.failUnhandledEndErrorOnParent(ctx, batch, parentErrorEventContext, tokens, endEventId, errorCode)
			if err != nil {
				return nil, err
			}
			return tokens, nil
		}

		propagatingScopes = append(propagatingScopes, parentErrorEventContext)
		parentErrorEventContext = nextParentEventContext
	}

	return tokens, nil
}

func (engine *Engine) terminateEndErrorThrowingScope(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	currentToken runtime.ExecutionToken,
	tokens []runtime.ExecutionToken,
	endEventId string,
) ([]runtime.ExecutionToken, error) {
	terminatedTokens, err := engine.handleProcessInstanceInnerCancel(ctx, instance, batch, currentToken.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to terminate error end event scope %s: %w", endEventId, err)
	}

	instance.ProcessInstance().State = runtime.ActivityStateTerminated
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return nil, fmt.Errorf("failed to save terminated error end event scope on process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	return append(terminatedTokens, tokens...), nil
}

func (engine *Engine) activateBoundaryErrorHandler(
	ctx context.Context,
	batch *EngineBatch,
	parentScope *errorEventContext,
	boundaryEvent *bpmn20.TBoundaryEvent,
) error {
	boundaryInstance, parentTokens, err := engine.prepareBoundaryErrorTransition(
		ctx,
		batch,
		parentScope.instance,
		&boundaryErrorMatch{
			scope: parentScope,
			event: boundaryEvent,
		},
		nil,
		false,
	)
	if err != nil {
		return err
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(engine.context, boundaryInstance, parentTokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance after error end event %d: %w", parentScope.instance.ProcessInstance().Key, err)
			}
		}()
	})

	return nil
}

func (engine *Engine) failUnhandledEndErrorOnParent(
	ctx context.Context,
	batch *EngineBatch,
	parentScope *errorEventContext,
	tokens []runtime.ExecutionToken,
	endEventId string,
	errorCode *string,
) ([]runtime.ExecutionToken, error) {
	if err := engine.cancelBoundarySubscriptions(ctx, batch, parentScope.instance.ProcessInstance().Key, &parentScope.token); err != nil {
		return tokens, fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", parentScope.instance.ProcessInstance().Key, err)
	}
	parentScope.token.State = runtime.TokenStateCompleted
	if err := batch.SaveToken(ctx, parentScope.token); err != nil {
		return tokens, fmt.Errorf("failed to save completed parent token %d: %w", parentScope.token.Key, err)
	}
	if err := batch.SaveIncident(ctx, createNewIncidentFromToken(unhandledEndError(endEventId, errorCode), parentScope.token, engine)); err != nil {
		return tokens, fmt.Errorf("failed to create incident for parent token %d: %w", parentScope.token.Key, err)
	}

	tokens = append(tokens, parentScope.token)
	return tokens, nil
}

func (engine *Engine) terminateEndErrorPropagatingScope(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, propagatingToken runtime.ExecutionToken) error {
	if _, err := engine.handleProcessInstanceInnerCancel(ctx, instance, batch, propagatingToken.Key); err != nil {
		return fmt.Errorf("failed to terminate propagating error scope on process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	propagatingToken.State = runtime.TokenStateCanceled
	if err := batch.SaveToken(ctx, propagatingToken); err != nil {
		return fmt.Errorf("failed to save canceled propagating token %d: %w", propagatingToken.Key, err)
	}

	instance.ProcessInstance().State = runtime.ActivityStateTerminated
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return fmt.Errorf("failed to save terminated propagating error scope on process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	return nil
}

func unhandledEndError(endEventId string, errorCode *string) error {
	if errorCode == nil {
		return fmt.Errorf("unhandled error end event %s", endEventId)
	}
	return fmt.Errorf("unhandled error end event %s: %s", endEventId, *errorCode)
}

func (engine *Engine) markTokenAndInstanceFailed(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	token *runtime.ExecutionToken,
	incidentErr error,
) error {
	token.State = runtime.TokenStateFailed
	if err := batch.SaveToken(ctx, *token); err != nil {
		return err
	}

	instance.ProcessInstance().State = runtime.ActivityStateFailed
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	if err := batch.SaveIncident(ctx, createNewIncidentFromToken(incidentErr, *token, engine)); err != nil {
		return err
	}

	return nil
}
