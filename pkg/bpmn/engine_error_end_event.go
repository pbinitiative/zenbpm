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

	tokens, err := engine.handleProcessInstanceInnerCancel(ctx, instance, batch, currentToken.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to terminate error end event scope %s: %w", endEvent.Id, err)
	}
	currentToken.State = runtime.TokenStateCompleted
	tokens = append(tokens, currentToken)

	parentScope, err := engine.loadParentErrorEventContext(ctx, batch, instance, true)
	if err != nil {
		return nil, err
	}

	if parentScope == nil {
		return engine.failUnhandledEndError(ctx, batch, tokens, currentToken, instance, endEvent.Id, errorCode)
	}

	instance.ProcessInstance().State = runtime.ActivityStateTerminated
	return engine.propagateErrorToParentHierarchy(ctx, batch, tokens, parentScope, endEvent.Id, errorCode)
}

func (engine *Engine) findErrorCode(instance runtime.ProcessInstance, endEvent *bpmn20.TEndEvent) (*string, error) {
	for _, eventDefinition := range endEvent.EvenDefinitions {
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
	instance runtime.ProcessInstance,
	endEventId string,
	errorCode *string,
) ([]runtime.ExecutionToken, error) {
	if err := engine.markTokenAndInstanceFailed(ctx, batch, instance, &failToken, unhandledEndError(endEventId, errorCode)); err != nil {
		return nil, fmt.Errorf("failed to create incident for unhandled error end event %s: %w", endEventId, err)
	}
	tokens[len(tokens)-1] = failToken
	return tokens, nil
}

func (engine *Engine) propagateErrorToParentHierarchy(
	ctx context.Context,
	batch *EngineBatch,
	tokens []runtime.ExecutionToken,
	parentErrorEventContext *errorEventContext,
	endEventId string,
	errorCode *string,
) ([]runtime.ExecutionToken, error) {
	for parentErrorEventContext != nil {
		boundaryEvent := engine.findMatchingBoundaryErrorEventInCurrentProcessInstance(parentErrorEventContext.instance, parentErrorEventContext.attachedToRef, errorCode)
		if boundaryEvent != nil {
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
			if err := engine.failUnhandledEndErrorOnParent(ctx, batch, parentErrorEventContext, endEventId, errorCode); err != nil {
				return nil, err
			}
			return tokens, nil
		}

		if err := engine.terminateEndErrorPropagatingScope(ctx, batch, parentErrorEventContext.instance, parentErrorEventContext.token); err != nil {
			return nil, err
		}

		parentErrorEventContext = nextParentEventContext
	}

	return tokens, nil
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
	endEventId string,
	errorCode *string,
) error {
	if err := engine.cancelBoundarySubscriptions(ctx, batch, parentScope.instance.ProcessInstance().Key, &parentScope.token); err != nil {
		return fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", parentScope.instance.ProcessInstance().Key, err)
	}
	if err := engine.markTokenAndInstanceFailed(ctx, batch, parentScope.instance, &parentScope.token, unhandledEndError(endEventId, errorCode)); err != nil {
		return fmt.Errorf("failed to create incident for parent token %d: %w", parentScope.token.Key, err)
	}
	return nil
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
