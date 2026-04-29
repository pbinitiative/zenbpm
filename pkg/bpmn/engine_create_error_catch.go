package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
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
