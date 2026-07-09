package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) handleParallelGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TParallelGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	incoming := element.GetIncomingAssociation()
	instanceTokens, err := engine.persistence.GetAllTokensForProcessInstance(ctx, instance.ProcessInstance().Key)

	if err != nil {
		return nil, fmt.Errorf("failed to get current tokens for process instance: %w", err)
	}

	gatewayTokens := []runtime.ExecutionToken{}
	for _, token := range instanceTokens {
		if token.ElementId == currentToken.ElementId {
			gatewayTokens = append(gatewayTokens, token)
		}
	}
	waitingGatewayTokens := []runtime.ExecutionToken{}
	for _, token := range gatewayTokens {
		if token.State == runtime.TokenStateWaiting {
			waitingGatewayTokens = append(waitingGatewayTokens, token)
		}
	}
	currentToken.State = runtime.TokenStateWaiting
	if len(waitingGatewayTokens) != len(incoming)-1 {
		return []runtime.ExecutionToken{currentToken}, nil
	}

	outgoing := element.GetOutgoingAssociation()
	resTokens := make([]runtime.ExecutionToken, 0, len(waitingGatewayTokens)+len(outgoing)+1)
	for _, token := range waitingGatewayTokens {
		token.State = runtime.TokenStateCompleted
		resTokens = append(resTokens, token)
	}
	currentToken.State = runtime.TokenStateCompleted
	resTokens = append(resTokens, currentToken)

	for _, flow := range outgoing {
		tokenKey := engine.generateKey()
		sequenceFlowInstanceKey := engine.generateKey()
		targetElementInstanceKey := engine.generateKey()
		newToken := runtime.ExecutionToken{
			Key:                tokenKey,
			ElementInstanceKey: targetElementInstanceKey,
			ElementId:          flow.GetTargetRef().GetId(),
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
		err := batch.SaveFlowElementInstance(ctx,
			runtime.FlowElementInstance{
				Key:                sequenceFlowInstanceKey,
				ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
				ElementId:          flow.GetId(),
				ElementType:        string(bpmn20.ElementTypeSequenceFlow),
				CreatedAt:          time.Now(),
				ExecutionTokenKey:  newToken.Key,
			},
		)
		if err != nil {
			return nil, err
		}
		resTokens = append(resTokens, newToken)
	}

	return resTokens, nil
}
