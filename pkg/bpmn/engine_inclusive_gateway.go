package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) handleInclusiveGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TInclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	incoming := element.GetIncomingAssociation()
	waitingGatewayTokens := []runtime.ExecutionToken{}

	if len(incoming) > 1 {
		instanceTokens, err := engine.persistence.GetAllTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
		if err != nil {
			return nil, fmt.Errorf("failed to get current tokens for process instance: %w", err)
		}
		for _, token := range instanceTokens {
			if token.ElementId == currentToken.ElementId && token.State == runtime.TokenStateWaiting {
				waitingGatewayTokens = append(waitingGatewayTokens, token)
			}
		}
	}

	if len(waitingGatewayTokens) == 0 {
		if err := batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          element.GetId(),
			ElementType:        string(element.GetType()),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
		}); err != nil {
			return nil, fmt.Errorf("failed to save inclusive gateway history %s: %w", element.GetId(), err)
		}
	} else {
		// All tokens consumed by one synchronization cycle share the first arrival's
		// flow-element instance. Re-entry creates a new cycle after these tokens complete.
		currentToken.ElementInstanceKey = waitingGatewayTokens[0].ElementInstanceKey
	}

	if len(incoming) > 1 {
		waitingForToken, err := engine.hasPendingTokenThatCanReachGateway(ctx, instance, element.GetId(), currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate pending tokens for InclusiveGateway: %w", err)
		}
		if waitingForToken {
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		}
	}

	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := engine.inclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from InclusiveGateway: %w", err)
	}

	resTokens := make([]runtime.ExecutionToken, 0, len(waitingGatewayTokens)+len(activatedFlows)+1)
	for _, token := range waitingGatewayTokens {
		token.State = runtime.TokenStateCompleted
		resTokens = append(resTokens, token)
	}
	currentToken.State = runtime.TokenStateCompleted
	resTokens = append(resTokens, currentToken)
	if err := engine.completeFlowElementInstance(ctx, batch, instance, element, currentToken); err != nil {
		return nil, fmt.Errorf("failed to complete inclusive gateway history %s: %w", element.GetId(), err)
	}

	for _, flow := range activatedFlows {
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
				CompletedAt:        new(time.Now()),
			},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to save flow element instance, token: %d, error: %w", newToken.Key, err)
		}
		resTokens = append(resTokens, newToken)
	}
	return resTokens, nil
}

func (engine *Engine) hasPendingTokenThatCanReachGateway(ctx context.Context, instance runtime.ProcessInstance, gatewayElementId string, currentToken runtime.ExecutionToken) (bool, error) {
	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return false, fmt.Errorf("failed to get active tokens for process instance: %w", err)
	}

	for _, token := range tokens {
		// Tokens already waiting at this join belong to the same synchronization
		// cycle. They must not make the current arrival wait for itself.
		if token.Key == currentToken.Key {
			continue
		}
		if token.ElementId == gatewayElementId && token.State == runtime.TokenStateWaiting {
			continue
		}

		activity, err := engine.getExecutionTokenActivity(ctx, instance, token)
		if err != nil {
			return false, fmt.Errorf("failed to resolve token %d activity: %w", token.Key, err)
		}
		if flowNodeCanReach(activity.Element(), gatewayElementId, map[string]struct{}{}) {
			return true, nil
		}
	}
	return false, nil
}

func flowNodeCanReach(source bpmn20.FlowNode, targetElementId string, visited map[string]struct{}) bool {
	if source.GetId() == targetElementId {
		return true
	}
	if _, ok := visited[source.GetId()]; ok {
		return false
	}
	visited[source.GetId()] = struct{}{}

	for _, flow := range source.GetOutgoingAssociation() {
		target := flow.GetTargetRef()
		if target == nil {
			continue
		}
		if flowNodeCanReach(target, targetElementId, visited) {
			return true
		}
	}
	return false
}
