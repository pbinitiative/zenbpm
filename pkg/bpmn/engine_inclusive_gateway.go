package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) handleInclusiveGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TInclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {

	if len(element.GetIncomingAssociation()) > 1 {
		currentToken.State = runtime.TokenStateCompleted

		waitingForToken, err := engine.hasPendingTokenThatCanReachGateway(ctx, instance, element.GetId(), currentToken)

		if err != nil {
			return nil, fmt.Errorf("failed to evaluate pending tokens for InclusiveGateway: %w", err)
		}

		if waitingForToken {
			return []runtime.ExecutionToken{currentToken}, nil
		}
	}

	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := engine.inclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.ProcessInstance().VariableHolder.LocalVariables())

	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from InclusiveGateway: %w", err)
	}

	resTokens := make([]runtime.ExecutionToken, len(activatedFlows)+1)

	currentToken.State = runtime.TokenStateCompleted
	resTokens[0] = currentToken

	for i, flow := range activatedFlows {
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
			return nil, fmt.Errorf("failed to save flow element instance, token: %d, error: %w", newToken.Key, err)
		}
		resTokens[i+1] = newToken
	}
	return resTokens, nil
}

func (engine *Engine) hasPendingTokenThatCanReachGateway(ctx context.Context, instance runtime.ProcessInstance, gatewayElementId string, currentToken runtime.ExecutionToken) (bool, error) {
	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return false, fmt.Errorf("failed to get active tokens for process instance: %w", err)
	}

	for _, token := range tokens {
		if token.Key == currentToken.Key {
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
