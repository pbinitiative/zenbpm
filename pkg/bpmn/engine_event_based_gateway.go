package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) handleEventBasedGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TEventBasedGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	outgoing := element.GetOutgoingAssociation()
	for _, flow := range outgoing {
		switch targetElem := flow.GetTargetRef().(type) {
		case *bpmn20.TIntermediateCatchEvent:
			switch eventDefinition := targetElem.EventDefinition.(type) {
			case bpmn20.TMessageEventDefinition:
				token, err := engine.createMessageCatchEvent(ctx, batch, instance, eventDefinition, targetElem, currentToken)
				if err != nil {
					return []runtime.ExecutionToken{token}, fmt.Errorf("failed to handle IntermediateCatchEvent: %w", err)
				}
				currentToken = token
			case bpmn20.TTimerEventDefinition:
				token, err := engine.createTimerCatchEvent(ctx, batch, instance, eventDefinition, targetElem, currentToken)
				if err != nil {
					return []runtime.ExecutionToken{token}, fmt.Errorf("failed to handle IntermediateCatchEvent: %w", err)
				}
				currentToken = token
			default:
				return []runtime.ExecutionToken{currentToken}, fmt.Errorf("unsupported event definition after EventBasedGateway: id=%q, type=%T", targetElem.GetId(), targetElem.EventDefinition)
			}
		default:
			return []runtime.ExecutionToken{currentToken}, fmt.Errorf("unsupported element after EventBasedGateway: id=%q, type=%T", flow.GetTargetRef().GetId(), flow.GetTargetRef())
		}
	}
	return []runtime.ExecutionToken{currentToken}, nil
}
