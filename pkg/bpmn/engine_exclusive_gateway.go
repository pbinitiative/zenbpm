package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// handleExclusiveGateway handles Exclusive gateway behaviour
// A diverging Exclusive Gateway (Decision) is used to create alternative paths within a Process flow. This is basically
// the “diversion point in the road” for a Process. For a given instance of the Process, only one of the paths can be taken.
func (engine *Engine) handleExclusiveGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TExclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: handle incoming mapping
	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := engine.exclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.ProcessInstance().VariableHolder.LocalVariables())

	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from ExclusiveGateway: %w", err)
	}

	if len(activatedFlows) != 0 {
		err := batch.SaveFlowElementInstance(ctx,
			runtime.FlowElementInstance{
				Key:                engine.generateKey(),
				ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
				ElementId:          activatedFlows[0].GetId(),
				ElementType:        string(bpmn20.ElementTypeSequenceFlow),
				CreatedAt:          time.Now(),
				ExecutionTokenKey:  currentToken.Key,
				CompletedAt:        new(time.Now()),
			},
		)

		if err != nil {
			return nil, err
		}
		if err := engine.completeFlowElementInstance(ctx, batch, instance, element, currentToken); err != nil {
			return nil, fmt.Errorf("failed to complete exclusive gateway history %s: %w", element.GetId(), err)
		}

		currentToken.ElementId = activatedFlows[0].GetTargetRef().GetId()
		currentToken.ElementInstanceKey = engine.generateKey()
	}
	return []runtime.ExecutionToken{currentToken}, nil
}
