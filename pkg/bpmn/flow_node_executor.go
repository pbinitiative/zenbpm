package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// FlowNodeExecutor defines the behavior of all BPMN executors
type FlowNodeInstanceExecutor interface {
	Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error)
}

func GetExecutorDefinition(elementType bpmn20.ElementType) func(elementActivity) FlowNodeInstanceExecutor {
	switch elementType {
	// Activities
	case bpmn20.ElementTypeServiceTask:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &ServiceTaskExecutor{ActivityExecutor{FlowNodeExecutor{flowNode: &executedActivity}}}
		}
	case bpmn20.ElementTypeUserTask:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &UserTaskExecutor{ActivityExecutor{FlowNodeExecutor{flowNode: &executedActivity}}}
		}
	// Events
	case bpmn20.ElementTypeStartEvent:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &StartEventExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeEndEvent:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &EndEventExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeIntermediateCatchEvent:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &IntermediateCatchEventExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeIntermediateThrowEvent:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &IntermediateThrowEventExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	// Gateways
	case bpmn20.ElementTypeParallelGateway:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &ParallelGatewayExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeExclusiveGateway:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &ExclusiveGatewayExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeInclusiveGateway:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &InclusiveGatewayExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	case bpmn20.ElementTypeEventBasedGateway:
		return func(executedActivity elementActivity) FlowNodeInstanceExecutor {
			return &EventBasedGatewayExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
		}
	default:
		return nil
	}
}

// GetExecutor retrieves the registered executor instance for a BPMN element type
func GetExecutorInstance(state *Engine, element bpmn20.FlowNode) (FlowNodeInstanceExecutor, error) {
	elementType := fmt.Sprintf("%T", element) // Get the actual type of the element // TODO: use ElementType
	element.GetType()

	constructor := GetExecutorDefinition(element.GetType())
	if constructor == nil {
		return nil, fmt.Errorf("no executor defined for type: %s", elementType)
	}

	elementActivity := elementActivity{
		key:     state.generateKey(),
		state:   runtime.ActivityStateReady,
		element: element,
	}

	executor := constructor(elementActivity)

	return executor, nil
}
