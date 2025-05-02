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

// ExecutorRegistry stores mappings of BPMN element types to executor constructors
var ExecutorRegistry = make(map[string]func(elementActivity) FlowNodeInstanceExecutor)

// RegisterExecutor registers an executor for a BPMN element type
func RegisterExecutor(elementType string, constructor func(elementActivity) FlowNodeInstanceExecutor) {
	ExecutorRegistry[elementType] = constructor
}

func init() {
	RegisterExecutor("bpmn20.TServiceTask", func(executedActivity elementActivity) FlowNodeInstanceExecutor {
		return &ServiceTaskExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
	})
	RegisterExecutor("bpmn20.TUserTask", func(executedActivity elementActivity) FlowNodeInstanceExecutor {
		return &UserTaskExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
	})
	RegisterExecutor("bpmn20.TIntermediateCatchEvent", func(executedActivity elementActivity) FlowNodeInstanceExecutor {
		return &IntermediateCatchEventExecutor{FlowNodeExecutor{flowNode: &executedActivity}}
	})
}

// GetExecutor retrieves the registered executor for a BPMN element type
func GetExecutorInstance(state *Engine, element bpmn20.FlowNode) (FlowNodeInstanceExecutor, error) {
	elementType := fmt.Sprintf("%T", element) // Get the actual type of the element // TODO: use ElementType
	if constructor, exists := ExecutorRegistry[elementType]; exists {
		elementActivity := elementActivity{
			key:     state.generateKey(),
			state:   runtime.ActivityStateReady,
			element: element,
		}

		executor := constructor(elementActivity)

		return executor, nil
	}
	return nil, fmt.Errorf("no executor registered for type: %s", elementType)
}
