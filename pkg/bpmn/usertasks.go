package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) handleUserTask(ctx context.Context, batch storage.Batch, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TaskElement) (*runtime.Job, error) {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	return engine.handleServiceTask(ctx, batch, process, instance, element)
}

type UserTaskExecutor struct {
	FlowNodeExecutor
}

func (e *UserTaskExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	e.FlowNodeExecutor.Execute(ctx, batch, engine, process, instance)
	element := e.flowNode.element
	taskElement := element.(bpmn20.TaskElement)
	act, err := engine.handleUserTask(ctx, batch, process, instance, taskElement)
	if err != nil {
		return false, nil, []command{}, err
	}
	return act.State == runtime.ActivityStateCompleted, act, []command{}, nil

}
