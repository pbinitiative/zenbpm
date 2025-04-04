package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) handleUserTask(ctx context.Context, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TaskElement) (*runtime.Job, error) {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	return engine.handleServiceTask(ctx, process, instance, element)
}
