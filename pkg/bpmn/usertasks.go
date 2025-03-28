package bpmn

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (state *Engine) handleUserTask(ctx context.Context, process *runtime.ProcessDefinition, instance *processInstanceInfo, element bpmn20.TaskElement) *runtime.Job {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	_, j := state.handleServiceTask(ctx, process, instance, element)
	return j
}
