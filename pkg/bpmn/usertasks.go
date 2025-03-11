package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (state *BpmnEngineState) handleUserTask(ctx context.Context, process *ProcessInfo, instance *processInstanceInfo, element bpmn20.TaskElement) *job {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	_, j := state.handleServiceTask(ctx, process, instance, element)
	return j
}
