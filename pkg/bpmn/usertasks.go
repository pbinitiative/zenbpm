package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) createUserTask(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element bpmn20.InternalTask, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	return engine.createInternalTask(ctx, batch, instance, element, currentToken)
}
