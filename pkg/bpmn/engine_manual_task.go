package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// completeManualTask runs a manual task: work a person performs outside the
// engine. The engine does not wait for it, so the task is recorded in history
// and completed at once, without a job or a user task. Its input mappings are
// evaluated into the task's local scope and its output mappings propagate the
// mapped values to the parent scope, as for any other activity.
func (engine *Engine) completeManualTask(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TManualTask,
	currentToken runtime.ExecutionToken,
	variableHolder runtime.VariableHolder,
) (runtime.ActivityState, error) {
	flowElementInput := variableHolder.ExecutionScopeSnapshot()
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate input mappings of manual task %s: %w", element.GetId(), err)
	}

	// The input variables are saved explicitly because a multi-instance iteration
	// builds its output element from them; see flowNodeOwnsHistory.
	if err := batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
		ElementId:          element.GetId(),
		ElementType:        string(element.GetType()),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     flowElementInput,
	}); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to save history of manual task %s: %w", element.GetId(), err)
	}

	outputVariables, err := variableHolder.PropagateOnlyMappedOutputs(element.GetOutputMapping(), nil, engine.evaluateExpression)
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate output mappings of manual task %s: %w", element.GetId(), err)
	}

	if err := batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
		ElementId:          element.GetId(),
		ElementType:        string(element.GetType()),
		ExecutionTokenKey:  currentToken.Key,
		OutputVariables:    outputVariables,
		CompletedAt:        new(time.Now()),
	}); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to complete history of manual task %s: %w", element.GetId(), err)
	}

	return runtime.ActivityStateCompleted, nil
}
