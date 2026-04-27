package bpmn

import (
	"context"
	"errors"
	"fmt"
)

// RegisterForPotentialTimerStartEvents registers timers for all TimerStartEvents of a process definition.
// Those timers would fire the creation of process instance
func (engine *Engine) RegisterForPotentialTimerStartEvents(ctx context.Context, processDefinitionKey int64) error {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", processDefinitionKey), err)
	}

	batch := engine.persistence.NewBatch()
	err = engine.createTimerStartEventsTimers(ctx, batch, processDefinition.Definitions.Process, processDefinitionKey, nil)
	if err != nil {
		return fmt.Errorf("failed to create timers for timer start events of process definition %d: %w", processDefinitionKey, err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush batch for process definition %d: %w", processDefinitionKey, err)
	}
	return nil
}
