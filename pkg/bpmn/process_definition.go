package bpmn

import (
	"context"
	"errors"
	"fmt"
)

// RegisterProcessDefinitionSubscriptions registers: timer and message subscriptions of a process definition.
// Those subscriptions  would fire the creation of process instance
func (engine *Engine) RegisterProcessDefinitionSubscriptions(ctx context.Context, processDefinitionKey int64) error {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", processDefinitionKey), err)
	}

	batch := engine.persistence.NewBatch()
	err = engine.createStartEventSubscriptions(ctx, batch, processDefinition.Definitions.Process, processDefinition, nil)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions for start events of process definition %d: %w", processDefinitionKey, err)
	}

	// Register definition-level message subscriptions for instantiating receive tasks (instantiate="true").
	// Publishing the corresponding message (with a nil correlation key) creates a new process instance that
	// starts at the receive task, similar to a message start event.
	err = engine.createInstantiatingReceiveTaskSubscriptions(ctx, batch, processDefinition)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions for instantiating receive tasks of process definition %d: %w", processDefinitionKey, err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush batch for process definition %d: %w", processDefinitionKey, err)
	}
	return nil
}
