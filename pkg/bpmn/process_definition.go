package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// RegisterProcessDefinitionSubscriptions registers the definition-level subscriptions of a process definition:
// the timers and message subscriptions of its start events and instantiating receive tasks, which
// fire the creation of process instances.
//
// Only the latest version of a process owns those subscriptions: a
// definition that is not the latest version of its process (an allocation
// delivered after a newer one) is left without any. The registration is
// idempotent: a start element that already has a subscription, in whatever
// state, is not registered again, so a repeated deployment neither schedules
// a start event twice nor re-arms one that already fired.
func (engine *Engine) RegisterProcessDefinitionSubscriptions(ctx context.Context, processDefinitionKey int64) error {
	engine.definitionMu.Lock()
	defer engine.definitionMu.Unlock()
	return engine.registerProcessDefinitionSubscriptionsLocked(ctx, processDefinitionKey)
}

// registerProcessDefinitionSubscriptionsLocked is RegisterProcessDefinitionSubscriptions
// for callers that already hold definitionMu.
func (engine *Engine) registerProcessDefinitionSubscriptionsLocked(ctx context.Context, processDefinitionKey int64) error {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", processDefinitionKey), err)
	}
	latest, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processDefinition.BpmnProcessId)
	if err != nil {
		return fmt.Errorf("failed to find the latest version of process %s before registering subscriptions of definition %d: %w", processDefinition.BpmnProcessId, processDefinitionKey, err)
	}
	if latest.Key != processDefinitionKey {
		return nil
	}
	unregistered, err := engine.unregisteredStartElements(ctx, processDefinition)
	if err != nil {
		return err
	}

	batch := engine.persistence.NewBatch()
	err = engine.createStartEventSubscriptions(ctx, batch, unregistered.Definitions.Process, processDefinition, nil)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions for start events of process definition %d: %w", processDefinitionKey, err)
	}

	// Register definition-level message subscriptions for instantiating receive tasks (instantiate="true").
	// Publishing the corresponding message (with a nil correlation key) creates a new process instance that
	// starts at the receive task, similar to a message start event.
	err = engine.createInstantiatingReceiveTaskSubscriptions(ctx, batch, unregistered)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions for instantiating receive tasks of process definition %d: %w", processDefinitionKey, err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush batch for process definition %d: %w", processDefinitionKey, err)
	}
	return nil
}

// unregisteredStartElements returns a copy of the definition whose process keeps
// only the start events and instantiating receive tasks that have no definition-level subscription yet.
func (engine *Engine) unregisteredStartElements(ctx context.Context, definition runtime.ProcessDefinition) (runtime.ProcessDefinition, error) {
	pending := definition
	pending.Definitions.Process.StartEvents = nil
	for _, startEvent := range definition.Definitions.Process.StartEvents {
		registered, err := engine.startEventRegistered(ctx, definition, startEvent)
		if err != nil {
			return runtime.ProcessDefinition{}, err
		}
		if !registered {
			pending.Definitions.Process.StartEvents = append(pending.Definitions.Process.StartEvents, startEvent)
		}
	}
	pending.Definitions.Process.ReceiveTask = nil
	for _, receiveTask := range definition.Definitions.Process.ReceiveTask {
		if !receiveTask.Instantiate {
			continue
		}
		subscription, err := engine.newReceiveTaskDefinitionSubscription(definition, &receiveTask)
		if err != nil {
			return runtime.ProcessDefinition{}, err
		}
		registered, err := engine.definitionMessageSubscriptionExists(ctx, definition.Key, receiveTask.GetId(), subscription.Name)
		if err != nil {
			return runtime.ProcessDefinition{}, err
		}
		if !registered {
			pending.Definitions.Process.ReceiveTask = append(pending.Definitions.Process.ReceiveTask, receiveTask)
		}
	}
	return pending, nil
}

// startEventRegistered reports whether the start event already has a
// definition-level timer or message subscription, in any state.
func (engine *Engine) startEventRegistered(ctx context.Context, definition runtime.ProcessDefinition, startEvent bpmn20.TStartEvent) (bool, error) {
	for _, eventDefinition := range startEvent.EventDefinitions {
		switch eventDefinition := eventDefinition.(type) {
		case bpmn20.TTimerEventDefinition:
			for _, state := range []runtime.TimerState{runtime.TimerStateCreated, runtime.TimerStateTriggered, runtime.TimerStateCancelled} {
				timers, err := engine.persistence.FindProcessDefinitionTimersByElement(ctx, definition.Key, startEvent.GetId(), state)
				if err != nil {
					return false, fmt.Errorf("failed to look up timers of start event %s of definition %d: %w", startEvent.GetId(), definition.Key, err)
				}
				if len(timers) > 0 {
					return true, nil
				}
			}
		case bpmn20.TMessageEventDefinition:
			name, err := engine.getMessageName(definition, eventDefinition)
			if err != nil {
				return false, err
			}
			registered, err := engine.definitionMessageSubscriptionExists(ctx, definition.Key, startEvent.GetId(), name)
			if err != nil || registered {
				return registered, err
			}
		}
	}
	return false, nil
}

// definitionMessageSubscriptionExists reports whether a definition-level
// message subscription exists for the element, whether it is still armed or was already consumed.
func (engine *Engine) definitionMessageSubscriptionExists(ctx context.Context, definitionKey int64, elementId string, name string) (bool, error) {
	for _, state := range []runtime.ActivityState{runtime.ActivityStateActive, runtime.ActivityStateCompleted, runtime.ActivityStateTerminated} {
		_, err := engine.persistence.FindDefinitionMessageSubscription(ctx, definitionKey, elementId, name, state)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return false, fmt.Errorf("failed to look up definition message subscription of element %s of definition %d: %w", elementId, definitionKey, err)
		}
	}
	return false, nil
}
