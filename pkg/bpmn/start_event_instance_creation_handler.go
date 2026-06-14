package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// startEventInstanceCreationTrigger captures the per-trigger (message subscription or timer)
// variations of activating a process-definition-level start event that creates a brand-new
// process instance. The shared activation flow is implemented in
// (*Engine).handleStartEventInstanceCreation.
type startEventInstanceCreationTrigger struct {
	// triggerKey is the unique key of the subscription/timer that fired. It is used to
	// serialize concurrent activations of the same trigger via runningInstances.lockInstance.
	triggerKey int64
	// processDefinitionKey is the process definition that owns the start event.
	processDefinitionKey int64
	// elementId is the BPMN id of the start event that owns the trigger.
	elementId string
	// inputVariables are the variables passed to the newly created process instance
	// (e.g. message payload). For timers this is empty.
	inputVariables map[string]any
	// refresh re-reads the trigger state under the trigger lock and reports whether activation
	// should be skipped (because a concurrent publisher/timer-fire has already consumed it).
	// It must NOT mutate the batch.
	refresh func(ctx context.Context) (skip bool, err error)
	// markConsumed records the trigger as consumed (timer Triggered / subscription Completed)
	// on the batch.
	markConsumed func(ctx context.Context, batch storage.Batch) error
	// inFlightTriggeredTimerKey is set for timer triggers and identifies the timer whose Triggered
	// transition is being written by markConsumed on the same batch. It lets timeCycle renewal see
	// a consistent view of the cycle history even though the just-consumed timer is still persisted as Created.
	inFlightTriggeredTimerKey *int64
}

// handleStartEventInstanceCreation is the shared implementation for activating a
// process-definition-level start event (message or timer). It:
//  1. serializes concurrent fires of the same trigger,
//  2. re-validates the trigger via t.refresh (silently no-ops if already consumed),
//  3. marks the original trigger as consumed in a batch,
//  4. re-creates a fresh subscription/timer (Active/Created state) for the same start event so that the next event can fire another process instance,
//  5. flushes the batch (so that even if instance creation later fails the trigger is not re-fired and the renewal is persisted), and
//  6. creates the new process instance.
func (engine *Engine) handleStartEventInstanceCreation(ctx context.Context, t startEventInstanceCreationTrigger) error {
	engine.runningInstances.lockInstance(t.triggerKey)
	defer engine.runningInstances.unlockInstance(t.triggerKey)

	skip, err := t.refresh(ctx)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}

	batch := engine.persistence.NewBatch()

	// Mark the original trigger as consumed BEFORE attempting to create the process instance.
	// This prevents the same trigger from being processed twice if a concurrent
	// publisher races with us, or if instance creation later fails.
	if err := t.markConsumed(ctx, batch); err != nil {
		return err
	}

	// Look up the process definition + start event so we can renew the trigger. If either lookup fails we still
	// flush the consumption above so that the original trigger does not get re-fired forever.
	processDefinition, pdErr := engine.persistence.FindProcessDefinitionByKey(ctx, t.processDefinitionKey)
	var startEvent *bpmn20.TStartEvent
	if pdErr == nil {
		startEvent = findStartEventById(&processDefinition.Definitions.Process, t.elementId)
		if startEvent != nil {
			if renewErr := engine.renewStartEventTrigger(ctx, batch, processDefinition, *startEvent, t.inFlightTriggeredTimerKey); renewErr != nil {
				return renewErr
			}
		}
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush start event trigger consumption batch (trigger=%d, definition=%d): %w", t.triggerKey, t.processDefinitionKey, err)
	}

	// NOTE: from this point on the original trigger has been persisted as "consumed" and the renewed
	// trigger (if any) has been persisted as Active. If the lookups below or the instance creation
	// itself fail we return an error to the caller, but the consumption is intentionally not rolled
	// back — re-firing the trigger would risk duplicate process instances. Callers (REST/gRPC) will
	// surface the error to the client even though the subscription/timer state has advanced.
	if pdErr != nil {
		return errors.Join(newEngineErrorf("no process definition with key %d", t.processDefinitionKey), pdErr)
	}
	if startEvent == nil {
		return fmt.Errorf("failed to find start event %q on process definition %d", t.elementId, t.processDefinitionKey)
	}

	if _, err := engine.CreateInstanceWithStartingElements(ctx, t.processDefinitionKey, []string{t.elementId}, t.inputVariables, nil); err != nil {
		return fmt.Errorf("failed to create process instance for start event %s of definition %d: %w", t.elementId, t.processDefinitionKey, err)
	}

	return nil
}

// renewStartEventTrigger re-creates the trigger on the given start event so that subsequent firings
// can spawn additional process instances. The new trigger is persisted via the batch and is independent
// of the just-consumed one (different key, Active/Created state).
//
// Renewal semantics by event definition kind:
//   - Message: always re-created (start-event message subscriptions are inherently long-lived).
//   - Timer with timeCycle: re-armed via createTimerStartEventTimers; createCycleStartTimer is a no-op once the configured number of repetitions has been consumed.
//   - Timer with timeDate / timeDuration: NOT renewed — these are one-shot per BPMN semantics.
//   - Other event definitions (signal/error/...): not yet supported as definition-level start triggers.
//
// inFlightTriggeredTimerKey, if non-nil, identifies the timer whose Triggered transition is being written by markConsumed
// on the same batch. Cycle renewal compensates for the fact that this timer is still persisted as Created when stats are queried.
func (engine *Engine) renewStartEventTrigger(
	ctx context.Context,
	batch storage.Batch,
	processDefinition runtime.ProcessDefinition,
	startEvent bpmn20.TStartEvent,
	inFlightTriggeredTimerKey *int64,
) error {
	for _, eventDef := range startEvent.EventDefinitions {
		switch typed := eventDef.(type) {
		case bpmn20.TMessageEventDefinition:
			if err := engine.createMessageStartEventMessageSubscriptions(ctx, batch, eventDef, startEvent, processDefinition, nil); err != nil {
				return fmt.Errorf("failed to re-create message subscription for start event %s of definition %d: %w", startEvent.GetId(), processDefinition.Key, err)
			}
		case bpmn20.TTimerEventDefinition:
			if typed.TimeCycle == nil { // timeDate / timeDuration timers are one-shot per BPMN semantics and must NOT be renewed.
				continue
			}
			if err := engine.createTimerStartEventTimers(ctx, batch, eventDef, startEvent, processDefinition.Key, nil, inFlightTriggeredTimerKey); err != nil {
				return fmt.Errorf("failed to re-create cycle timer for start event %s of definition %d: %w", startEvent.GetId(), processDefinition.Key, err)
			}
		default:
			// Other event definitions (signal/error/...) are not supported as definition-level start-event triggers yet; nothing to renew.
		}
	}
	return nil
}

func findStartEventById(process *bpmn20.TProcess, id string) *bpmn20.TStartEvent {
	for i := range process.StartEvents {
		if process.StartEvents[i].GetId() == id {
			return &process.StartEvents[i]
		}
	}
	return nil
}

// publishMessageOnInstanceCreation activates a (definition-level) message subscription that creates a new
// process instance. It routes to the instantiating receive task path when the subscription belongs to a
// receive task marked with instantiate="true", otherwise it activates a message start event.
func (engine *Engine) publishMessageOnInstanceCreation(ctx context.Context, message *runtime.DefinitionMessageSubscription, variables map[string]any) error {
	if processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, message.ProcessDefinitionKey); err == nil {
		if receiveTask := findReceiveTaskById(&processDefinition.Definitions.Process, message.ElementId); receiveTask != nil && receiveTask.Instantiate {
			return engine.publishMessageOnReceiveTaskInstanceCreation(ctx, message, variables)
		}
	}

	// Refreshed subscription is captured here so markConsumed below can save the up-to-date pointer.
	current := message
	return engine.handleStartEventInstanceCreation(ctx, startEventInstanceCreationTrigger{
		triggerKey:           message.Key,
		processDefinitionKey: message.ProcessDefinitionKey,
		elementId:            message.ElementId,
		inputVariables:       variables,
		refresh: func(ctx context.Context) (bool, error) {
			// Re-fetch under the trigger lock to confirm we're the winner. If another publisher already
			// consumed this subscription it will be Completed / Terminated / not found, in which case we
			// silently no-op (BPMN allows at-least-once message delivery to be deduplicated).
			refreshed, err := engine.persistence.FindMessageSubscriptionByKey(ctx, message.Key, runtime.ActivityStateActive)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return true, nil
				}
				return false, errors.Join(newEngineErrorf("failed to find active message subscription: %d", message.Key), err)
			}
			defSub, ok := refreshed.(*runtime.DefinitionMessageSubscription)
			if !ok {
				return false, fmt.Errorf("expected DefinitionMessageSubscription for key %d, got %T", message.Key, refreshed)
			}
			current = defSub
			return false, nil
		},
		markConsumed: func(ctx context.Context, batch storage.Batch) error {
			current.State = runtime.ActivityStateCompleted
			if err := batch.SaveMessageSubscription(ctx, current); err != nil {
				return fmt.Errorf("failed to save message subscription %d: %w", current.Key, err)
			}
			return nil
		},
	})
}

// processTimerTriggerOnInstanceCreation activates a (definition-level) timer start event by consuming the given timer,
// registering a fresh timer for follow-up firings and creating a new process instance.
func (engine *Engine) processTimerTriggerOnInstanceCreation(ctx context.Context, timer runtime.Timer) (*runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	current := timer
	err := engine.handleStartEventInstanceCreation(ctx, startEventInstanceCreationTrigger{
		triggerKey:                timer.Key,
		processDefinitionKey:      timer.ProcessDefinitionKey,
		elementId:                 timer.ElementId,
		inputVariables:            map[string]any{},
		inFlightTriggeredTimerKey: &timer.Key,
		refresh: func(ctx context.Context) (bool, error) {
			refreshed, err := engine.persistence.GetTimer(ctx, timer.Key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return true, nil
				}
				return false, errors.Join(newEngineErrorf("failed to find timer %d", timer.Key), err)
			}
			switch refreshed.TimerState {
			case runtime.TimerStateTriggered, runtime.TimerStateCancelled:
				return true, nil
			}
			current = refreshed
			return false, nil
		},
		markConsumed: func(ctx context.Context, batch storage.Batch) error {
			current.TimerState = runtime.TimerStateTriggered
			if err := batch.SaveTimer(ctx, current); err != nil {
				return fmt.Errorf("failed to update timer state for timer %d: %w", current.Key, err)
			}
			return nil
		},
	})
	return nil, nil, err
}
