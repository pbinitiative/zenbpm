package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// eventSubprocessTrigger captures the per-trigger (message subscription or timer) variations of activating an event subprocess.
// The shared activation flow is implemented in (*Engine).startEventSubprocess.
type eventSubprocessTrigger struct {
	// processInstanceKey is the parent process instance whose event subprocess should be activated.
	processInstanceKey int64
	// elementId is the BPMN id of the event subprocess start event.
	elementId string
	// inputVariables are the variables coming with the trigger (e.g. message payload). For timers this is empty.
	inputVariables map[string]any
	// safegoLabel is the label used for the post-flush goroutine that runs the newly created event subprocess instance.
	safegoLabel string
	// refresh re-reads the trigger state under the parent-instance lock and reports whether activation should be skipped
	// (because a concurrent activity already consumed/cancelled the trigger). It must NOT mutate the batch.
	refresh func(ctx context.Context) (skip bool, err error)
	// markConsumed records the trigger as consumed (timer triggered / subscription completed) on the batch.
	markConsumed func(ctx context.Context, batch *EngineBatch) error
}

// startEventSubprocess implements the activation logic shared by message and timer (and potentially future new) start event subprocesses.
// It locks the parent process instance, re-validates the trigger via t.refresh, optionally cancels the parent (when the start event is interrupting),
// propagates trigger variables, creates the event subprocess instance and schedules its execution.
func (engine *Engine) startEventSubprocess(ctx context.Context, t eventSubprocessTrigger) (retErr error) {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, t.processInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process instance with key: %d", t.processInstanceKey), err)
	}

	subProcessDef, startEventDef := instance.ProcessInstance().Definition.Definitions.Process.GetSubprocessAndStartEventById(t.elementId)
	if startEventDef == nil {
		return fmt.Errorf("failed to find a startEvent with id: %s", t.elementId)
	}
	if subProcessDef == nil {
		return fmt.Errorf("failed to find a subProcess for start element id: %s", t.elementId)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		// If the parent instance was already terminated/completed by a sibling interrupting event subprocess that won the race,
		// treat this trigger as a clean no-op rather than surfacing an error to the caller.
		if errors.Is(err, ErrInstanceAlreadyTerminal) {
			return nil
		}
		return fmt.Errorf("failed to create engine batch: %w", err)
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	// Re-fetch the trigger state now that the parent process instance is locked. When multiple interrupting
	// event-subprocess triggers fire concurrently, the first one to be processed cancels all sibling
	// triggers on the parent. Without this check the losing triggers would race for the parent-instance lock and
	// produce spurious errors and unnecessary work.
	skip, err := t.refresh(ctx)
	if err != nil {
		return err
	}
	if skip {
		batch.Clear(ctx)
		return nil
	}

	if startEventDef.IsInterrupting {
		_, err = engine.handleProcessInstanceInnerCancel(ctx, instance, &batch)
		if err != nil {
			return fmt.Errorf("failed to cancel parent instance %d for interrupting event subprocess: %w", t.processInstanceKey, err)
		}
	}

	// Propagate trigger variables to the parent process instance.
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	propagatedVariables, err := variableHolder.PropagateOutputVariablesToParent(startEventDef.GetOutputMapping(), t.inputVariables, engine.evaluateExpression)
	if err != nil {
		return fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	// Make sure the propagated variables are also visible inside the event subprocess itself.
	for k, v := range propagatedVariables {
		variableHolder.SetLocalVariable(k, v)
	}
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return fmt.Errorf("failed to save parent process instance %d after propagating trigger variables: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to get active tokens for process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if len(tokens) == 0 { // Parent process is not active anymore — nothing to start.
		batch.Clear(ctx)
		return nil
	}

	startingFlowNodes := []bpmn20.FlowNode{startEventDef}
	subProcessInstance, subTokens, err := engine.createInstanceWithStartingElements(
		ctx,
		&batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		variableHolder,
		&runtime.SubProcessInstance{
			ParentProcessExecutionToken:           tokens[0],
			ParentProcessTargetElementInstanceKey: tokens[0].ElementInstanceKey,
			// Reference to the subprocess definition needed for continuation.
			ParentProcessTargetElementId: subProcessDef.Id,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create event subprocess instance: %w", err)
	}

	// Create event sub process subscriptions for event subprocesses nested within this event sub process.
	if err := engine.createEventSubProcessSubscriptions(ctx, &batch, subProcessInstance, &subProcessDef.TFlowElementsContainer); err != nil {
		return fmt.Errorf("failed to create event subprocess subscriptions in sub process %s: %w", subProcessDef.Id, err)
	}

	if err := t.markConsumed(ctx, &batch); err != nil {
		return err
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go(t.safegoLabel, engine.logger, func() {
			if err := engine.RunProcessInstance(engine.context, subProcessInstance, subTokens); err != nil {
				engine.logger.Error(fmt.Sprintf("failed to run event subprocess instance %d: %s", subProcessInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})

	if err := batch.Flush(ctx); err != nil {
		return errors.Join(newEngineErrorf("failed to flush event subprocess activation batch: %s", err), err)
	}
	return nil
}

// PublishMessageOnEventSubprocess activates a (possibly interrupting) message start event subprocess on the
// parent process instance referenced by the given InstanceMessageSubscription.
func (engine *Engine) PublishMessageOnEventSubprocess(ctx context.Context, message *runtime.InstanceMessageSubscription, messageVariables map[string]any) error {
	// Refreshed subscription is captured here so markConsumed below can save the up-to-date pointer.
	current := message
	return engine.startEventSubprocess(ctx, eventSubprocessTrigger{
		processInstanceKey: message.ProcessInstanceKey,
		elementId:          message.ElementId,
		inputVariables:     messageVariables,
		safegoLabel:        "event-subprocess-message",
		refresh: func(ctx context.Context) (bool, error) {
			// When multiple interrupting message-start event subprocesses on the same parent are published concurrently,
			// the first one to win cancels the parent instance which terminates all sibling InstanceMessageSubscriptions.
			// The state-filtered lookup below would then return ErrNotFound for the losing publishes — treat that as a clean no-op.
			messageSub, err := engine.persistence.FindMessageSubscriptionByKey(ctx, message.Key, runtime.ActivityStateActive)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return true, nil
				}
				return false, errors.Join(newEngineErrorf("failed to find active message subscription: %d", message.Key), err)
			}
			refreshed, ok := messageSub.(*runtime.InstanceMessageSubscription)
			if !ok {
				return false, fmt.Errorf("message type after refresh not supported")
			}
			if refreshed.State != runtime.ActivityStateActive {
				return true, nil
			}
			current = refreshed
			return false, nil
		},
		markConsumed: func(ctx context.Context, batch *EngineBatch) error {
			current.State = runtime.ActivityStateCompleted
			if err := batch.SaveMessageSubscription(ctx, current); err != nil {
				return fmt.Errorf("failed to save message subscription %d: %w", current.Key, err)
			}
			return nil
		},
	})
}

// processTimerTriggerOnEventSubprocess activates a (possibly interrupting) timer-start event subprocess on the
// parent process instance referenced by the given timer.
func (engine *Engine) processTimerTriggerOnEventSubprocess(ctx context.Context, timer runtime.Timer) (
	*runtime.ProcessInstance, []runtime.ExecutionToken, error,
) {
	current := timer
	err := engine.startEventSubprocess(ctx, eventSubprocessTrigger{
		processInstanceKey: *timer.ProcessInstanceKey,
		elementId:          timer.ElementId,
		inputVariables:     map[string]any{},
		safegoLabel:        "event-subprocess-timer",
		refresh: func(ctx context.Context) (bool, error) {
			refreshed, err := engine.persistence.GetTimer(ctx, timer.Key)
			if err != nil {
				return false, fmt.Errorf("failed to refresh timer %d: %w", timer.Key, err)
			}
			switch refreshed.TimerState {
			case runtime.TimerStateTriggered, runtime.TimerStateCancelled:
				return true, nil
			}
			current = refreshed
			return false, nil
		},
		markConsumed: func(ctx context.Context, batch *EngineBatch) error {
			current.TimerState = runtime.TimerStateTriggered
			if err := batch.SaveTimer(ctx, current); err != nil {
				return fmt.Errorf("failed to update timer state for timer %d: %w", current.Key, err)
			}
			return nil
		},
	})
	return nil, nil, err
}
