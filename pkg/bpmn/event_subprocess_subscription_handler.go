package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	// renew, when non-nil, is invoked AFTER markConsumed but only when the event subprocess start event is
	// non-interrupting. It is responsible for re-creating the trigger (e.g. a fresh Active
	// InstanceMessageSubscription) so subsequent fires can activate the event subprocess again — a
	// non-interrupting event subprocess can run any number of times for the lifetime of its parent.
	renew func(ctx context.Context, batch *EngineBatch) error
}

// startEventSubprocess implements the activation logic shared by message and timer (and potentially future new) start event subprocesses.
// It locks the parent process instance, re-validates the trigger via t.refresh, optionally cancels the parent (when the start event is interrupting),
// propagates trigger variables, creates the event subprocess instance and schedules its execution.
func (engine *Engine) startEventSubprocess(ctx context.Context, t eventSubprocessTrigger) (retErr error) {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, t.processInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process instance with key: %d", t.processInstanceKey), err)
	}

	subProcessDef, startEventDef, err := resolveEventSubprocessDefs(instance, t.elementId)
	if err != nil {
		return err
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

	variableHolder, tokens, err := engine.prepareParentForEventSubprocess(ctx, &batch, instance, startEventDef, t.inputVariables)
	if err != nil {
		return err
	}
	if len(tokens) == 0 { // Parent process is not active anymore — nothing to start.
		batch.Clear(ctx)
		return nil
	}

	subProcessInstance, subTokens, err := engine.buildEventSubprocessInstance(ctx, &batch, instance, subProcessDef, startEventDef, variableHolder, tokens)
	if err != nil {
		return err
	}

	if err := t.markConsumed(ctx, &batch); err != nil {
		return err
	}

	// Non-interrupting event subprocesses can be triggered multiple times for a single parent process instance.
	if !startEventDef.IsInterrupting && t.renew != nil {
		if err := t.renew(ctx, &batch); err != nil {
			return err
		}
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

// resolveEventSubprocessDefs looks up the subprocess definition and its start event from the process instance model.
// It performs no I/O and returns an error when either definition cannot be found.
func resolveEventSubprocessDefs(instance runtime.ProcessInstance, elementId string) (*bpmn20.TSubProcess, *bpmn20.TStartEvent, error) {
	subProcessDef, startEventDef := instance.ProcessInstance().Definition.Definitions.Process.GetSubprocessAndStartEventById(elementId)
	if startEventDef == nil {
		return nil, nil, fmt.Errorf("failed to find a startEvent with id: %s", elementId)
	}
	if subProcessDef == nil {
		return nil, nil, fmt.Errorf("failed to find a subProcess for start element id: %s", elementId)
	}
	return subProcessDef, startEventDef, nil
}

// prepareParentForEventSubprocess handles all mutations to the parent process instance that must happen before the
// event subprocess can be created: optionally interrupts the parent, propagates trigger variables and saves the
// updated parent instance. It also fetches and returns the parent's currently active tokens.
func (engine *Engine) prepareParentForEventSubprocess(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	startEventDef *bpmn20.TStartEvent,
	inputVariables map[string]any,
) (runtime.VariableHolder, []runtime.ExecutionToken, error) {
	if startEventDef.IsInterrupting {
		if _, err := engine.handleProcessInstanceInnerCancel(ctx, instance, batch); err != nil {
			return runtime.VariableHolder{}, nil, fmt.Errorf("failed to cancel parent instance %d for interrupting event subprocess: %w", instance.ProcessInstance().Key, err)
		}
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	propagatedVariables, err := variableHolder.PropagateMappedOutputsOrAll(startEventDef.GetOutputMapping(), inputVariables, engine.evaluateExpression)
	if err != nil {
		return runtime.VariableHolder{}, nil, fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	// Make sure the propagated variables are also visible inside the event subprocess itself.
	for k, v := range propagatedVariables {
		variableHolder.SetLocalVariable(k, v)
	}
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return runtime.VariableHolder{}, nil, fmt.Errorf("failed to save parent process instance %d after propagating trigger variables: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return runtime.VariableHolder{}, nil, fmt.Errorf("failed to get active tokens for process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	return variableHolder, tokens, nil
}

// buildEventSubprocessInstance creates the event subprocess process instance together with its execution tokens and
// registers any nested event-subprocess subscriptions on the new instance.
func (engine *Engine) buildEventSubprocessInstance(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	subProcessDef *bpmn20.TSubProcess,
	startEventDef *bpmn20.TStartEvent,
	variableHolder runtime.VariableHolder,
	tokens []runtime.ExecutionToken,
) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	startingFlowNodes := []bpmn20.FlowNode{startEventDef}
	if len(tokens) == 0 {
		return nil, nil, fmt.Errorf(
			"cannot build event subprocess instance for %q: parent process instance %d has no active tokens",
			subProcessDef.Id, instance.ProcessInstance().Key,
		)
	}
	subProcessInstance, subTokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		variableHolder,
		&runtime.SubProcessInstance{
			ParentProcessExecutionToken:           tokens[0],
			ParentProcessTargetElementInstanceKey: tokens[0].ElementInstanceKey,
			ParentProcessTargetElementId:          subProcessDef.Id,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create event subprocess instance: %w", err)
	}

	if err := engine.createEventSubProcessSubscriptions(ctx, batch, subProcessInstance, &subProcessDef.TFlowElementsContainer); err != nil {
		return nil, nil, fmt.Errorf("failed to create event subprocess subscriptions in sub process %s: %w", subProcessDef.Id, err)
	}
	return subProcessInstance, subTokens, nil
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
		renew: func(ctx context.Context, batch *EngineBatch) error {
			// Re-create the InstanceMessageSubscription with the same name/correlation key but a fresh
			// key and Active state so subsequent publishes can trigger the non-interrupting event subprocess again.
			renewed := &runtime.InstanceMessageSubscription{
				ProcessInstanceKey: current.ProcessInstanceKey,
				CorrelationKey:     current.CorrelationKey,
				MessageSubscriptionData: runtime.MessageSubscriptionData{
					Key:                  engine.generateKey(),
					ElementId:            current.ElementId,
					Name:                 current.Name,
					State:                runtime.ActivityStateActive,
					ProcessDefinitionKey: current.ProcessDefinitionKey,
					CreatedAt:            time.Now(),
				},
			}
			if err := batch.SaveMessageSubscription(ctx, renewed); err != nil {
				return fmt.Errorf("failed to renew non-interrupting message subscription for element %s on instance %d: %w",
					current.ElementId, current.ProcessInstanceKey, err)
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
