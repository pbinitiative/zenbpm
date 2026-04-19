package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"go.opentelemetry.io/otel/codes"
)

func (engine *Engine) ProcessTimer(ctx context.Context, timer runtime.Timer) {
	instance, tokens, err := engine.TriggerTimer(ctx, timer)
	if err != nil {
		engine.logger.Error(fmt.Sprintf("failed to trigger timer %d: %s", timer.Key, err))
		return
	}
	if instance == nil {
		return
	}
	err = engine.RunProcessInstance(ctx, *instance, tokens)
	if err != nil {
		engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", (*instance).ProcessInstance().Key, err))
		return
	}
}

func (engine *Engine) TriggerTimer(ctx context.Context, timer runtime.Timer) (
	resInstance *runtime.ProcessInstance,
	tokens []runtime.ExecutionToken,
	retErr error,
) {
	ctx, completeTimerSpan := engine.tracer.Start(ctx, fmt.Sprintf("timer:%d", timer.Key))
	defer func() {
		if retErr != nil {
			completeTimerSpan.RecordError(retErr)
			completeTimerSpan.SetStatus(codes.Error, retErr.Error())
		}
		completeTimerSpan.End()
	}()

	if timer.ProcessInstanceKey == nil { // timer start event creates and starts the new process instance
		return engine.createStartProcessOnTimerStartEvent(ctx, timer)
	}

	if timer.Token == nil { // timer start event creates and starts the event sub process instance
		return engine.createStartEventSubProcessOnTimerStartEvent(ctx, timer)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, *timer.ProcessInstanceKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find process instance with key: %d", *timer.ProcessInstanceKey), err)
	}

	currentToken := timer.Token
	tokenNode := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(currentToken.ElementId)
	if tokenNode.GetId() == "" {
		return nil, nil, errors.Join(newEngineErrorf("failed to find timer node with elementId: %s", timer.ElementId), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to create batch for timer %d: %s", timer.Key, err))
	}
	timerRefreshed, err := engine.persistence.GetTimer(ctx, timer.Key)
	if err != nil {
		return nil, nil, newEngineErrorf("failed to find timer %d: %s", timer.Key, err)
	}
	timer = timerRefreshed
	switch timer.TimerState {
	case runtime.TimerStateTriggered:
		return nil, nil, newEngineErrorf("timer is already triggered: %d", timer.Key)
	case runtime.TimerStateCancelled:
		return nil, nil, newEngineErrorf("timer is already cancelled: %d", timer.Key)
	default:
		// do nothing
	}

	switch nodeT := tokenNode.(type) {
	case *bpmn20.TEventBasedGateway:
		t, err := engine.publishEventOnEventGateway(ctx, &batch, nodeT, timer, instance, nil)
		if err != nil {
			batch.Clear(ctx)
			return nil, nil, fmt.Errorf("failed to handle timer event gateway transition %+v: %w", timer, err)
		}
		tokens = t
		err = batch.Flush(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}
	case *bpmn20.TIntermediateCatchEvent:
		timer.TimerState = runtime.TimerStateTriggered
		err := batch.SaveTimer(ctx, timer)
		if err != nil {
			return nil, nil, err
		}
		//TODO: BUG ? Tokens are never saved
		if timer.Token == nil {
			return nil, nil, fmt.Errorf("timer %d does not have an associated token", timer.Key)
		}
		tokens, err = engine.handleElementTransition(ctx, &batch, instance, nodeT, *timer.Token)
		if err != nil {
			batch.Clear(ctx)
			return nil, nil, fmt.Errorf("failed to handle timer transition %+v: %w", timer, err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			batch.Clear(ctx)
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}
	case *bpmn20.TServiceTask, *bpmn20.TSendTask, *bpmn20.TUserTask, *bpmn20.TBusinessRuleTask, *bpmn20.TCallActivity, *bpmn20.TSubProcess:
		if timer.Token == nil {
			return nil, nil, fmt.Errorf("timer %d does not have an associated token", timer.Key)
		}
		tokens, err = engine.handleBoundaryTimer(ctx, &batch, timer, instance, *timer.Token)
		if err != nil {
			batch.Clear(ctx)
			return nil, nil, fmt.Errorf("failed to handle timer transition %+v: %w", timer, err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			batch.Clear(ctx)
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}

	default:
		msg := fmt.Sprintf("failed to trigger timer %+v to instance %d. Unexpected node type %T", timer, instance.ProcessInstance().Key, nodeT)
		engine.logger.Error(msg)
		return nil, nil, &BpmnEngineError{Msg: msg}
	}

	return &instance, tokens, nil
}

// Creates and starts the process instance activated by the timer start event
func (engine *Engine) createStartProcessOnTimerStartEvent(ctx context.Context, timer runtime.Timer) (*runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	_, err := engine.CreateInstanceWithStartingElements(ctx, timer.ProcessDefinitionKey, []string{timer.ElementId}, make(map[string]interface{}), nil)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to create process instance for timer %d: %s", timer.Key, err), err)
	}
	batch := engine.persistence.NewBatch()
	timer.TimerState = runtime.TimerStateTriggered
	err = batch.SaveTimer(ctx, timer)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to update timer state for timer %d: %s", timer.Key, err), err)
	}
	err = batch.Flush(ctx)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to flush batch for timer %d: %s", timer.Key, err), err)
	}
	return nil, nil, nil
}

// Creates and starts the event sub process instance activated by the timer start event.
// Cancels the parent process instance if the event sub process is interrupting.
// If the event sub process started by the timer contains another event sub process
// the triggers for that nested event sub process will also be created.
func (engine *Engine) createStartEventSubProcessOnTimerStartEvent(ctx context.Context, timer runtime.Timer) (
	retInstance *runtime.ProcessInstance, retToken []runtime.ExecutionToken, retErr error,
) {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, *timer.ProcessInstanceKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find process instance with key: %d", *timer.ProcessInstanceKey)
	}

	subProcessDef, startEventDef := instance.ProcessInstance().Definition.Definitions.Process.GetSubprocessAndStartEventById(timer.ElementId)
	if startEventDef == nil {
		return nil, nil, fmt.Errorf("failed to find a startEvent with id: %s", timer.ElementId)
	}
	if subProcessDef == nil {
		return nil, nil, fmt.Errorf("failed to find a subProcess with id: %s", timer.ElementId)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create engine batch: %w", err)
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	// If the start event is interrupting, cancel the parent instance
	if startEventDef.IsInterrupting {
		_, err = engine.handleProcessInstanceInnerCancel(ctx, instance, &batch)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to cancel parent instance %d for interrupting event subprocess: %w", *timer.ProcessInstanceKey, err)
		}
	}

	// Propagate variables to the sub-process instance
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, map[string]interface{}{})

	// Create starting flow nodes from the event subprocess start event
	startingFlowNodes := make([]bpmn20.FlowNode, 0, 1)
	startingFlowNodes = append(startingFlowNodes, startEventDef)

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get active tokens for process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if len(tokens) == 0 {
		return nil, nil, nil // parent process is not active? then no reason to start an event subprocess
	}

	subProcessInstance, subTokens, err := engine.createInstanceWithStartingElements(
		ctx,
		&batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		variableHolder,
		&runtime.SubProcessInstance{
			ParentProcessExecutionToken:           tokens[0],                    // TODO: for event sub processes we don't need this, should we make it nullable?
			ParentProcessTargetElementInstanceKey: tokens[0].ElementInstanceKey, // TODO: for event sub processes we don't need this, should we make it nullable?
			ParentProcessTargetElementId:          subProcessDef.Id,             // the reference to the subprocess definition needed for continuation
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create event subprocess instance: %w", err)
	}

	// Create event sub process triggers for event subprocesses nested within this event sub process
	err = engine.createEventSubProcessTriggers(ctx, &batch, subProcessInstance, &subProcessDef.TFlowElementsContainer)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create event subprocess subscriptions in sub process %s: %w", subProcessDef.Id, err)
	}

	timer.TimerState = runtime.TimerStateTriggered
	err = batch.SaveTimer(ctx, timer)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update timer state for timer %d: %s", timer.Key, err)
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(ctx, subProcessInstance, subTokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to run event subprocess instance %d: %s", subProcessInstance.ProcessInstance().Key, err.Error()))
			}
		}()
	})

	err = batch.Flush(ctx)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to flush batch for timer %d: %s", timer.Key, err), err)
	}
	return nil, nil, nil
}
