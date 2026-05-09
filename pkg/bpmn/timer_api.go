package bpmn

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

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
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("failed to process timer, panic recovered: %v\n%s", r, debug.Stack())
			engine.logger.Error(panicErr.Error())
			retErr = panicErr
		}
		if retErr != nil {
			completeTimerSpan.RecordError(retErr)
			completeTimerSpan.SetStatus(codes.Error, retErr.Error())
		}
		completeTimerSpan.End()
	}()

	if timer.ProcessInstanceKey == nil { // timer start event creates and starts the new process instance
		return engine.processTimerTriggerOnInstanceCreation(ctx, timer)
	}

	if timer.Token == nil { // timer start event creates and starts the event sub process instance
		return engine.processTimerTriggerOnEventSubprocess(ctx, timer)
	}

	return engine.processTimerTriggerOnToken(ctx, timer, tokens)
}

func (engine *Engine) processTimerTriggerOnToken(ctx context.Context, timer runtime.Timer, tokens []runtime.ExecutionToken) (*runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, *timer.ProcessInstanceKey)
	if err != nil {
		return nil, nil, newEngineErrorf("failed to find process instance with key: %d", *timer.ProcessInstanceKey)
	}

	currentToken := timer.Token
	tokenNode := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(currentToken.ElementId)
	if tokenNode == nil || tokenNode.GetId() == "" {
		return nil, nil, newEngineErrorf("failed to find timer node with elementId: %s", timer.ElementId)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return nil, nil, newEngineErrorf("failed to create batch for timer %d: %s", timer.Key, err)
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
func (engine *Engine) processTimerTriggerOnInstanceCreation(ctx context.Context, timer runtime.Timer) (*runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	// Re-fetch the timer to verify it has not already been triggered or cancelled by a concurrent
	// activity (e.g. an interrupting event subprocess fired in parallel).
	timerRefreshed, err := engine.persistence.GetTimer(ctx, timer.Key)
	if err != nil {
		return nil, nil, newEngineErrorf("failed to find timer %d: %s", timer.Key, err)
	}
	switch timerRefreshed.TimerState {
	case runtime.TimerStateTriggered:
		return nil, nil, nil
	case runtime.TimerStateCancelled:
		return nil, nil, nil
	}
	timer = timerRefreshed

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

	_, err = engine.CreateInstanceWithStartingElements(ctx, timer.ProcessDefinitionKey, []string{timer.ElementId}, make(map[string]interface{}), nil)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to create process instance for timer %d: %s", timer.Key, err), err)
	}
	return nil, nil, nil
}
