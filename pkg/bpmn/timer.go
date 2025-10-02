package bpmn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/codes"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/senseyeio/duration"
)

func (engine *Engine) createTimerCatchEvent(ctx context.Context, timerWriter storage.TimerStorageWriter, instance *runtime.ProcessInstance, timerDef bpmn20.TTimerEventDefinition, element bpmn20.FlowNode, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	timer, err := engine.createTimer(ctx, timerWriter, instance, timerDef, element, currentToken)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return currentToken, fmt.Errorf("failed to create timer %+v: %w", timer, err)
	}

	// TODO: add timer into engine timer registry
	_ = timer

	currentToken.State = runtime.TokenStateWaiting
	return currentToken, err
}

func (engine *Engine) createTimer(
	ctx context.Context,
	timerStorageWriter storage.TimerStorageWriter,
	instance *runtime.ProcessInstance,
	timerDef bpmn20.TTimerEventDefinition,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) (*runtime.Timer, error) {
	durationVal, err := findDurationValue(timerDef)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from Activity with ID=%s. Error:%s", element.GetId(), err.Error())}
	}
	now := time.Now()
	t := runtime.Timer{
		ElementId:            element.GetId(),
		Key:                  engine.generateKey(),
		ElementInstanceKey:   token.ElementInstanceKey,
		ProcessDefinitionKey: instance.Definition.Key,
		ProcessInstanceKey:   instance.Key,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                durationVal.Shift(now),
		Duration:             time.Duration(durationVal.TS) * time.Second,
		Token:                token,
	}
	err = timerStorageWriter.SaveTimer(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("failed to save timer: %w", err)
	}
	engine.timerManager.registerTimer(t)
	return &t, nil
}

func (engine *Engine) processTimer(ctx context.Context, timer runtime.Timer) {
	instance, tokens, err := engine.triggerTimer(ctx, timer)
	if err != nil {
		engine.logger.Error(fmt.Sprintf("failed to trigger timer %d: %s", timer.Key, err))
	}

	err = engine.runProcessInstance(ctx, instance, tokens)
	if err != nil {
		engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", instance.Key, err))
		return
	}
}

func (engine *Engine) triggerTimer(ctx context.Context, timer runtime.Timer) (
	instance *runtime.ProcessInstance,
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
	inst, err := engine.persistence.FindProcessInstanceByKey(ctx, timer.ProcessInstanceKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find process instance with key: %d", timer.ProcessInstanceKey), err)
	}
	instance = &inst

	currentToken := timer.Token
	tokenNode := instance.Definition.Definitions.Process.GetFlowNodeById(currentToken.ElementId)
	if tokenNode.GetId() == "" {
		return nil, nil, errors.Join(newEngineErrorf("failed to find timer node with elementId: %s", timer.ElementId), err)
	}

	batch := engine.persistence.NewBatch()

	switch nodeT := tokenNode.(type) {
	case *bpmn20.TEventBasedGateway:
		t, err := engine.publishEventOnEventGateway(ctx, batch, nodeT, timer, instance, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to handle timer event gateway transition %+v: %w", timer, err)
		}
		tokens = t
		err = batch.Flush(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}
	case *bpmn20.TIntermediateCatchEvent:
		timer.TimerState = runtime.TimerStateTriggered
		batch.SaveTimer(ctx, timer)
		tokens, err = engine.handleSimpleTransition(ctx, batch, instance, nodeT, timer.Token)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to handle timer transition %+v: %w", timer, err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}
	case *bpmn20.TServiceTask, *bpmn20.TSendTask, *bpmn20.TUserTask, *bpmn20.TBusinessRuleTask, *bpmn20.TCallActivity:
		tokens, err = engine.handleBoundaryTimer(ctx, batch, timer, instance, timer.Token)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to handle timer transition %+v: %w", timer, err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}

	default:
		msg := fmt.Sprintf("failed to trigger timer %+v to instance %d. Unexpected node type %T", timer, instance.Key, nodeT)
		engine.logger.Error(msg)
		return nil, nil, &BpmnEngineError{Msg: msg}
	}

	return instance, tokens, nil
}

func findDurationValue(timerDef bpmn20.TTimerEventDefinition) (duration.Duration, error) {
	durationStr := timerDef.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=%s", timerDef.Id)
	}
	return duration.ParseISO8601(durationStr)
}

func (engine *Engine) handleBoundaryTimer(ctx context.Context, batch storage.Batch, timer runtime.Timer, instance *runtime.ProcessInstance, token runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	var listener *bpmn20.TBoundaryEvent

	for _, be := range instance.Definition.Definitions.Process.BoundaryEvent {
		if be.AttachedToRef != timer.Token.ElementId {
			continue
		}
		if _, ok := be.EventDefinition.(bpmn20.TTimerEventDefinition); ok {
			listener = &be
			break
		}
	}
	if listener == nil {
		return nil, fmt.Errorf("failed to find boundary event for timer %s", timer.GetId())
	}

	timer.TimerState = runtime.TimerStateTriggered
	batch.SaveTimer(ctx, timer)

	if listener.CancellActivity {
		// cancel job
		job, err := engine.persistence.FindJobByElementID(ctx, instance.Key, token.ElementId)
		if err != nil {
			return nil, fmt.Errorf("failed to find job for token %d: %w", token.Key, err)
		}
		job.State = runtime.ActivityStateTerminated
		err = batch.SaveJob(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
		}
		engine.cancelBoundarySubscriptions(ctx, batch, instance, &token)
		// cancel all called processes
		calledProcesses, err := engine.persistence.FindProcessInstanceByParentExecutionTokenKey(ctx, token.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to find called processes for token %d: %w", token.Key, err)
		}
		for _, calledProcess := range calledProcesses {
			engine.cancelInstance(ctx, calledProcess, batch)
		}
	} else {
		element := instance.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
		// recreate the message subscription
		_, err := engine.createTimerCatchEvent(ctx, batch, instance, listener.EventDefinition.(bpmn20.TTimerEventDefinition), element, token)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate message subscription: %w", err)
		}
	}
	tokens, err := engine.handleSimpleTransition(ctx, batch, instance, listener, timer.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to handle boundary timer transition %+v: %w", timer, err)
	}
	return tokens, nil
}
