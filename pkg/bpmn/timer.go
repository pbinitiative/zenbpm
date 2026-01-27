package bpmn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/senseyeio/duration"
)

func (engine *Engine) createTimerCatchEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, timerDef bpmn20.TTimerEventDefinition, element bpmn20.FlowNode, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	timer, err := engine.createTimer(ctx, batch, instance, timerDef, element, currentToken)
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
	batch *EngineBatch,
	instance runtime.ProcessInstance,
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
		ProcessDefinitionKey: instance.ProcessInstance().Definition.Key,
		ProcessInstanceKey:   instance.ProcessInstance().Key,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                durationVal.Shift(now),
		Duration:             time.Duration(durationVal.TS) * time.Second,
		Token:                token,
	}
	err = batch.SaveTimer(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("failed to save timer: %w", err)
	}
	engine.timerManager.registerTimer(t)
	return &t, nil
}

func findDurationValue(timerDef bpmn20.TTimerEventDefinition) (duration.Duration, error) {
	durationStr := timerDef.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=%s", timerDef.Id)
	}
	return duration.ParseISO8601(durationStr)
}

func (engine *Engine) handleBoundaryTimer(ctx context.Context, batch *EngineBatch, timer runtime.Timer, instance runtime.ProcessInstance, token runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	var listener *bpmn20.TBoundaryEvent

	for _, be := range instance.ProcessInstance().Definition.Definitions.Process.BoundaryEvent {
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
		job, err := engine.persistence.FindJobByElementID(ctx, instance.ProcessInstance().Key, token.ElementId)
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
			err := batch.AddLockedInstance(ctx, calledProcess)
			if err != nil {
				return nil, err
			}
			engine.cancelInstance(ctx, calledProcess, batch)
		}
	} else {
		element := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
		// recreate the message subscription
		_, err := engine.createTimerCatchEvent(ctx, batch, instance, listener.EventDefinition.(bpmn20.TTimerEventDefinition), element, token)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate message subscription: %w", err)
		}
	}
	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, timer.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to handle boundary timer transition %+v: %w", timer, err)
	}
	return tokens, nil
}
