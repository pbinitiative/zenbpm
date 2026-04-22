package bpmn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/navsmb/datetime"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/senseyeio/duration"
)

func (engine *Engine) createTimerCatchEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, timerDef bpmn20.TTimerEventDefinition, element bpmn20.FlowNode, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	timer, err := engine.createDurationTimer(instance, timerDef, element.GetId(), &currentToken)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return currentToken, fmt.Errorf("failed to create timer %+v: %w", timer, err)
	}
	err = batch.SaveTimer(ctx, *timer)
	if err != nil {
		return currentToken, fmt.Errorf("failed to save timer: %w", err)
	}

	// TODO: add timer into engine timer registry
	_ = timer

	currentToken.State = runtime.TokenStateWaiting
	return currentToken, err
}

func (engine *Engine) createDurationTimer(
	instance runtime.ProcessInstance,
	timerDef bpmn20.TTimerEventDefinition,
	elementId string,
	token *runtime.ExecutionToken,
) (*runtime.Timer, error) {
	durationVal, err := findDurationValue(timerDef)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from Activity with ID=%s. Error:%s", elementId, err.Error())}
	}
	now := time.Now()
	var elementInstanceKey *int64
	if token != nil {
		elementInstanceKey = &token.ElementInstanceKey
	}
	t := runtime.Timer{
		ElementId:            elementId,
		Key:                  engine.generateKey(),
		ElementInstanceKey:   elementInstanceKey,
		ProcessDefinitionKey: instance.ProcessInstance().Definition.Key,
		ProcessInstanceKey:   &instance.ProcessInstance().Key,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                durationVal.Shift(now),
		Duration:             time.Duration(durationVal.TS) * time.Second,
		Token:                token,
	}
	engine.timerManager.registerTimer(t)
	return &t, nil
}

func findDurationValue(timerDef bpmn20.TTimerEventDefinition) (duration.Duration, error) {
	if timerDef.TimeDuration == nil {
		return duration.Duration{}, fmt.Errorf("timerDef.TimeDuration is nil")
	}
	durationStr := timerDef.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		id := "<unknown>"
		if timerDef.Id != nil {
			id = *timerDef.Id
		}
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for element with id=%s", id)
	}
	return duration.ParseISO8601(durationStr)
}

func findStartTime(timerDef bpmn20.TTimerEventDefinition) (time.Time, error) {
	if timerDef.TimeDate == nil {
		return time.Time{}, fmt.Errorf("timerDef.TimeDate is nil")
	}
	timeDateStr := timerDef.TimeDate.XMLText
	if len(strings.TrimSpace(timeDateStr)) == 0 {
		id := "<unknown>"
		if timerDef.Id != nil {
			id = *timerDef.Id
		}
		return time.Time{}, newEngineErrorf("Can't find 'timeDate' value for %s with id=%s", bpmn20.ElementTypeStartEvent, id)
	}
	return datetime.ParseLocal(timeDateStr)
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
		jobs, err := engine.persistence.GetJobsInStateByTokenKey(ctx, token.Key, []runtime.ActivityState{runtime.ActivityStateActive})
		if err != nil {
			return nil, fmt.Errorf("failed to find job for token %d: %w", token.Key, err)
		}
		for i := range jobs {
			jobs[i].State = runtime.ActivityStateTerminated
			err = batch.SaveJob(ctx, jobs[i])
			if err != nil {
				return nil, fmt.Errorf("failed to save changes to job %d: %w", jobs[i].Key, err)
			}
		}

		err = engine.cancelBoundarySubscriptions(ctx, batch, instance.ProcessInstance().Key, &token)
		if err != nil {
			return nil, err
		}
		// cancel all called processes
		calledProcesses, err := engine.persistence.FindProcessInstancesByParentExecutionTokenKey(ctx, token.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to find called processes for token %d: %w", token.Key, err)
		}
		for _, calledProcess := range calledProcesses {
			err := engine.cancelSubProcessInstance(ctx, calledProcess, batch)
			if err != nil {
				return nil, err
			}
		}
	} else {
		element := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
		// recreate the message subscription
		_, err := engine.createTimerCatchEvent(ctx, batch, instance, listener.EventDefinition.(bpmn20.TTimerEventDefinition), element, token)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate message subscription: %w", err)
		}

		token = runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          listener.GetId(),
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
		err = batch.SaveToken(ctx, token)
		if err != nil {
			return nil, err
		}
	}
	if timer.Token == nil {
		return nil, fmt.Errorf("boundary timer event must have token assigned, timerKey=%d", timer.GetKey())
	}
	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, *timer.Token)

	err := batch.SaveFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:                engine.generateKey(),
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          listener.GetId(),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  token.Key,
			InputVariables:     nil,
			OutputVariables:    nil,
		},
	)
	if err != nil {
		return nil, err
	}

	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, token)
	if err != nil {
		return nil, fmt.Errorf("failed to handle boundary timer transition %+v: %w", timer, err)
	}
	return tokens, nil
}
