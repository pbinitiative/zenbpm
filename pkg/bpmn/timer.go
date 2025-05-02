package bpmn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/senseyeio/duration"
)

func (engine *Engine) handleIntermediateTimerCatchEvent(ctx context.Context, timerWriter storage.TimerStorageWriter, instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (continueFlow bool, timer *runtime.Timer, err error) {
	timer, err = findExistingTimerNotYetTriggered(engine, ice.Id, instance)
	if err != nil {
		return false, nil, fmt.Errorf("failed to find not triggered timer: %w", err)
	}

	if timer != nil && timer.OriginActivity != nil {
		originActivity := instance.FindActivity(timer.OriginActivity.GetKey())
		if originActivity != nil && originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
			ebgActivity := originActivity.(eventBasedGatewayActivity)
			if ebgActivity.OutboundCompleted() {
				timer.TimerState = runtime.TimerStateCancelled
				return false, timer, err
			}
		}
	}

	if timer == nil {
		timer, err = engine.createTimer(ctx, timerWriter, instance, ice, originActivity)
		if err != nil {
			evalErr := &ExpressionEvaluationError{
				Msg: fmt.Sprintf("Error evaluating expression in intermediate timer cacht event Activity id='%s' name='%s'", ice.Id, ice.Name),
				Err: err,
			}
			return false, timer, evalErr
		}
	}

	if time.Now().After(timer.DueAt) {
		timer.TimerState = runtime.TimerStateTriggered
		if timer.OriginActivity != nil {
			originActivity := instance.FindActivity(timer.OriginActivity.GetKey())
			if originActivity != nil && originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
				ebgActivity := originActivity.(eventBasedGatewayActivity)
				ebgActivity.SetOutboundCompleted(ice.Id)
			}
		}
		return true, timer, err
	}
	return false, timer, err
}

func (engine *Engine) createTimer(
	ctx context.Context,
	timerStorageWriter storage.TimerStorageWriter,
	instance *runtime.ProcessInstance,
	ice bpmn20.TIntermediateCatchEvent,
	originActivity runtime.Activity,
) (*runtime.Timer, error) {
	durationVal, err := findDurationValue(ice)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from Activity with ID=%s. Error:%s", ice.Id, err.Error())}
	}
	var be bpmn20.FlowNode = &ice
	now := time.Now()
	t := runtime.Timer{
		ElementId:            ice.Id,
		Key:                  engine.generateKey(),
		ProcessDefinitionKey: instance.Definition.Key,
		ProcessInstanceKey:   instance.Key,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                durationVal.Shift(now),
		Duration:             time.Duration(durationVal.TS) * time.Second,
		BaseElement:          be,
		OriginActivity:       originActivity,
	}
	_err := timerStorageWriter.SaveTimer(ctx, t)
	return &t, _err
}

func findExistingTimerNotYetTriggered(engine *Engine, id string, instance *runtime.ProcessInstance) (*runtime.Timer, error) {
	key := instance.GetInstanceKey()

	timers, err := engine.persistence.FindTimersByState(context.TODO(), key, runtime.TimerStateCreated)
	if err != nil {
		return nil, fmt.Errorf("failed to find timers by engine for key: %d: %w", key, err)
	}
	for _, timer := range timers {
		if timer.ElementId == id && timer.ProcessInstanceKey == key && timer.TimerState == runtime.TimerStateCreated {
			return &timer, nil
		}
	}
	return nil, nil
}

func findDurationValue(ice bpmn20.TIntermediateCatchEvent) (duration.Duration, error) {
	durationStr := ice.TimerEventDefinition.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=%s", ice.Id)
	}
	return duration.ParseISO8601(durationStr)
}
