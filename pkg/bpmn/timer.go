package bpmn

import (
	"context"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/senseyeio/duration"
)

func (state *Engine) handleIntermediateTimerCatchEvent(ctx context.Context, instance *processInstanceInfo, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (continueFlow bool, timer *runtime.Timer, err error) {
	timer = findExistingTimerNotYetTriggered(state, ice.Id, instance)

	if timer != nil && timer.OriginActivity != nil {
		originActivity := instance.findActivity(timer.OriginActivity.Key())
		if originActivity != nil && originActivity.Element().GetType() == bpmn20.EventBasedGateway {
			ebgActivity := originActivity.(eventBasedGatewayActivity)
			if ebgActivity.OutboundCompleted() {
				timer.TimerState = runtime.TimerCancelled
				return false, timer, err
			}
		}
	}

	if timer == nil {
		timer, err = state.createTimer(ctx, instance, ice, originActivity)
		if err != nil {
			evalErr := &ExpressionEvaluationError{
				Msg: fmt.Sprintf("Error evaluating expression in intermediate timer cacht event Activity id='%s' name='%s'", ice.Id, ice.Name),
				Err: err,
			}
			return false, timer, evalErr
		}
	}

	if time.Now().After(timer.DueAt) {
		timer.TimerState = runtime.TimerTriggered
		if timer.OriginActivity != nil {
			originActivity := instance.findActivity(timer.OriginActivity.Key())
			if originActivity != nil && originActivity.Element().GetType() == bpmn20.EventBasedGateway {
				ebgActivity := originActivity.(eventBasedGatewayActivity)
				ebgActivity.SetOutboundCompleted(ice.Id)
			}
		}
		return true, timer, err
	}
	return false, timer, err
}

func (state *Engine) createTimer(ctx context.Context, instance *processInstanceInfo, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (*runtime.Timer, error) {
	durationVal, err := findDurationValue(ice)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from Activity with ID=%s. Error:%s", ice.Id, err.Error())}
	}
	var be bpmn20.FlowNode = ice
	now := time.Now()
	t := &runtime.Timer{
		ElementId:          ice.Id,
		ElementInstanceKey: state.generateKey(),
		ProcessKey:         instance.ProcessInfo.ProcessKey,
		ProcessInstanceKey: instance.InstanceKey,
		TimerState:         runtime.TimerCreated,
		CreatedAt:          now,
		DueAt:              durationVal.Shift(now),
		Duration:           time.Duration(durationVal.TS) * time.Second,
		BaseElement:        be,
		OriginActivity:     originActivity,
	}
	_err := state.persistence.PersistNewTimer(ctx, t)
	return t, _err
}

func findExistingTimerNotYetTriggered(state *Engine, id string, instance *processInstanceInfo) *runtime.Timer {
	var t *runtime.Timer
	var key *int64
	if instance != nil {
		key = ptr.To(instance.GetInstanceKey())
	}

	timers := state.persistence.FindTimers(nil, key, runtime.TimerCreated)
	for _, timer := range timers {
		if timer.ElementId == id && timer.ProcessInstanceKey == *key && timer.TimerState == runtime.TimerCreated {
			return t
		}
	}
	return t
}

func findDurationValue(ice bpmn20.TIntermediateCatchEvent) (duration.Duration, error) {
	durationStr := ice.TimerEventDefinition.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=%s", ice.Id)
	}
	return duration.ParseISO8601(durationStr)
}
