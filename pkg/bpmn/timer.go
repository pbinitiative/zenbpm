package bpmn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/senseyeio/duration"
)

// Timer is created, when a process instance reaches a Timer Intermediate Message Event.
// The logic is simple: CreatedAt + Duration = DueAt
// The TimerState is one of [ TimerCreated, TimerTriggered, TimerCancelled ]
type Timer struct {
	ElementId          string        `json:"id"`
	ElementInstanceKey int64         `json:"ik"`
	ProcessKey         int64         `json:"pk"`
	ProcessInstanceKey int64         `json:"pik"`
	TimerState         TimerState    `json:"s"`
	CreatedAt          time.Time     `json:"c"`
	DueAt              time.Time     `json:"da"`
	Duration           time.Duration `json:"du"`
	originActivity     activity
	baseElement        bpmn20.FlowNode
}

type TimerState string

const TimerCreated TimerState = "CREATED"
const TimerTriggered TimerState = "TRIGGERED"
const TimerCancelled TimerState = "CANCELLED"

func (t Timer) Key() int64 {
	return t.ElementInstanceKey
}

func (t Timer) State() ActivityState {
	switch t.TimerState {
	case TimerCreated:
		return Active
	case TimerTriggered:
		return Completed
	case TimerCancelled:
		return Withdrawn
	}
	panic(fmt.Sprintf("[invariant check] missing mapping for timer state=%s", t.TimerState))
}

func (t Timer) Element() bpmn20.FlowNode {
	return t.baseElement
}

func (state *Engine) handleIntermediateTimerCatchEvent(ctx context.Context, instance *processInstanceInfo, ice bpmn20.TIntermediateCatchEvent, originActivity activity) (continueFlow bool, timer *Timer, err error) {
	timer = findExistingTimerNotYetTriggered(state, ice.Id, instance)

	if timer != nil && timer.originActivity != nil {
		originActivity := instance.findActivity(timer.originActivity.Key())
		if originActivity != nil && originActivity.Element().GetType() == bpmn20.EventBasedGateway {
			ebgActivity := originActivity.(eventBasedGatewayActivity)
			if ebgActivity.OutboundCompleted() {
				timer.TimerState = TimerCancelled
				return false, timer, err
			}
		}
	}

	if timer == nil {
		timer, err = state.createTimer(ctx, instance, ice, originActivity)
		if err != nil {
			evalErr := &ExpressionEvaluationError{
				Msg: fmt.Sprintf("Error evaluating expression in intermediate timer cacht event activity id='%s' name='%s'", ice.Id, ice.Name),
				Err: err,
			}
			return false, timer, evalErr
		}
	}

	if time.Now().After(timer.DueAt) {
		timer.TimerState = TimerTriggered
		if timer.originActivity != nil {
			originActivity := instance.findActivity(timer.originActivity.Key())
			if originActivity != nil && originActivity.Element().GetType() == bpmn20.EventBasedGateway {
				ebgActivity := originActivity.(eventBasedGatewayActivity)
				ebgActivity.SetOutboundCompleted(ice.Id)
			}
		}
		return true, timer, err
	}
	return false, timer, err
}

func (state *Engine) createTimer(ctx context.Context, instance *processInstanceInfo, ice bpmn20.TIntermediateCatchEvent, originActivity activity) (*Timer, error) {
	durationVal, err := findDurationValue(ice)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from activity with ID=%s. Error:%s", ice.Id, err.Error())}
	}
	var be bpmn20.FlowNode = ice
	now := time.Now()
	t := &Timer{
		ElementId:          ice.Id,
		ElementInstanceKey: state.generateKey(),
		ProcessKey:         instance.ProcessInfo.ProcessKey,
		ProcessInstanceKey: instance.InstanceKey,
		TimerState:         TimerCreated,
		CreatedAt:          now,
		DueAt:              durationVal.Shift(now),
		Duration:           time.Duration(durationVal.TS) * time.Second,
		baseElement:        be,
		originActivity:     originActivity,
	}
	_err := state.persistence.PersistNewTimer(ctx, t)
	return t, _err
}

func findExistingTimerNotYetTriggered(state *Engine, id string, instance *processInstanceInfo) *Timer {
	var t *Timer
	var key *int64
	if instance != nil {
		key = ptr.To(instance.GetInstanceKey())
	}

	timers := state.persistence.FindTimers(nil, key, TimerCreated)
	for _, timer := range timers {
		if timer.ElementId == id && timer.ProcessInstanceKey == *key && timer.TimerState == TimerCreated {
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
