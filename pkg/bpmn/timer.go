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

func (engine *Engine) createIntermediateTimerCatchEvent(ctx context.Context, timerWriter storage.TimerStorageWriter, instance *runtime.ProcessInstance, ice *bpmn20.TIntermediateCatchEvent, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	timer, err := engine.createTimer(ctx, timerWriter, instance, ice, currentToken)
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
	ice *bpmn20.TIntermediateCatchEvent,
	token runtime.ExecutionToken,
) (*runtime.Timer, error) {
	timerDef := ice.EventDefinition.(bpmn20.TTimerEventDefinition)
	durationVal, err := findDurationValue(timerDef)
	if err != nil {
		return nil, &BpmnEngineError{Msg: fmt.Sprintf("Error parsing 'timeDuration' value "+
			"from Activity with ID=%s. Error:%s", ice.Id, err.Error())}
	}
	var be bpmn20.FlowNode = ice
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
		Token:                token,
	}
	_err := timerStorageWriter.SaveTimer(ctx, t)
	return &t, _err
}

func findDurationValue(timerDef bpmn20.TTimerEventDefinition) (duration.Duration, error) {
	durationStr := timerDef.TimeDuration.XMLText
	if len(strings.TrimSpace(durationStr)) == 0 {
		return duration.Duration{}, newEngineErrorf("Can't find 'timeDuration' value for INTERMEDIATE_CATCH_EVENT with id=%s", timerDef.Id)
	}
	return duration.ParseISO8601(durationStr)
}
