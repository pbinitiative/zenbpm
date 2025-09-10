// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
	now := time.Now()
	t := runtime.Timer{
		ElementId:            ice.Id,
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
		t, msubs, err := engine.publishEventOnEventGateway(ctx, batch, nodeT, timer, instance, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to handle timer event gateway transition %+v: %w", timer, err)
		}
		tokens = t
		err = batch.Flush(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to flush trigger timer batch %+v: %w", timer, err)
		}
		engine.persistence.TerminateMessageSubscriptionPointersForExecution(ctx, msubs, timer.Token.Key)
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
