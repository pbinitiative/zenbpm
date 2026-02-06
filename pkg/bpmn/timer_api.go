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

	err = engine.RunProcessInstance(ctx, instance, tokens)
	if err != nil {
		engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", instance.ProcessInstance().Key, err))
		return
	}
}

// TODO: Fix this method needs to refresh after locking instance
func (engine *Engine) TriggerTimer(ctx context.Context, timer runtime.Timer) (
	instance runtime.ProcessInstance,
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
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, timer.ProcessInstanceKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find process instance with key: %d", timer.ProcessInstanceKey), err)
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
		tokens, err = engine.handleElementTransition(ctx, &batch, instance, nodeT, timer.Token)
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
		tokens, err = engine.handleBoundaryTimer(ctx, &batch, timer, instance, timer.Token)
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

	return instance, tokens, nil
}
