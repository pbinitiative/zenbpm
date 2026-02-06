package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// PublishMessageByName publishes a message by name and correlationKey and also adds variables to the process instance
func (engine *Engine) PublishMessageByName(ctx context.Context, name string, correlationKey string, variables map[string]any) error {
	subscriptionKey, err := engine.persistence.FindActiveMessageSubscriptionKey(ctx, name, correlationKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find active message subscription: %s", name), err)
	}
	return engine.PublishMessage(ctx, subscriptionKey, variables)
}

// PublishMessage publishes a message given by subscription key and also adds variables to the process instance, which fetches this event
func (engine *Engine) PublishMessage(ctx context.Context, subscriptionKey int64, variables map[string]interface{}) error {
	message, err := engine.persistence.FindMessageSubscriptionById(ctx, subscriptionKey, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find active message subscription: %d", message.Key), err)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, message.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process instance with key: %d", message.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to create engine batch: %w", err)
	}

	// Token points either to message listener or event based gateway
	pd := instance.ProcessInstance().Definition.Definitions.Process
	node := pd.GetFlowNodeById(message.Token.ElementId)
	switch nodeT := node.(type) {
	case *bpmn20.TEventBasedGateway:
		tokens, err := engine.publishEventOnEventGateway(ctx, &batch, nodeT, message, instance, variables)
		if err != nil {
			batch.Clear(ctx)
			return newEngineErrorf("failed to publish message %s to event gateway in instance %d. Unexpected node type %T", message.Name, message.ProcessInstanceKey, nodeT)
		}
		err = batch.Flush(ctx)
		if err != nil {
			batch.Clear(ctx)
			return fmt.Errorf("failed to flush publish message b %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	case *bpmn20.TIntermediateCatchEvent:
		tokens, err := engine.publishMessageOnListener(ctx, &batch, nodeT, &message, instance, variables)
		if err != nil {
			message.State = runtime.ActivityStateFailed
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			batch.WriteMessageIncident(ctx, message, instance, err)
			err := batch.Flush(ctx)
			if err != nil {
				batch.Clear(ctx)
				return errors.Join(newEngineErrorf("failed to flush and failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), err)
			}
			return errors.Join(newEngineErrorf("failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			batch.Clear(ctx)
			return fmt.Errorf("failed to flush publish message batch %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	case *bpmn20.TServiceTask, *bpmn20.TSendTask, *bpmn20.TUserTask, *bpmn20.TBusinessRuleTask, *bpmn20.TCallActivity, *bpmn20.TSubProcess:
		tokens, err := engine.handleBoundaryMessage(ctx, &batch, message, instance, variables)
		if err != nil {
			message.State = runtime.ActivityStateFailed
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			batch.WriteMessageIncident(ctx, message, instance, err)
			err := batch.Flush(ctx)
			if err != nil {
				batch.Clear(ctx)
				return errors.Join(newEngineErrorf("failed to flush and failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), err)
			}
			return errors.Join(newEngineErrorf("failed to publish message %s to task %d. ", message.Name, message.ProcessInstanceKey), err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			batch.Clear(ctx)
			return fmt.Errorf("failed to flush publish message batch %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	default:
		msg := fmt.Sprintf("failed to publish message %s to instance %d. Unexpected node type %T", message.Name, message.ProcessInstanceKey, nodeT)
		engine.logger.Error(msg)
		return &BpmnEngineError{Msg: msg}
	}
	// TODO: create something for processing events from API, needs to be able to add tokens to currently running instances or add instance to queue for processing with token updated by API
	// we need to check if token has any more events waiting
	// if so we need to handle interrupting/non interrupting boundary events
}
