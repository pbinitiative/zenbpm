package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func (engine *Engine) PublishMessageByName(ctx context.Context, name string, correlationKey *string, variables map[string]any) error {
	message, err := engine.persistence.FindMessageSubscriptionByName(ctx, name, correlationKey, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find active message subscription with name: %s", name), err)
	}
	return engine.PublishMessage(ctx, message, variables)
}

func (engine *Engine) PublishMessageByKey(ctx context.Context, subscriptionKey int64, variables map[string]any) error {
	message, err := engine.persistence.FindMessageSubscriptionByKey(ctx, subscriptionKey, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find active message subscription: %d", subscriptionKey), err)
	}
	return engine.PublishMessage(ctx, message, variables)
}

// PublishMessage publishes a message given by subscription key and also adds variables to the process instance, which fetches this event
func (engine *Engine) PublishMessage(ctx context.Context, message runtime.MessageSubscription, variables map[string]interface{}) (retErr error) {
	switch message := message.(type) {
	case *runtime.DefinitionMessageSubscription:
		_, err := engine.CreateInstanceWithStartingElements(
			ctx,
			message.ProcessDefinitionKey,
			[]string{message.ElementId},
			variables,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to process DefinitionMessageSubscription %+v: %w", message, err)
		}
	case *runtime.InstanceMessageSubscription:
		err := engine.PublishMessageOnEventSubprocess()
		if err != nil {
			return fmt.Errorf("failed to process InstanceMessageSubscription %+v: %w", message, err)
		}
	case *runtime.TokenMessageSubscription:
		err := engine.PublishMessageOnToken(ctx, message, variables)
		if err != nil {
			return fmt.Errorf("failed to process TokenMessageSubscription %+v: %w", message, err)
		}
	default:
		return fmt.Errorf("message type not supported")
	}
	return nil
	// TODO: create something for processing events from API, needs to be able to add tokens to currently running instances or add instance to queue for processing with token updated by API
	// we need to check if token has any more events waiting
	// if so we need to handle interrupting/non interrupting boundary events
}

func (engine *Engine) PublishMessageOnToken(ctx context.Context, message *runtime.TokenMessageSubscription, variables map[string]any) (retErr error) {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, message.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process instance with key: %d", message.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to create engine batch: %w", err)
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	//refresh
	messageSub, err := engine.persistence.FindMessageSubscriptionByKey(ctx, message.Key, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find active message subscription: %d", message.Key), err)
	}
	message, ok := messageSub.(*runtime.TokenMessageSubscription)
	if !ok {
		return fmt.Errorf("message type after refresh not supported")
	}
	switch message.State {
	case runtime.ActivityStateCompleted:
		return errors.Join(newEngineErrorf("message subscription already completed: %d", message.Key), err)
	case runtime.ActivityStateTerminated:
		return errors.Join(newEngineErrorf("message subscription already terminated: %d", message.Key), err)
	default:
		// do nothing
	}

	// Token points either to message listener or event based gateway
	pd := instance.ProcessInstance().Definition.Definitions.Process
	node := pd.GetFlowNodeById(message.Token.ElementId)
	switch nodeT := node.(type) {
	case *bpmn20.TEventBasedGateway:
		tokens, err := engine.publishEventOnEventGateway(ctx, &batch, nodeT, message, instance, variables)
		if err != nil {
			return fmt.Errorf("failed to publishEventOnEventGateway %+v: %w", message, err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			return fmt.Errorf("failed to flush publish message b %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	case *bpmn20.TIntermediateCatchEvent:
		tokens, err := engine.publishMessageOnListener(ctx, &batch, nodeT, message, instance, variables)
		if err != nil {
			message.State = runtime.ActivityStateFailed
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			batch.WriteMessageIncident(ctx, message, instance, err)
			err = batch.Flush(ctx)
			if err != nil {
				return errors.Join(newEngineErrorf("failed to flush and failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), err)
			}
			return errors.Join(newEngineErrorf("failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			return fmt.Errorf("failed to flush publish message batch %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	case *bpmn20.TServiceTask, *bpmn20.TSendTask, *bpmn20.TUserTask, *bpmn20.TBusinessRuleTask, *bpmn20.TCallActivity, *bpmn20.TSubProcess:
		tokens, err := engine.handleBoundaryMessage(ctx, &batch, message, instance, variables)
		if err != nil {
			message.State = runtime.ActivityStateFailed
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			batch.WriteMessageIncident(ctx, message, instance, err)
			flushErr := batch.Flush(ctx)
			if flushErr != nil {
				return errors.Join(newEngineErrorf("failed to flush and failed to publish message %s to listener in instance %d. ", message.Name, message.ProcessInstanceKey), flushErr)
			}
			return errors.Join(newEngineErrorf("failed to publish message %s to task %d. ", message.Name, message.ProcessInstanceKey), err)
		}
		err = batch.Flush(ctx)
		if err != nil {
			return fmt.Errorf("failed to flush publish message batch %+v: %w", message, err)
		}
		return engine.RunProcessInstance(ctx, instance, tokens)
	default:
		msg := fmt.Sprintf("failed to publish message %s to instance %d. Unexpected node type %T", message.Name, message.ProcessInstanceKey, nodeT)
		engine.logger.Error(msg)
		return &BpmnEngineError{Msg: msg}
	}
}

func (engine *Engine) PublishMessageOnEventSubprocess() {

}
