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
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// PublishMessageForInstance publishes a message with a given name and also adds variables to the process instance, which fetches this event
func (engine *Engine) PublishMessageForInstance(ctx context.Context, processInstanceKey int64, messageName string, variables map[string]interface{}) error {
	subs, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, processInstanceKey, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find subscriptions for instance: %d", processInstanceKey), err)
	}
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("no process instance with key: %d", processInstanceKey), err)
	}

	message := runtime.MessageSubscription{}
	for _, mes := range subs {
		if mes.Name == messageName {
			message = mes
			break
		}
	}
	if message.Name == "" {
		engine.logger.Debug(fmt.Sprintf("no message subscription found with name: %s", messageName))
		return nil
	}
	batch := engine.persistence.NewBatch()

	// Token points either to message listener or event based gateway
	pd := instance.Definition.Definitions.Process
	node := pd.GetFlowNodeById(message.Token.ElementId)
	switch nodeT := node.(type) {
	case *bpmn20.TEventBasedGateway:
		tokens, err := engine.publishEventOnEventGateway(ctx, batch, nodeT, message, &instance, variables)
		if err != nil {
			return newEngineErrorf("failed to publish message %s to event gateway in instance %d. Unexpected node type %T", messageName, processInstanceKey, nodeT)
		}
		batch.Flush(ctx)
		return engine.runProcessInstance(ctx, &instance, tokens)
	case *bpmn20.TIntermediateCatchEvent:
		tokens, err := engine.publishMessageOnListener(ctx, batch, nodeT, &message, &instance, variables)
		if err != nil {
			errBatch := engine.persistence.NewBatch()
			message.MessageState = runtime.ActivityStateFailed
			errBatch.SaveMessageSubscription(ctx, message)
			instance.State = runtime.ActivityStateFailed
			errBatch.SaveProcessInstance(ctx, instance)
			if err := errBatch.Flush(ctx); err != nil {
				engine.logger.Error("Failed to save failed message subscription", "msg", message, "err", err)
			}
			return errors.Join(newEngineErrorf("failed to publish message %s to listener in instance %d. ", messageName, processInstanceKey), err)
		}
		batch.Flush(ctx)
		return engine.runProcessInstance(ctx, &instance, tokens)
	default:
		msg := fmt.Sprintf("failed to publish message %s to instance %d. Unexpected node type %T", messageName, processInstanceKey, nodeT)
		engine.logger.Error(msg)
		return &BpmnEngineError{Msg: msg}
	}

	// TODO: create something for processing events from API, needs to be able to add tokens to currently running instances or add instance to queue for processing with token updated by API
	// we need to check if token has any more events waiting
	// if so we need to handle interrupting/non interrupting boundary events
}

func (engine *Engine) publishMessageOnListener(ctx context.Context, batch storage.Batch, listener *bpmn20.TIntermediateCatchEvent, message *runtime.MessageSubscription, instance *runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	token := message.Token
	message.MessageState = runtime.ActivityStateCompleted
	err := batch.SaveMessageSubscription(ctx, *message)
	if err != nil {
		return nil, fmt.Errorf("failed to save message subscription %s on instance %d: %w", message.Name, instance.Key, err)
	}

	vars := runtime.NewVariableHolderForPropagation(&instance.VariableHolder, variables)
	err = propagateProcessInstanceVariables(&vars, listener.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.Key, err)
	}
	err = batch.SaveProcessInstance(ctx, *instance)
	if err != nil {
		return nil, fmt.Errorf("failed to save changes to process instance %d: %w", instance.Key, err)
	}

	tokens, err := engine.handleSimpleTransition(ctx, batch, instance, listener, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process MessageSubscription flow transition %s: %w", listener.GetId(), err)
	}
	return tokens, nil
}

type GatewayEvent interface {
	GetId() string
	GetKey() int64
	GatewayEvent()
}

// publishEventOnEventGateway currently supports gateway events:
// runtime.MessageSubscription
// runtime.Timer
func (engine *Engine) publishEventOnEventGateway(ctx context.Context, batch storage.Batch, gateway *bpmn20.TEventBasedGateway, event GatewayEvent, instance *runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	outgoing := gateway.GetOutgoingAssociation()
	var catchEvent *bpmn20.TIntermediateCatchEvent
	for _, flow := range outgoing {
		if flow.GetTargetRef().GetId() != event.GetId() {
			continue
		}
		e := flow.GetTargetRef().(*bpmn20.TIntermediateCatchEvent)
		_, isMessage := e.EventDefinition.(bpmn20.TMessageEventDefinition)
		_, isTimer := e.EventDefinition.(bpmn20.TTimerEventDefinition)
		isMessageOrTimer := isMessage || isTimer
		if !isMessageOrTimer {
			continue
		}
		catchEvent = e
	}
	if catchEvent == nil {
		return nil, nil
	}
	var token runtime.ExecutionToken
	switch catchEvent.EventDefinition.(type) {
	case bpmn20.TMessageEventDefinition:
		message := event.(runtime.MessageSubscription)
		message.MessageState = runtime.ActivityStateCompleted
		batch.SaveMessageSubscription(ctx, message)
		token = message.Token
		vars := runtime.NewVariableHolder(&instance.VariableHolder, variables)
		propagateProcessInstanceVariables(&vars, catchEvent.Output)
		instance.VariableHolder = vars
		err := batch.SaveProcessInstance(ctx, *instance)
		if err != nil {
			return nil, fmt.Errorf("failed to save changes to process instance %d: %w", instance.Key, err)
		}
	case bpmn20.TTimerEventDefinition:
		timer := event.(runtime.Timer)
		timer.TimerState = runtime.TimerStateTriggered
		batch.SaveTimer(ctx, timer)
		token = timer.Token
	}
	// cancel all the subs on the gateway
	msubs, err := engine.persistence.FindTokenMessageSubscriptions(ctx, token.Key, runtime.ActivityStateActive)
	if err != nil {
		return nil, fmt.Errorf("failed to find message subscriptions to cancel for token %+v", token.Key)
	}
	for _, sub := range msubs {
		if event.GetKey() == sub.ElementInstanceKey {
			continue
		}
		sub.MessageState = runtime.ActivityStateTerminated
		batch.SaveMessageSubscription(ctx, sub)
	}
	tsubs, err := engine.persistence.FindTokenActiveTimerSubscriptions(ctx, token.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to find timer subscriptions to cancel for token %+v", token.Key)
	}
	for _, sub := range tsubs {
		if event.GetKey() == sub.ElementInstanceKey {
			continue
		}
		sub.TimerState = runtime.TimerStateCancelled
		engine.timerManager.removeTimer(sub)
		batch.SaveTimer(ctx, sub)
	}
	tokens, err := engine.handleSimpleTransition(ctx, batch, instance, catchEvent, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process gateway event flow transition %s: %w", event.GetId(), err)
	}

	return tokens, nil
}

func (engine *Engine) createIntermediateMessageCatchEvent(
	ctx context.Context,
	messageWriter storage.MessageStorageWriter,
	instance *runtime.ProcessInstance,
	ice *bpmn20.TIntermediateCatchEvent,
	token runtime.ExecutionToken,
) (runtime.ExecutionToken, error) {
	// TODO: handle input variables
	ms, err := engine.createMessageSubscription(instance, ice)
	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to create intermediate message event: %w", err)
	}
	ms.Token = token
	err = messageWriter.SaveMessageSubscription(ctx, ms)
	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to save new message subscription %+v: %w", ms, err)
	}
	token.State = runtime.TokenStateWaiting
	return token, nil
}

func (engine *Engine) createMessageSubscription(instance *runtime.ProcessInstance, ice *bpmn20.TIntermediateCatchEvent) (runtime.MessageSubscription, error) {
	messageDef := ice.EventDefinition.(bpmn20.TMessageEventDefinition)
	message, err := instance.Definition.Definitions.GetMessageByRef(messageDef.MessageRef)
	if err != nil {
		return runtime.MessageSubscription{}, fmt.Errorf("failed to create message subscription: %w", err)
	}

	ms := runtime.MessageSubscription{
		ElementId:            ice.Id,
		ElementInstanceKey:   engine.generateKey(),
		ProcessDefinitionKey: instance.Definition.Key,
		ProcessInstanceKey:   instance.GetInstanceKey(),
		Name:                 message.Name,
		CreatedAt:            time.Now(),
		MessageState:         runtime.ActivityStateActive,
	}
	return ms, nil
}

func (engine *Engine) handleIntermediateThrowEvent(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, ite *bpmn20.TIntermediateThrowEvent, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	switch ed := ite.EventDefinition.(type) {
	case bpmn20.TMessageEventDefinition:
		activityResult, err := engine.createInternalTask(ctx, batch, instance, ite, currentToken)
		if err != nil {
			currentToken.State = runtime.TokenStateFailed
			return []runtime.ExecutionToken{currentToken}, fmt.Errorf("failed to process MessageThrowEvent %d: %w", currentToken.ElementInstanceKey, err)
		}
		switch activityResult {
		case runtime.ActivityStateActive:
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateCompleted:
			tokens, err := engine.handleSimpleTransition(ctx, batch, instance, ite, currentToken)
			if err != nil {
				return []runtime.ExecutionToken{currentToken}, fmt.Errorf("failed to process MessageThrowEvent flow transition %d: %w", currentToken.ElementInstanceKey, err)
			}
			return tokens, nil
		default:
			panic(fmt.Sprintf("unexpected activity state in handling MessageThrowEvent %s", activityResult))
		}

	case bpmn20.TLinkEventDefinition:
		token, err := engine.handleIntermediateThrowLinkEvent(ctx, instance, ite, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to handle IntermediateThrowLinkEvent: %w", err)
		}
		return []runtime.ExecutionToken{token}, nil
	default:
		panic(fmt.Sprintf("unhandled type for IntermediateThrowEvent EventDefinition: %T", ed))
	}
}
