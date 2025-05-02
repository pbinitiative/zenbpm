package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// PublishEventForInstance publishes a message with a given name and also adds variables to the process instance, which fetches this event
func (engine *Engine) PublishEventForInstance(processInstanceKey int64, messageName string, variables map[string]interface{}) error {
	processInstance, err := engine.FindProcessInstance(processInstanceKey)
	if err != nil {
		return fmt.Errorf("no process instance with key: %d: %w", processInstanceKey, err)
	}
	event := runtime.CatchEvent{
		CaughtAt:   time.Now(),
		Name:       messageName,
		Variables:  variables,
		IsConsumed: false,
	}
	processInstance.CaughtEvents = append(processInstance.CaughtEvents, event)
	err = engine.persistence.SaveProcessInstance(context.TODO(), processInstance)
	if err != nil {
		return fmt.Errorf("failed to save process instance: %w", err)
	}
	// TODO: should we run the instance after event is published otherwise caller needs to do it

	return nil
}

func (engine *Engine) handleIntermediateMessageCatchEvent(
	ctx context.Context,
	messageWriter storage.MessageStorageWriter,
	process *runtime.ProcessDefinition,
	instance *runtime.ProcessInstance,
	ice bpmn20.TIntermediateCatchEvent,
	originActivity runtime.Activity,
) (continueFlow bool, ms *runtime.MessageSubscription, err error) {
	messageSubscriptions, err := engine.persistence.FindActivityMessageSubscriptions(ctx, originActivity.GetKey(), runtime.ActivityStateActive)
	if len(messageSubscriptions) > 0 {
		ms = &messageSubscriptions[0]
	}

	if originActivity != nil && originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
		ebgActivity := originActivity.(*eventBasedGatewayActivity)
		if ebgActivity.OutboundCompleted() {
			ms.MessageState = runtime.ActivityStateWithdrawn // FIXME: is this correct?
			return false, ms, err
		}
	}

	if ms == nil {
		ms = engine.createMessageSubscription(instance, ice)
		ms.OriginActivity = originActivity
		messageWriter.SaveMessageSubscription(ctx, *ms)
	}

	messages := process.Definitions.Messages
	caughtEvent := findMatchingCaughtEvent(messages, instance, ice)

	if caughtEvent != nil {
		caughtEvent.IsConsumed = true
		for k, v := range caughtEvent.Variables {
			instance.SetVariable(k, v)
		}
		if err := evaluateLocalVariables(&instance.VariableHolder, ice.Output); err != nil {
			ms.MessageState = runtime.ActivityStateFailed
			instance.State = runtime.ActivityStateFailed
			evalErr := &ExpressionEvaluationError{
				Msg: fmt.Sprintf("Error evaluating expression in intermediate message catch event element id='%s' name='%s'", ice.Id, ice.Name),
				Err: err,
			}
			return false, ms, evalErr
		}
		ms.MessageState = runtime.ActivityStateCompleted
		if ms.OriginActivity != nil {
			originActivity := instance.FindActivity(ms.OriginActivity.GetKey())
			if originActivity != nil && originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
				ebgActivity := originActivity.(*eventBasedGatewayActivity)
				ebgActivity.SetOutboundCompleted(ice.Id)
			}
		}
		return true, ms, err
	}
	return false, ms, err
}

func (engine *Engine) createMessageSubscription(instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent) *runtime.MessageSubscription {
	var be bpmn20.FlowNode = &ice
	ms := &runtime.MessageSubscription{
		ElementId:            ice.Id,
		ElementInstanceKey:   engine.generateKey(),
		ProcessDefinitionKey: instance.Definition.Key,
		ProcessInstanceKey:   instance.GetInstanceKey(),
		Name:                 ice.Name,
		CreatedAt:            time.Now(),
		MessageState:         runtime.ActivityStateActive,
		BaseElement:          be,
	}
	return ms
}

// find first matching catchEvent
func findMatchingCaughtEvent(messages []bpmn20.TMessage, instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent) *runtime.CatchEvent {
	msgName := findMessageNameById(messages, ice.MessageEventDefinition.MessageRef)
	for i := 0; i < len(instance.CaughtEvents); i++ {
		var caughtEvent = &instance.CaughtEvents[i]
		if !caughtEvent.IsConsumed && msgName == caughtEvent.Name {
			return caughtEvent
		}
	}
	return nil
}

func findMessageNameById(messages []bpmn20.TMessage, msgId string) string {
	for _, message := range messages {
		if message.Id == msgId {
			return message.Name
		}
	}
	return ""
}
