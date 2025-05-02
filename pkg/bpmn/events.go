package bpmn

import (
	"context"
	"fmt"
	"log"
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
	_, err = engine.RunOrContinueInstance(processInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to run process instance: %w", err)
	}

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

type IntermediateCatchEventExecutor struct {
	FlowNodeExecutor
}

func createCheckExclusiveGatewayDoneCommand(originActivity runtime.Activity) (cmds []command) {
	if originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
		evtBasedGatewayActivity := originActivity.(*eventBasedGatewayActivity)
		cmds = append(cmds, checkExclusiveGatewayDoneCommand{
			gatewayActivity: *evtBasedGatewayActivity,
		})
	}
	return cmds
}

func (e *IntermediateCatchEventExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	e.FlowNodeExecutor.Execute(ctx, batch, engine, process, instance)

	var activity runtime.Activity
	nextCommands = []command{}

	element := e.flowNode.element

	ice := element.(bpmn20.TIntermediateCatchEvent)
	createFlowTransitions, activity, err = engine.handleIntermediateCatchEvent(ctx, batch, process, instance, ice, originActivity)

	if ms, ok := activity.(*runtime.MessageSubscription); ok {
		batch.SaveMessageSubscription(ctx, *ms)
		// TODO: this is needed because endevent checks subscriptions and if transaction is not flushed yet it will lock process in active state
		batch.Flush(ctx)
	} else {
		// Handle the case when activity is not a MessageSubscription
		// For example, you can return an error or log a message
		log.Panicf("Unexpected Activity type: %T", activity)
	}
	if err != nil {
		return createFlowTransitions, activity, nextCommands, err
	} else {
		nextCommands = append(nextCommands, createCheckExclusiveGatewayDoneCommand(originActivity)...)
	}

	return createFlowTransitions, activity, nextCommands, err

}

func (engine *Engine) handleIntermediateCatchEvent(ctx context.Context, batch storage.Batch, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (continueFlow bool, activity runtime.Activity, err error) {
	continueFlow = false
	if ice.MessageEventDefinition.Id != "" {
		continueFlow, activity, err = engine.handleIntermediateMessageCatchEvent(ctx, batch, process, instance, ice, originActivity)
	} else if ice.TimerEventDefinition.Id != "" {
		continueFlow, activity, err = engine.handleIntermediateTimerCatchEvent(ctx, batch, instance, ice, originActivity)
	} else if ice.LinkEventDefinition.Id != "" {
		var be bpmn20.FlowNode = ice
		activity = &elementActivity{
			key:     engine.generateKey(),
			state:   runtime.ActivityStateActive, // FIXME: should be Completed?
			element: be,
		}
		throwLinkName := originActivity.Element().(bpmn20.TIntermediateThrowEvent).LinkEventDefinition.Name
		catchLinkName := ice.LinkEventDefinition.Name
		elementVarHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
		if err := propagateProcessInstanceVariables(&elementVarHolder, ice.Output); err != nil {
			msg := fmt.Sprintf("Can't evaluate expression in element id=%s name=%s", ice.Id, ice.Name)
			err = &ExpressionEvaluationError{Msg: msg, Err: err}
		} else {
			continueFlow = throwLinkName == catchLinkName // just stating the obvious
		}
	}
	return continueFlow, activity, err
}

func (engine *Engine) createMessageSubscription(instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent) *runtime.MessageSubscription {
	var be bpmn20.FlowNode = ice
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
