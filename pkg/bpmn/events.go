package bpmn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) publishMessageOnListener(ctx context.Context, batch *EngineBatch, listener *bpmn20.TIntermediateCatchEvent, message *runtime.MessageSubscription, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	token := message.Token
	message.State = runtime.ActivityStateCompleted
	err := batch.SaveMessageSubscription(ctx, *message)
	if err != nil {
		return nil, err
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	_, err = variableHolder.PropagateOutputVariablesToParent(listener.Output, variables, engine.evaluateExpression)
	if err != nil {
		return nil, fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to save process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process MessageSubscription flow transition %s: %w", listener.GetId(), err)
	}
	return tokens, nil
}

func (engine *Engine) handleBoundaryMessage(ctx context.Context, batch *EngineBatch, message runtime.MessageSubscription, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	var listener *bpmn20.TBoundaryEvent

	for _, be := range instance.ProcessInstance().Definition.Definitions.Process.BoundaryEvent {
		if be.AttachedToRef != message.Token.ElementId {
			continue
		}
		if messageDef, ok := be.EventDefinition.(bpmn20.TMessageEventDefinition); ok {
			m, err := instance.ProcessInstance().Definition.Definitions.GetMessageByRef(messageDef.MessageRef)
			if err != nil {
				return nil, fmt.Errorf("failed to find message %s: %w", messageDef.MessageRef, err)
			}
			if m.Name == message.Name {
				listener = &be
				break
			}
		}
	}
	if listener == nil {
		return nil, fmt.Errorf("failed to find boundary event for message subscription %s", message.Name)
	}

	tokens, err := engine.publishMessageOnBoundaryListener(ctx, batch, listener, &message, instance, variables)

	if err != nil {
		return tokens, errors.Join(newEngineErrorf("failed to publish message %s to listener in instance %d. ", message.Name, instance.ProcessInstance().Key), err)
	}
	return tokens, nil
}

func (engine *Engine) publishMessageOnBoundaryListener(ctx context.Context, batch *EngineBatch, listener *bpmn20.TBoundaryEvent, message *runtime.MessageSubscription, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
	token := message.Token
	message.State = runtime.ActivityStateCompleted
	err := batch.SaveMessageSubscription(ctx, *message)
	if err != nil {
		return nil, fmt.Errorf("failed to save message subscription %s on instance %d: %w", message.Name, instance.ProcessInstance().Key, err)
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if _, err = variableHolder.PropagateOutputVariablesToParent(listener.Output, variables, engine.evaluateExpression); err != nil {
		return nil, fmt.Errorf("failed to propagate variables to process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	if listener.CancellActivity {
		// cancel job
		job, err := engine.persistence.FindJobByElementID(ctx, instance.ProcessInstance().Key, token.ElementId)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("failed to find job for token %d: %w", token.Key, err)
		}

		if !errors.Is(err, storage.ErrNotFound) {
			job.State = runtime.ActivityStateTerminated
			err = batch.SaveJob(ctx, job)
			if err != nil {
				return nil, fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
			}
		}
		err = engine.cancelBoundarySubscriptions(ctx, batch, instance, &token)
		if err != nil {
			return nil, err
		}
		// cancel all called processes
		calledProcesses, err := engine.persistence.FindProcessInstanceByParentExecutionTokenKey(ctx, token.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to find called processes for token %d: %w", token.Key, err)
		}
		for _, calledProcess := range calledProcesses {
			err := engine.cancelSubProcessInstance(ctx, calledProcess, batch)
			if err != nil {
				return nil, err
			}
		}

	} else {
		element := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
		// recreate the message subscription
		_, err := engine.createMessageCatchEvent(ctx, batch, instance, listener.EventDefinition.(bpmn20.TMessageEventDefinition), element, token)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate message subscription: %w", err)
		}
	}

	tokens, err := engine.handleElementTransition(ctx, batch, instance, listener, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process MessageSubscription flow transition %s: %w", listener.GetId(), err)
	}
	return tokens, nil
}

func (engine *Engine) cancelBoundarySubscriptions(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, token *runtime.ExecutionToken) error {
	// cancel other message subscriptions
	subscriptions, err := engine.persistence.FindTokenMessageSubscriptions(ctx, token.Key, runtime.ActivityStateActive)
	if err != nil {
		return fmt.Errorf("failed to find message subscriptions for instance %d: %w", instance.ProcessInstance().Key, err)
	}
	for _, sub := range subscriptions {
		sub.State = runtime.ActivityStateTerminated
		err = batch.SaveMessageSubscription(ctx, sub)
		if err != nil {
			return fmt.Errorf("failed to save changes to message subscription %d: %w", sub.GetKey(), err)
		}
	}

	// cancel other timer subscriptions
	timers, err := engine.persistence.FindTokenActiveTimerSubscriptions(ctx, token.Key)
	if err != nil {
		return fmt.Errorf("failed to find timers for instance %d: %w", instance.ProcessInstance().Key, err)
	}
	for _, timer := range timers {
		timer.TimerState = runtime.TimerStateCancelled
		err = batch.SaveTimer(ctx, timer)
		if err != nil {
			return fmt.Errorf("failed to save changes to timer %d: %w", timer.Key, err)
		}
	}
	return nil
}

type GatewayEvent interface {
	GetId() string
	GetKey() int64
	GatewayEvent()
}

// publishEventOnEventGateway currently supports gateway events:
// runtime.MessageSubscription
// runtime.Timer
func (engine *Engine) publishEventOnEventGateway(ctx context.Context, batch *EngineBatch, gateway *bpmn20.TEventBasedGateway, event GatewayEvent, instance runtime.ProcessInstance, variables map[string]interface{}) ([]runtime.ExecutionToken, error) {
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
		message.State = runtime.ActivityStateCompleted
		err := batch.SaveMessageSubscription(ctx, message)
		if err != nil {
			return nil, fmt.Errorf("failed to save changes to message subscription %d: %w", message.Key, err)
		}
		token = message.Token
		variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
		if _, err = variableHolder.PropagateOutputVariablesToParent(catchEvent.Output, variables, engine.evaluateExpression); err != nil {
			return nil, err
		}
		err = batch.SaveProcessInstance(ctx, instance)
		if err != nil {
			return nil, fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
		}
	case bpmn20.TTimerEventDefinition:
		timer := event.(runtime.Timer)
		timer.TimerState = runtime.TimerStateTriggered
		err := batch.SaveTimer(ctx, timer)
		if err != nil {
			return nil, err
		}
		token = timer.Token
	}
	msubs, err := engine.persistence.FindTokenMessageSubscriptions(ctx, token.Key, runtime.ActivityStateActive)
	if err != nil {
		return nil, fmt.Errorf("failed to find message subscriptions to cancel for token %+v", token.Key)
	}
	for _, sub := range msubs {
		if event.GetKey() == sub.Key {
			continue
		}
		sub.State = runtime.ActivityStateTerminated
		err := batch.SaveMessageSubscription(ctx, sub)
		if err != nil {
			return nil, err
		}
	}
	tsubs, err := engine.persistence.FindTokenActiveTimerSubscriptions(ctx, token.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to find timer subscriptions to cancel for token %+v", token.Key)
	}
	for _, sub := range tsubs {
		if event.GetKey() == sub.Key {
			continue
		}
		sub.TimerState = runtime.TimerStateCancelled
		engine.timerManager.removeTimer(sub)
		err := batch.SaveTimer(ctx, sub)
		if err != nil {
			return nil, err
		}
	}
	tokens, err := engine.handleElementTransition(ctx, batch, instance, catchEvent, token)
	if err != nil {
		return nil, fmt.Errorf("failed to process gateway event flow transition %s: %w", event.GetId(), err)
	}

	return tokens, nil
}

func (engine *Engine) createMessageCatchEvent(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	messageDef bpmn20.TMessageEventDefinition,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) (runtime.ExecutionToken, error) {
	// TODO: handle input variables
	ms, err := engine.createMessageSubscription(instance, messageDef, element, token)
	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to create message subscription: %w", err)
	}

	err = batch.SaveMessageSubscription(ctx, ms)
	if err != nil {
		token.State = runtime.TokenStateFailed
		return token, fmt.Errorf("failed to save new message subscription %+v: %w", ms, err)
	}

	token.State = runtime.TokenStateWaiting
	return token, nil
}

func (engine *Engine) createMessageSubscription(instance runtime.ProcessInstance, messageDef bpmn20.TMessageEventDefinition, element bpmn20.FlowNode, token runtime.ExecutionToken) (runtime.MessageSubscription, error) {
	message, err := instance.ProcessInstance().Definition.Definitions.GetMessageByRef(messageDef.MessageRef)
	if err != nil {
		return runtime.MessageSubscription{}, fmt.Errorf("failed to create message subscription: %w", err)
	}

	correlationKey := message.Extension.CorrelationKey
	if strings.HasPrefix(message.Extension.CorrelationKey, "=") {
		correlationKeyResult, err := engine.evaluateExpression(message.Extension.CorrelationKey, instance.ProcessInstance().VariableHolder.LocalVariables())
		if err != nil {
			token.State = runtime.TokenStateFailed
			return runtime.MessageSubscription{}, fmt.Errorf("failed to evaluate correlation key in message subscription: %w", err)
		}
		ck, ok := correlationKeyResult.(string)
		if !ok {
			token.State = runtime.TokenStateFailed
			return runtime.MessageSubscription{}, fmt.Errorf("result of correlation key  evaluation is not a string: %w", err)
		}
		correlationKey = ck
	}

	ms := runtime.MessageSubscription{
		Key:                  engine.generateKey(),
		ElementId:            element.GetId(),
		ProcessDefinitionKey: instance.ProcessInstance().Definition.Key,
		ProcessInstanceKey:   instance.ProcessInstance().GetInstanceKey(),
		Name:                 message.Name,
		CorrelationKey:       correlationKey,
		State:                runtime.ActivityStateActive,
		CreatedAt:            time.Now(),
		Token:                token,
	}

	return ms, nil
}

func (engine *Engine) handleIntermediateThrowEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, ite *bpmn20.TIntermediateThrowEvent, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
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
			tokens, err := engine.handleElementTransition(ctx, batch, instance, ite, currentToken)
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
