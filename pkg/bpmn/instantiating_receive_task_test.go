package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const instantiatingReceiveTaskBpmn = "./test-cases/receive_task/receive-task-boundary-message-interrupting-instantiating.bpmn"

const (
	instantiatingReceiveTaskElement = "ReceiveTask_1efx577"
	instantiatingReceiveMessageName = "globalMsgRefMsgIInst"
)

// activeTokenSubscriptionsForElement counts the active TokenMessageSubscriptions registered against the given
// element id (the receive task's own subscription and any attached boundary message subscriptions are both
// registered against the receive task element).
func activeTokenSubscriptionsForElement(store *inmemory.Storage, elementId string) int {
	count := 0
	for _, sub := range store.MessageSubscriptions {
		tokenSub, ok := sub.(*runtime.TokenMessageSubscription)
		if !ok {
			continue
		}
		if tokenSub.MessageSubscription().ElementId == elementId &&
			tokenSub.MessageSubscription().State == runtime.ActivityStateActive {
			count++
		}
	}
	return count
}

// TestInstantiatingReceiveTask_RegisterCreatesDefinitionSubscription verifies that deploying a process whose
// only entry point is an instantiating ReceiveTask registers exactly one definition-level message subscription
// (with no correlation key, like a message start event) and that such a definition cannot be started manually.
func TestInstantiatingReceiveTask_RegisterCreatesDefinitionSubscription(t *testing.T) {
	cases := []struct {
		file        string
		messageName string
	}{
		{"./test-cases/receive_task/receive-task-boundary-message-interrupting-instantiating.bpmn", "globalMsgRefMsgIInst"},
		{"./test-cases/receive_task/receive-task-boundary-message-noninterrupting-instantiating.bpmn", "globalMsgRefMsgNIInst"},
		{"./test-cases/receive_task/receive-task-boundary-timer-interrupting-instantiating.bpmn", "globalMsgRefTimerIInst"},
		{"./test-cases/receive_task/receive-task-boundary-timer-noninterrupting-instantiating.bpmn", "globalMsgRefTimerNIInst"},
	}

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			store := inmemory.NewStorage()
			engine := NewEngine(EngineWithStorage(store))
			require.NoError(t, engine.Start(t.Context()))
			defer engine.Stop()

			def, err := engine.LoadFromFile(t.Context(), tc.file)
			require.NoError(t, err)
			require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

			subs := definitionMessageSubscriptionsForDefinition(store, def.Key)
			require.Len(t, subs, 1, "exactly one definition-level subscription should be registered for the instantiating receive task")
			assert.Equal(t, instantiatingReceiveTaskElement, subs[0].MessageSubscription().ElementId,
				"the definition subscription must point at the receive task element")
			assert.Equal(t, tc.messageName, subs[0].MessageSubscription().Name)
			assert.Equal(t, runtime.ActivityStateActive, subs[0].MessageSubscription().State)

			// A process definition with an instantiating receive task must not be startable manually.
			_, err = engine.CreateInstanceByKey(t.Context(), def.Key, map[string]any{})
			require.Error(t, err, "manual start must be rejected for definitions with an instantiating receive task")
		})
	}
}

// TestInstantiatingReceiveTask_PublishCreatesInstanceAndGuardsDuplicates verifies the full instantiation
// behaviour: publishing the message (with a nil correlation key) creates a new active process instance waiting
// on the receive task, consumes the definition subscription, and—while that instance is active—a second
// publish does not create another instance. After re-arming, a fresh definition subscription is available.
func TestInstantiatingReceiveTask_PublishCreatesInstanceAndGuardsDuplicates(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	// Publishing with a nil correlation key routes to the definition subscription and creates an instance.
	require.NoError(t, engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, map[string]any{
		"messagePayloadVar": "value",
	}))

	activeInstances := activeInstancesForDefinition(store, def.Key)
	require.Len(t, activeInstances, 1, "publishing the instantiating message must create exactly one active process instance")
	instance := activeInstances[0]
	assert.Equal(t, "value", instance.ProcessInstance().VariableHolder.GetLocalVariable("messagePayloadVar"),
		"variables published with the instantiating message must be propagated to the new instance")

	// The receive task waits: its own subscription plus the boundary message subscription are both active.
	assert.Equal(t, 2, activeTokenSubscriptionsForElement(store, instantiatingReceiveTaskElement),
		"the receive task and its boundary message subscription should both be active")

	// The definition subscription was consumed and is intentionally NOT renewed while the instance is active.
	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		assert.Equal(t, runtime.ActivityStateCompleted, sub.MessageSubscription().State,
			"the definition subscription must be Completed while an instance is active")
	}

	// Guard: while the instance is active there is no active definition subscription, so a second publish
	// must not create another instance.
	err = engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, map[string]any{})
	require.Error(t, err, "publishing while an instance is active must not create a second instance")
	require.Len(t, activeInstancesForDefinition(store, def.Key), 1,
		"no additional process instance may be created while one is already active")

	// Once the instance is no longer active, re-arming makes a fresh definition subscription available so a
	// subsequent message can create a new instance.
	require.NoError(t, engine.rearmInstantiatingReceiveTaskSubscriptions(t.Context(), def))
	activeDefSubs := 0
	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		if sub.MessageSubscription().State == runtime.ActivityStateActive {
			activeDefSubs++
		}
	}
	assert.Equal(t, 1, activeDefSubs, "re-arming must create exactly one fresh active definition subscription")
}

func TestInstantiatingReceiveTask_CancelRearmsDefinitionSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	require.NoError(t, engine.PublishMessageByName(t.Context(), instantiatingReceiveMessageName, nil, map[string]any{}))
	activeInstances := activeInstancesForDefinition(store, def.Key)
	require.Len(t, activeInstances, 1)

	require.NoError(t, engine.CancelInstanceByKey(t.Context(), activeInstances[0].ProcessInstance().Key))

	activeDefSubs := 0
	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		if sub.MessageSubscription().State == runtime.ActivityStateActive {
			activeDefSubs++
		}
	}
	assert.Equal(t, 1, activeDefSubs, "cancelling the active instance must re-arm the instantiating receive task")
}

func TestInstantiatingReceiveTask_CreateInstanceFailureRearmsDefinitionSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))
	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		sub.MessageSubscription().State = runtime.ActivityStateCompleted
		require.NoError(t, store.SaveMessageSubscription(t.Context(), sub))
	}

	brokenSubscription := &runtime.DefinitionMessageSubscription{
		MessageSubscriptionData: runtime.MessageSubscriptionData{
			Key:                  engine.generateKey(),
			ElementId:            "missing_receive_task",
			Name:                 instantiatingReceiveMessageName,
			State:                runtime.ActivityStateActive,
			ProcessDefinitionKey: def.Key,
		},
	}
	require.NoError(t, store.SaveMessageSubscription(t.Context(), brokenSubscription))

	err = engine.publishMessageOnReceiveTaskInstanceCreation(t.Context(), brokenSubscription, map[string]any{})
	require.Error(t, err)

	activeDefSubs := 0
	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		data := sub.MessageSubscription()
		if data.ElementId == instantiatingReceiveTaskElement && data.State == runtime.ActivityStateActive {
			activeDefSubs++
		}
	}
	assert.Equal(t, 1, activeDefSubs, "failed instantiating instance creation must restore the real receive task subscription")
}

func TestInstantiatingReceiveTask_RearmIsScopedToDefinitionAndElement(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	def, err := engine.LoadFromFile(t.Context(), instantiatingReceiveTaskBpmn)
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), def.Key))

	for _, sub := range definitionMessageSubscriptionsForDefinition(store, def.Key) {
		sub.MessageSubscription().State = runtime.ActivityStateCompleted
		require.NoError(t, store.SaveMessageSubscription(t.Context(), sub))
	}

	foreignSubscription := &runtime.DefinitionMessageSubscription{
		MessageSubscriptionData: runtime.MessageSubscriptionData{
			Key:                  engine.generateKey(),
			ElementId:            "ReceiveTask_other_definition",
			Name:                 instantiatingReceiveMessageName,
			State:                runtime.ActivityStateActive,
			ProcessDefinitionKey: def.Key + 1000,
		},
	}
	require.NoError(t, store.SaveMessageSubscription(t.Context(), foreignSubscription))

	require.NoError(t, engine.rearmInstantiatingReceiveTaskSubscriptions(t.Context(), def))

	targetActive := 0
	foreignActive := 0
	for _, sub := range store.MessageSubscriptions {
		defSub, ok := sub.(*runtime.DefinitionMessageSubscription)
		if !ok || defSub.State != runtime.ActivityStateActive || defSub.Name != instantiatingReceiveMessageName {
			continue
		}
		switch defSub.ProcessDefinitionKey {
		case def.Key:
			if defSub.ElementId == instantiatingReceiveTaskElement {
				targetActive++
			}
		case foreignSubscription.ProcessDefinitionKey:
			foreignActive++
		}
	}
	assert.Equal(t, 1, targetActive, "target receive task must be re-armed even when another definition has the same active message name")
	assert.Equal(t, 1, foreignActive, "foreign active definition subscription must be left untouched")
}

func TestInstantiatingReceiveTask_ManualStartAllowedWhenPlainStartEventExists(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	def, err := engine.LoadFromBytes(t.Context(), []byte(mixedPlainStartAndInstantiatingReceiveTaskBPMN), engine.generateKey())
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), def.Key, map[string]any{})
	require.NoError(t, err)
	assert.NotNil(t, instance)
}

// activeInstancesForDefinition returns all active process instances in the store for the given definition.
func activeInstancesForDefinition(store *inmemory.Storage, processDefinitionKey int64) []runtime.ProcessInstance {
	var out []runtime.ProcessInstance
	for _, pi := range store.ProcessInstances {
		piData := pi.ProcessInstance()
		if piData.Definition != nil && piData.Definition.Key == processDefinitionKey &&
			piData.GetState() == runtime.ActivityStateActive {
			out = append(out, pi)
		}
	}
	return out
}

const mixedPlainStartAndInstantiatingReceiveTaskBPMN = `<?xml version="1.0" encoding="UTF-8"?>
		<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zen="https://github.com/pbinitiative/zenbpm-bpmn-moddle" id="Definitions_mixed_receive" targetNamespace="http://bpmn.io/schema/bpmn">
		  <bpmn:message id="Message_instantiating" name="mixedInstantiatingMessage">
			<bpmn:extensionElements>
			  <zen:correlationKey>mixed-correlation-key</zen:correlationKey>
			</bpmn:extensionElements>
		  </bpmn:message>
		  <bpmn:process id="Process_mixed_plain_and_receive" isExecutable="true">
			<bpmn:startEvent id="StartEvent_plain">
			  <bpmn:outgoing>Flow_start_end</bpmn:outgoing>
			</bpmn:startEvent>
			<bpmn:endEvent id="EndEvent_plain">
			  <bpmn:incoming>Flow_start_end</bpmn:incoming>
			</bpmn:endEvent>
			<bpmn:sequenceFlow id="Flow_start_end" sourceRef="StartEvent_plain" targetRef="EndEvent_plain" />
			<bpmn:receiveTask id="ReceiveTask_instantiating_mixed" messageRef="Message_instantiating" instantiate="true">
			  <bpmn:outgoing>Flow_receive_end</bpmn:outgoing>
			</bpmn:receiveTask>
			<bpmn:endEvent id="EndEvent_receive">
			  <bpmn:incoming>Flow_receive_end</bpmn:incoming>
			</bpmn:endEvent>
			<bpmn:sequenceFlow id="Flow_receive_end" sourceRef="ReceiveTask_instantiating_mixed" targetRef="EndEvent_receive" />
		  </bpmn:process>
		</bpmn:definitions>`
