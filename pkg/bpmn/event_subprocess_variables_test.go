package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterruptingTimerEventSubprocess_PropagatesStartEventOutputVariables(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(100*time.Millisecond))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Service task handler that never completes — keeps the main token at the
	// service task so the PT1S timer-start event of the interrupting event
	// subprocess gets a chance to fire.
	h := engine.NewTaskHandler().
		Type("input-task-timer-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {
			// intentionally left blank
		})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// Wait for the PT1S timer to fire, the interrupting event subprocess to run
	// to completion and the parent process instance to be completed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
		if err == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	persistedParent, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateCompleted, persistedParent.ProcessInstance().GetState(),
		"parent process instance must be completed by the interrupting timer event subprocess")

	assert.Equal(t, "timerStartEventValue", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("timerStartEventVar"),
		"timer start event output variable timerStartEventVar must be persisted on the parent process instance")

	// The event subprocess element itself has output mappings producing subProcessResult / timerInterrupted / interruptedBy.
	// They are applied by handleParentProcessContinuationForSubProcess as the subprocess completes and must end up on the parent.
	assert.Equal(t, "timer-fired", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("subProcessResult"),
		"subprocess output mapping subProcessResult must be propagated to the parent")
	assert.Equal(t, true, persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("timerInterrupted"),
		"subprocess output mapping timerInterrupted must be propagated to the parent")
	assert.Equal(t, "timer-event-subprocess", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("interruptedBy"),
		"subprocess output mapping interruptedBy must be propagated to the parent")

	// The event subprocess instance itself must also see the start-event output
	// variable in its local variables (so the subprocess body can use it).
	var subProcessInstance runtime.ProcessInstance
	for _, pi := range store.ProcessInstances {
		parentKey := pi.GetParentProcessInstanceKey()
		if parentKey != nil && *parentKey == piKey {
			subProcessInstance = pi
			break
		}
	}
	require.NotNil(t, subProcessInstance, "expected event subprocess instance to be persisted")
	assert.Equal(t, "timerStartEventValue", subProcessInstance.ProcessInstance().VariableHolder.GetLocalVariable("timerStartEventVar"),
		"event subprocess instance must have the start-event output variable timerStartEventVar in its local variables")

	// No incidents should be raised for the parent process instance.
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "no incident should have been raised on the parent process instance")
}

func TestInterruptingMessageEventSubprocess_PropagatesStartEventOutputVariables(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Service task handler that never completes — keeps the main token pinned
	// at the service task so the message-start event of the interrupting event
	// subprocess gets a chance to fire.
	h := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {
			// intentionally left blank
		})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// Wait for the message-start event subscription to be created on the
	// parent before publishing — the service task activates asynchronously
	// and the InstanceMessageSubscription is written as part of the same
	// activation batch.
	correlationKey := "correlation-key-event-subprocess-1"
	require.Eventually(t, func() bool {
		_, err := store.FindMessageSubscriptionByName(t.Context(), "globalMessageRef", &correlationKey, runtime.ActivityStateActive)
		return err == nil
	}, 2*time.Second, 25*time.Millisecond,
		"event subprocess message subscription was expected to be created")

	// Message payload variables — they must NOT leak into the parent or the
	// subprocess scope because the start event has its own (non-empty) output
	// mapping which replaces the default "propagate everything" behaviour.
	messageVars := map[string]any{
		"customerId": "cust-42",
		"reason":     "manual-interrupt",
	}
	err = engine.PublishMessageByName(t.Context(), "globalMessageRef", &correlationKey, messageVars)
	require.NoError(t, err)

	// Wait for the interrupting event subprocess to run to completion and the
	// parent process instance to be completed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
		if err == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	persistedParent, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateCompleted, persistedParent.ProcessInstance().GetState(),
		"parent process instance must be completed by the interrupting message event subprocess")

	// Start-event output mapping must be persisted on the parent.
	assert.Equal(t, "messageStartEventOutputValue", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("messageStartEventOutputVar"),
		"message start event output variable messageStartEventOutputVar must be persisted on the parent process instance")

	// The event subprocess element itself has output mappings producing
	// subProcessResult / messageInterrupted / interruptedBy. They are applied
	// by handleParentProcessContinuationForSubProcess as the subprocess
	// completes and must end up on the parent.
	assert.Equal(t, "message-fired", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("subProcessResult"),
		"subprocess output mapping subProcessResult must be propagated to the parent")
	assert.Equal(t, true, persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("messageInterrupted"),
		"subprocess output mapping messageInterrupted must be propagated to the parent")
	assert.Equal(t, "message-event-subprocess", persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("interruptedBy"),
		"subprocess output mapping interruptedBy must be propagated to the parent")

	// Raw (unmapped) message payload variables must not leak to the parent.
	assert.Nil(t, persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("customerId"),
		"raw message payload variable customerId must not leak to the parent because the start event has an explicit output mapping")
	assert.Nil(t, persistedParent.ProcessInstance().VariableHolder.GetLocalVariable("reason"),
		"raw message payload variable reason must not leak to the parent because the start event has an explicit output mapping")

	// The event subprocess instance itself must also see the mapped
	// start-event output variable in its local variables (so the subprocess
	// body can use it).
	var subProcessInstance runtime.ProcessInstance
	for _, pi := range store.ProcessInstances {
		parentKey := pi.GetParentProcessInstanceKey()
		if parentKey != nil && *parentKey == piKey {
			subProcessInstance = pi
			break
		}
	}
	require.NotNil(t, subProcessInstance, "expected event subprocess instance to be persisted")
	assert.Equal(t, "messageStartEventOutputValue", subProcessInstance.ProcessInstance().VariableHolder.GetLocalVariable("messageStartEventOutputVar"),
		"event subprocess instance must have the start-event output variable messageStartEventOutputVar in its local variables")

	// No incidents should be raised for the parent process instance.
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "no incident should have been raised on the parent process instance")
}
