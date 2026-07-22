package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNonInterruptingMessageEventSubprocess_CanFireMultipleTimes verifies that a
// non-interrupting message event subprocess can be triggered multiple times for the same
// parent process instance. Per BPMN 2.0 semantics, non-interrupting event subprocesses must
// remain active and re-triggerable for the entire lifetime of their parent — each published
// message matching the subscription name and correlation key should spawn a new event
// subprocess instance that runs concurrently with the parent and any sibling event subprocess
// instances, without terminating the parent or consuming the subscription permanently.
//
// The test publishes the same message three times in succession and asserts that each publish
// finds an Active InstanceMessageSubscription (renewed after each fire), spawns a new child
// instance, and leaves the parent in a healthy state.
func TestNonInterruptingEventSubprocessDoesNotCompleteActiveParentElement(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	mainHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting").
		Handler(func(_ ActivatedJob) {})
	defer engine.RemoveHandler(mainHandler)

	process, err := engine.LoadFromFile(t.Context(),
		"./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	correlationKey := "correlation-key-event-subprocess-non-interrupting"
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageNonInterruptingRef", &correlationKey, nil))

	require.Eventually(t, func() bool {
		persisted, findErr := store.FindProcessInstanceByKey(t.Context(), piKey)
		return findErr == nil && persisted.ProcessInstance().VariableHolder.GetLocalVariable("nonInterruptingExecuted") == true
	}, 2*time.Second, 25*time.Millisecond, "event subprocess should propagate its output to the parent scope")

	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), piKey, true)
	require.NoError(t, err)

	var activeTask *runtime.FlowElementInstance
	eventSubProcessHistoryCount := 0
	for i := range flowElements {
		switch flowElements[i].ElementId {
		case "service-task-1":
			activeTask = &flowElements[i]
		case "Activity_0adcic4":
			eventSubProcessHistoryCount++
		}
	}

	require.NotNil(t, activeTask, "active parent service task should be in history")
	assert.Nil(t, activeTask.CompletedAt, "event subprocess completion must not complete an unrelated active parent element")
	assert.Nil(t, activeTask.OutputVariables, "event subprocess output must not be written to an unrelated active parent element")
	assert.Zero(t, eventSubProcessHistoryCount, "event subprocess activation must not add a new row to parent history")
}

func TestConcurrentNonInterruptingEventSubprocessesDelayParentCompletion(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	mainHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting").
		Handler(func(_ ActivatedJob) {})
	defer engine.RemoveHandler(mainHandler)
	subHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting-2").
		Handler(func(_ ActivatedJob) {})
	defer engine.RemoveHandler(subHandler)

	process, err := engine.LoadFromFile(t.Context(),
		"./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-subprocess-service-task.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	correlationKey := "correlation-key-event-subprocess-non-interrupting"
	for range 2 {
		require.NoError(t, engine.PublishMessageByName(t.Context(), "messageNonInterruptingRef", &correlationKey, nil))
	}

	children := make([]*runtime.SubProcessInstance, 0, 2)
	require.Eventually(t, func() bool {
		children = children[:0]
		for _, processInstance := range store.ProcessInstancesSnapshot() {
			child, ok := processInstance.(*runtime.SubProcessInstance)
			if ok && child.ParentProcessExecutionToken.ProcessInstanceKey == piKey {
				children = append(children, child)
			}
		}
		if len(children) != 2 {
			return false
		}
		for _, child := range children {
			jobs, findErr := store.FindPendingProcessInstanceJobs(t.Context(), child.ProcessInstance().Key)
			if findErr != nil || len(jobs) != 1 {
				return false
			}
		}
		return true
	}, 2*time.Second, 25*time.Millisecond, "two event subprocesses should be waiting on their service tasks")

	mainJobs, err := store.FindPendingProcessInstanceJobs(t.Context(), piKey)
	require.NoError(t, err)
	require.Len(t, mainJobs, 1)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), mainJobs[0].Key, nil))

	require.Eventually(t, func() bool {
		persisted, findErr := store.FindProcessInstanceByKey(t.Context(), piKey)
		activeTokens, tokenErr := store.GetActiveTokensForProcessInstance(t.Context(), piKey)
		return findErr == nil && tokenErr == nil && persisted.ProcessInstance().State == runtime.ActivityStateActive && len(activeTokens) == 0
	}, 2*time.Second, 25*time.Millisecond, "parent should remain active only because event subprocess children are active")

	firstJobs, err := store.FindPendingProcessInstanceJobs(t.Context(), children[0].ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, firstJobs, 1)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), firstJobs[0].Key, nil))

	require.Eventually(t, func() bool {
		persisted, findErr := store.FindProcessInstanceByKey(t.Context(), piKey)
		first, firstErr := store.FindProcessInstanceByKey(t.Context(), children[0].ProcessInstance().Key)
		second, secondErr := store.FindProcessInstanceByKey(t.Context(), children[1].ProcessInstance().Key)
		return findErr == nil && firstErr == nil && secondErr == nil &&
			persisted.ProcessInstance().State == runtime.ActivityStateActive &&
			first.ProcessInstance().State == runtime.ActivityStateCompleted &&
			second.ProcessInstance().State == runtime.ActivityStateActive
	}, 2*time.Second, 25*time.Millisecond, "first child completion must not complete the parent while its sibling is active")

	secondJobs, err := store.FindPendingProcessInstanceJobs(t.Context(), children[1].ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, secondJobs, 1)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), secondJobs[0].Key, nil))

	require.Eventually(t, func() bool {
		persisted, findErr := store.FindProcessInstanceByKey(t.Context(), piKey)
		first, firstErr := store.FindProcessInstanceByKey(t.Context(), children[0].ProcessInstance().Key)
		second, secondErr := store.FindProcessInstanceByKey(t.Context(), children[1].ProcessInstance().Key)
		return findErr == nil && firstErr == nil && secondErr == nil &&
			persisted.ProcessInstance().State == runtime.ActivityStateCompleted &&
			first.ProcessInstance().State == runtime.ActivityStateCompleted &&
			second.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 25*time.Millisecond, "last child completion should complete the parent")
}

func TestNonInterruptingMessageEventSubprocess_CanFireMultipleTimes(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Keep the main service task pending — we don't want the parent to complete before we've finished publishing the messages.
	mainHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting").
		Handler(func(_ ActivatedJob) {})
	defer engine.RemoveHandler(mainHandler)

	// Keep the event subprocess service task pending so each event subprocess instance stays
	// alive and observable as a child of the parent.
	subHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting-2").
		Handler(func(_ ActivatedJob) {})
	defer engine.RemoveHandler(subHandler)

	process, err := engine.LoadFromFile(t.Context(),
		"./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-subprocess-service-task.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	const messageName = "messageNonInterruptingRef"
	correlationKey := "correlation-key-event-subprocess-non-interrupting"
	const fires = 3

	for i := 0; i < fires; i++ {
		// Each publish must succeed: a fresh Active InstanceMessageSubscription must exist
		// after consuming the previous one.
		require.NoErrorf(t,
			engine.PublishMessageByName(t.Context(), messageName, &correlationKey, nil),
			"publish #%d must find an active subscription for the non-interrupting event subprocess", i+1)

		// After each publish exactly one Active subscription must remain — the renewal —
		// and `i+1` Completed subscriptions must accumulate (the consumed ones).
		assert.Eventuallyf(t, func() bool {
			active, _ := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateActive)
			completed, _ := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateCompleted)
			return len(active) == 1 && len(completed) == i+1
		}, 2*time.Second, 25*time.Millisecond,
			"after publish #%d expected 1 Active + %d Completed InstanceMessageSubscriptions", i+1, i+1)
	}

	// Each publish must have spawned a separate event subprocess instance (children of the
	// parent). With non-interrupting semantics they all coexist and stay pending on their
	// service task.
	require.Eventuallyf(t, func() bool {
		parentTokens, err := store.GetAllTokensForProcessInstance(t.Context(), piKey)
		if err != nil {
			return false
		}

		children := 0
		for _, token := range parentTokens {
			childInstances, err := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), token.Key)
			if err != nil {
				return false
			}
			for _, child := range childInstances {
				if child.Type() == runtime.ProcessTypeSubProcess {
					children++
				}
			}
		}
		return children == fires
	}, 2*time.Second, 25*time.Millisecond,
		"expected %d event-subprocess child instances, one per published message", fires)

	// The parent must NOT have been failed/terminated by any of the non-interrupting fires.
	persisted, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.NotEqual(t, runtime.ActivityStateFailed, persisted.ProcessInstance().GetState())
	assert.NotEqual(t, runtime.ActivityStateTerminated, persisted.ProcessInstance().GetState())

	// And no incidents must have been raised by the message handling.
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "no incidents should have been raised by non-interrupting message publishes")
}
