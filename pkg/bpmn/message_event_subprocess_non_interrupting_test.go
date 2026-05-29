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
func TestNonInterruptingMessageEventSubprocess_CanFireMultipleTimes(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Keep the main service task pending — we don't want the parent to complete before we've finished publishing the messages.
	mainHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting").
		Handler(func(job ActivatedJob) {})
	defer engine.RemoveHandler(mainHandler)

	// Keep the event subprocess service task pending so each event subprocess instance stays
	// alive and observable as a child of the parent.
	subHandler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-non-interrupting-2").
		Handler(func(job ActivatedJob) {})
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
