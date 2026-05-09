package bpmn

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentInterruptingMessageEventSubProcesses_NoLockContention is a regression
// test mirroring TestConcurrentInterruptingTimerEventSubProcesses_NoLockContention but
// for message-triggered interrupting event subprocesses.
//
// Setup: a process with multiple (6) interrupting message-start event subprocesses
// on the same parent. We publish a message to each of the 6 InstanceMessageSubscriptions
// concurrently so that they all try to start their event subprocess (and cancel the parent) at the same time.
//
// Expectation: only the first event subprocess to actually start manages to
// complete the parent process; the remaining concurrent publishes must be a no-op
// (their subscriptions get short-circuited because the parent / sibling
// subscriptions have already been completed/cancelled). No incidents must be
// raised on the parent instance and the parent must end in Completed state with
// the variable propagated from the winning event subprocess.
func TestConcurrentInterruptingMessageEventSubProcesses_NoLockContention(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// No-op handler for the service task so the main token sits at the service task
	// while the message-triggered event subprocesses fire.
	h := engine.NewTaskHandler().
		Type("input-task-timer-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {
			// do nothing - leave the job pending until an interrupting event subprocess fires
		})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-interrupting-multiple-concurrent.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	require.NotNil(t, instance)
	piKey := instance.ProcessInstance().Key

	// All 6 event-subprocess message-start triggers should be present as InstanceMessageSubscriptions on the parent instance.
	subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateActive)
	require.NoError(t, err)

	instanceSubs := make([]*runtime.InstanceMessageSubscription, 0, 6)
	for _, sub := range subs {
		if instSub, ok := sub.(*runtime.InstanceMessageSubscription); ok &&
			strings.HasPrefix(instSub.MessageSubscription().Name, "globalMessageRef_") {
			instanceSubs = append(instanceSubs, instSub)
		}
	}
	require.Len(t, instanceSubs, 6, "expected 6 event-subprocess message subscriptions on the parent instance")

	// Publish a message to each subscription concurrently. Use a barrier so all publishes start at (approximately) the same instant.
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	var done sync.WaitGroup
	errs := make([]error, len(instanceSubs))
	for i, sub := range instanceSubs {
		done.Add(1)
		go func(idx int, s *runtime.InstanceMessageSubscription) {
			defer done.Done()
			startBarrier.Wait()
			errs[idx] = engine.PublishMessage(t.Context(), s, map[string]any{
				"publisher": idx,
			})
		}(i, sub)
	}
	startBarrier.Done()
	done.Wait()

	// None of the concurrent publishes should return an error: the losers must be cleanly short-circuited
	// (subscription already completed / parent already cancelled), not error out.
	for i, e := range errs {
		assert.NoError(t, e, "publish #%d returned an error", i)
	}

	// Wait for the parent instance to be Completed by the winning interrupting event subprocess and
	// for the event subprocess output mapping to have been propagated to the parent
	assert.Eventually(t, func() bool {
		pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
		if err != nil || pi.ProcessInstance().GetState() != runtime.ActivityStateCompleted {
			return false
		}
		return pi.ProcessInstance().VariableHolder.GetLocalVariable("eventSubProcessResult") == "concurrent-event-subprocess-fired" &&
			pi.ProcessInstance().VariableHolder.GetLocalVariable("messageStartEventOutputVar") == "messageStartEventOutputValue"
	}, 2*time.Second, 25*time.Millisecond,
		"parent process instance should have been completed by an interrupting event subprocess and its output variable propagated")

	pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.ProcessInstance().GetState())

	// No incidents must be recorded on the parent for the concurrent publishes.
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "no incidents should have been raised by the concurrent message publishes")

	// The event subprocess output mapping should have propagated its variable to the parent.
	vars := pi.ProcessInstance().VariableHolder
	assert.Equal(t, "concurrent-event-subprocess-fired", vars.GetLocalVariable("eventSubProcessResult"),
		"eventSubProcessResult should be propagated from the winning event subprocess to the parent")
	// The winning message start event has its own output mapping that targets
	// `messageStartEventOutputVar` on the parent process instance. It must be
	// propagated regardless of which of the 6 concurrent publishes won the race
	// (all 6 start events use the same output mapping).
	assert.Equal(t, "messageStartEventOutputValue", vars.GetLocalVariable("messageStartEventOutputVar"),
		"messageStartEventOutputVar from the winning message start event output mapping should be propagated to the parent")

	// Exactly one of the message subscriptions must have ended up Completed; the rest must have been Terminated
	// (or otherwise no longer Active) as a result of the parent cancellation.
	finalSubs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, finalSubs, "no message subscription should remain Active after the parent has been interrupted")
}
