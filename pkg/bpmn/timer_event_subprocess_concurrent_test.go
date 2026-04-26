package bpmn

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentInterruptingTimerEventSubProcesses_NoLockContention is a regression test for a
// deadlock/race between sibling interrupting timer-start event subprocesses and the parent
// process continuation goroutine spawned by the first event subprocess to complete.
//
// Setup: a process with multiple interrupting timer event subprocesses on the same parent that
// all become eligible to fire at almost exactly the same time (same PT1S duration).
//
// The fix re-checks the timer state both before any work (start-event processes) and after
// acquiring the parent lock (event-subprocess start) and bails out cleanly when the timer was
// already triggered or cancelled.
func TestConcurrentInterruptingTimerEventSubProcesses_NoLockContention(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(200*time.Millisecond))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Register a handler for the service task that intentionally does NOT complete the job so
	// that the main token sits at the service task while the timer-start event subprocesses fire
	// and one of them interrupts the parent.
	h := engine.NewTaskHandler().
		Type("input-task-timer-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {
			// do nothing - leave the job pending until an interrupting event subprocess fires
		})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-event-subprocess-interrupting-multiple-concurrent.bpmn")
	require.NoError(t, err)
	require.NotNil(t, process)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	require.NotNil(t, instance)

	// Wait long enough for the PT1S/PT2S timers to fire and be processed.
	deadline := time.Now().Add(2 * time.Second)
	piKey := instance.ProcessInstance().Key
	for time.Now().Before(deadline) {
		pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
		if err == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	// One of the interrupting event subprocesses must have fired and completed the parent
	assert.Equal(t, runtime.ActivityStateCompleted, pi.ProcessInstance().GetState(),
		"parent process instance should have been completed by an interrupting event subprocess")

	// All sibling timers attached to the parent process instance must end up in either
	// Triggered (the one whose event subprocess fired) or Cancelled (the others, cancelled by
	// the interrupting cancel) state. None should remain Created and none should fail in a way
	// that produces a leftover incident.
	parentTimers, err := store.FindProcessInstanceTimers(t.Context(), piKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, parentTimers, "no parent-instance timer should remain in Created state")

	// Sanity check: there should be no incidents recorded for this process instance.
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Empty(t, incidents, "no incidents should have been raised by the concurrent timers")

	// The event subprocess output mapping should have propagated its variable to the parent.
	vars := pi.ProcessInstance().VariableHolder
	assert.Equal(t, "concurrent-event-subprocess-fired", vars.GetLocalVariable("eventSubProcessResult"),
		"eventSubProcessResult should be propagated from the winning event subprocess to the parent")
}

// TestCreateStartEventSubProcessOnTimerStartEvent_TimerAlreadyCancelled exercises the fast-path
// added by the fix: when the parent process has already been cancelled / the timer has been
// flagged as Cancelled (e.g. by a sibling interrupting event subprocess that fired first),
// firing the timer again must be a no-op and must not return an error.
func TestCreateStartEventSubProcessOnTimerStartEvent_TimerAlreadyCancelled(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(10*time.Second))
	require.NoError(t, engine.Start(context.Background()))
	defer engine.Stop()

	// Register a no-op handler so the main token stays at the service task.
	h := engine.NewTaskHandler().
		Type("input-task-timer-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {})
	defer engine.RemoveHandler(h)

	process, err := engine.LoadFromFile(context.Background(), "./test-cases/timer-event-subprocess-interrupting-multiple-concurrent.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(context.Background(), process.Key, nil)
	require.NoError(t, err)

	piKey := instance.ProcessInstance().Key

	// Find one of the timer-start-event timers and mark it Cancelled to simulate a race where
	// a sibling interrupting event subprocess already fired and cancelled this one.
	timers, err := store.FindProcessInstanceTimers(context.Background(), piKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.NotEmpty(t, timers)

	target := timers[0]
	target.TimerState = runtime.TimerStateCancelled
	batch := store.NewBatch()
	require.NoError(t, batch.SaveTimer(context.Background(), target))
	require.NoError(t, batch.Flush(context.Background()))

	// Trigger the timer through the public engine API. It must short-circuit cleanly.
	resInstance, tokens, err := engine.TriggerTimer(context.Background(), target)
	require.NoError(t, err, "triggering an already-cancelled timer must not error")
	assert.Nil(t, resInstance)
	assert.Nil(t, tokens)

	// Timer state must remain Cancelled (no overwrite to Triggered).
	refreshed, err := store.GetTimer(context.Background(), target.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TimerStateCancelled, refreshed.TimerState)
}
