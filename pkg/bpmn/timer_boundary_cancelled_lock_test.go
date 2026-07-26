package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTriggerBoundaryTimer_AlreadyCancelled_ReleasesInstanceLock is a regression test for a
// deadlock where triggering a boundary timer that had already been cancelled (e.g. the job it
// was attached to was completed just before the timer fired) returned an error from
// processTimerTriggerOnToken without clearing the engine batch, leaving the process instance
// lock held forever. Any subsequent operation on the instance (job completion, cancellation)
// then blocked indefinitely, freezing the engine.
func TestTriggerBoundaryTimer_AlreadyCancelled_ReleasesInstanceLock(t *testing.T) {
	store := inmemory.NewStorage()
	// Long poll delay so the engine scheduler does not race with the manual trigger below.
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(10*time.Second))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-boundary-event-noninterrupting.bpmn")
	require.NoError(t, err)

	// Find the boundary timer and mark it Cancelled to simulate the race where the job was
	// completed (cancelling the timer) right before the scheduler fired it.
	piKey, timers := createInstanceAndGetTimers(t, &engine, store, process)
	target := timers[0]
	require.NotNil(t, target.Token, "boundary timer must carry an execution token")

	target.TimerState = runtime.TimerStateCancelled
	batch := store.NewBatch()
	require.NoError(t, batch.SaveTimer(t.Context(), target))
	require.NoError(t, batch.Flush(t.Context()))

	// Trigger the already-cancelled timer. The engine reports it as an error but must release
	// the process instance lock before returning.
	_, _, err = engine.TriggerTimer(t.Context(), target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timer is already cancelled")

	requireInstanceLockReleased(t, &engine, piKey, "process instance lock must be released after triggering an already-cancelled timer")

	job := findActiveJob(t, store, piKey, "simple-job")
	require.NotNil(t, job, "the service task job must still be active")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	waitForProcessInstanceState(t, store, piKey, runtime.ActivityStateCompleted)
}

func findActiveJob(t *testing.T, store *inmemory.Storage, processInstanceKey int64, jobType string) *runtime.Job {
	t.Helper()
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
	require.NoError(t, err)
	for i := range jobs {
		if jobs[i].Type == jobType && jobs[i].State == runtime.ActivityStateActive {
			return &jobs[i]
		}
	}
	return nil
}

// createInstanceAndGetTimers creates a new process instance from the given definition and
// returns its key along with the single Created boundary timer. The test fails immediately if
// instance creation or timer lookup does not produce exactly one Created timer.
func createInstanceAndGetTimers(t *testing.T, eng *Engine, store *inmemory.Storage, process *runtime.ProcessDefinition) (piKey int64, timers []runtime.Timer) {
	t.Helper()
	instance, err := eng.CreateInstance(t.Context(), process, nil)
	require.NoError(t, err)
	piKey = instance.ProcessInstance().Key

	timers, err = store.FindProcessInstanceTimers(t.Context(), piKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1)
	return
}

// requireInstanceLockReleased verifies that the process instance lock is not held by asserting
// that tryLockInstance succeeds, then immediately releasing it again.
func requireInstanceLockReleased(t *testing.T, eng *Engine, piKey int64, msg string) {
	t.Helper()
	require.NoError(t, eng.runningInstances.tryLockInstance(t.Context(), piKey), msg)
	eng.runningInstances.unlockInstance(piKey)
}
