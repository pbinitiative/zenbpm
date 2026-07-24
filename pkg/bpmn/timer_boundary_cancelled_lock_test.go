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

	instance, err := engine.CreateInstance(t.Context(), process, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// Find the boundary timer and mark it Cancelled to simulate the race where the job was
	// completed (cancelling the timer) right before the scheduler fired it.
	timers, err := store.FindProcessInstanceTimers(t.Context(), piKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1)
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

	require.NoError(t, engine.runningInstances.tryLockInstance(t.Context(), piKey),
		"process instance lock must be released after triggering an already-cancelled timer")
	engine.runningInstances.unlockInstance(piKey)

	job := findActiveJob(t, store, piKey, "simple-job")
	require.NotNil(t, job, "the service task job must still be active")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	require.Eventually(t, func() bool {
		pi, err := store.FindProcessInstanceByKey(t.Context(), piKey)
		return err == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted
	}, 5*time.Second, 100*time.Millisecond, "process instance should complete after the job is done")
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
