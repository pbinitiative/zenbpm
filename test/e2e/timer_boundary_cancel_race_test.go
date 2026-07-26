package e2e

import (
	"net/http"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimerBoundaryCancelledRightBeforeFiringKeepsEngineResponsive is an e2e regression test
// for a deadlock where a non-interrupting boundary timer that was cancelled (by completing the
// job of its attached activity) but had already been loaded into the timer manager's in-memory
// waiting list still fired, hit the "timer is already cancelled" error path and leaked the
// process instance lock. Every subsequent operation on the instance (completing the next job,
// cancelling the instance) then hung and the engine appeared frozen.
//
// The engine polls due timers into memory one poll cycle (POLL_TIMER_DELAY_SECONDS=1 in e2e)
// ahead of their due date. Job completion now cancels boundary timers through the engine's
// single cancelTimer method, which also removes the already-loaded in-memory copy from the
// timer manager. A short-lived post-flush cancellation tombstone rejects stale Created results
// from an in-flight poll; persisted-state validation remains the final guard if a waiter already
// reached the trigger channel. The test drives job completion into that last poll window
// (shortly before DueAt) and verifies the engine stays fully operable: the follow-up job completes
// and the instance reaches Completed. Several
// instances are exercised to make hitting the race window overwhelmingly likely.
func TestTimerBoundaryCancelledRightBeforeFiringKeepsEngineResponsive(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile        = "testdata/timer/timer-boundary-noninterrupting-cancel-race.bpmn"
		mainTaskElement = "main-task"
		finalTaskElem   = "final-task"
		attempts        = 3
	)

	definition := deployAndGetUniqueProcessDefinition(t, bpmnFile)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	exercisedAttempts := 0
	for i := range attempts {
		instance, err := createProcessInstance(t, &definition.Key, nil)
		require.NoError(t, err)
		require.NotZero(t, instance.Key, "Process instance key should not be zero")

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		mainJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, mainTaskElement)

		timer := waitForCreatedProcessInstanceTimer(t, store, instance.Key)

		// Complete the main job shortly before the boundary timer's due date, inside the last
		// poll window, when the timer has most likely already been loaded into the timer
		// manager's in-memory waiting list. The completion must cancel the timer in the DB and
		// remove the in-memory copy without allowing an in-flight poll to reinsert it.
		raceWindow := timer.DueAt.Add(-400 * time.Millisecond)
		if !time.Now().Before(raceWindow) {
			t.Logf("missed timer cancellation race window (attempt %d); cleaning up and retrying", i)
			cleanupOwnedProcessInstance(t, instance.Key)
			continue
		}
		waitUntil(t, raceWindow)
		exercisedAttempts++
		require.NoError(t, completeJob(t, mainJob.Key, nil))

		// Let the due date pass (plus a margin) so a leftover in-memory timer would have fired by now.
		waitUntil(t, timer.DueAt.Add(1500*time.Millisecond))

		_, err = listProcessDefinitions(t)
		require.NoError(t, err, "engine must keep serving process definition list after the cancelled timer fired")

		// The instance must remain fully operable: the follow-up job is active and completable.
		// Before the fix this hung on the leaked process instance lock.
		completeJobForElementId(t, instance.Key, finalTaskElem, nil)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			pi, err := getProcessInstance(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, pi.State,
				"process instance should be Completed after the final job is done")
		}, 15*time.Second, 100*time.Millisecond,
			"process instance %d should reach Completed state (attempt %d)", instance.Key, i)

		cancelled, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCancelled)
		require.NoError(t, err)
		require.Len(t, cancelled, 1, "the boundary timer should be Cancelled for instance %d", instance.Key)
	}
	require.Positive(t, exercisedAttempts, "at least one attempt must exercise the timer cancellation window")
}

// TestProcessInstanceCancelledRightBeforeBoundaryTimerFiresKeepsEngineResponsive covers the
// process-instance cancellation path of the engine's single timer-cancel method (cancelTimer):
// cancelling an instance whose non-interrupting boundary timer is already loaded into the timer
// manager's in-memory waiting list must remove that in-memory copy as well, so the timer does
// not fire against the cancelled DB state and the engine stays fully responsive afterwards.
// The cancellation is driven into the last poll window (shortly before DueAt), when the timer
// has already been polled into memory. Several instances are exercised to make hitting the window overwhelmingly likely.
func TestProcessInstanceCancelledRightBeforeBoundaryTimerFiresKeepsEngineResponsive(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile        = "testdata/timer/timer-boundary-noninterrupting-cancel-race.bpmn"
		mainTaskElement = "main-task"
		// A slow runner can miss the -400ms race window on some attempts; keep enough attempts
		// that at least one is overwhelmingly likely to exercise the cancellation window.
		attempts = 5
	)

	definition := deployAndGetUniqueProcessDefinition(t, bpmnFile)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	exercisedAttempts := 0
	for i := range attempts {
		instance, err := createProcessInstance(t, &definition.Key, nil)
		require.NoError(t, err)
		require.NotZero(t, instance.Key, "Process instance key should not be zero")

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, mainTaskElement)

		timer := waitForCreatedProcessInstanceTimer(t, store, instance.Key)

		// Cancel the process instance shortly before the boundary timer's due date, inside the
		// last poll window, when the timer has most likely already been loaded into the timer
		// manager's in-memory waiting list.
		raceWindow := timer.DueAt.Add(-400 * time.Millisecond)
		if !time.Now().Before(raceWindow) {
			t.Logf("missed timer cancellation race window (attempt %d); cleaning up and retrying", i)
			cleanupOwnedProcessInstance(t, instance.Key)
			continue
		}
		waitUntil(t, raceWindow)
		exercisedAttempts++

		cancelResp, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, cancelResp.StatusCode(),
			"process instance %d cancellation should succeed", instance.Key)

		// Let the due date pass (plus a margin) so a leftover in-memory timer would have fired by now.
		waitUntil(t, timer.DueAt.Add(1500*time.Millisecond))

		_, err = listProcessDefinitions(t)
		require.NoError(t, err, "engine must keep serving process definition list after the instance cancellation")

		pi, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, zenclient.ProcessInstanceStateTerminated, pi.State,
			"process instance %d should stay Terminated after the timer due date passed", instance.Key)

		cancelled, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCancelled)
		require.NoError(t, err)
		require.Len(t, cancelled, 1, "the boundary timer should be Cancelled for instance %d", instance.Key)
		created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
		require.NoError(t, err)
		require.Empty(t, created, "no Created timer should remain for cancelled instance %d", instance.Key)
	}
	require.Positive(t, exercisedAttempts, "at least one attempt must exercise the timer cancellation window")
}

// waitForCreatedProcessInstanceTimer waits until the process instance has exactly one timer in Created state and returns it.
func waitForCreatedProcessInstanceTimer(t testing.TB, store storage.Storage, processInstanceKey int64) bpmnruntime.Timer {
	t.Helper()

	var timer bpmnruntime.Timer
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, bpmnruntime.TimerStateCreated)
		if !assert.NoError(collect, err) {
			return
		}
		if !assert.Len(collect, timers, 1, "expected exactly one Created timer for instance %d", processInstanceKey) {
			return
		}
		timer = timers[0]
	}, 5*time.Second, 50*time.Millisecond, "boundary timer should be Created for instance %d", processInstanceKey)
	return timer
}

// waitUntil blocks until the given point in time has passed. It intentionally uses
// require.Eventually instead of time.Sleep so the wait is bounded and cancellable.
func waitUntil(t testing.TB, deadline time.Time) {
	t.Helper()
	require.Eventually(t, func() bool {
		return time.Now().After(deadline)
	}, time.Until(deadline)+5*time.Second, 10*time.Millisecond, "expected wall clock to pass %s", deadline)
}
