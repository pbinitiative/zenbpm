package e2e

import (
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
// ahead of their due date, while job completion cancels boundary timers in the DB only. The
// test drives the job completion into that last poll window (shortly before DueAt) so the
// already-loaded timer fires against the cancelled DB state. It then verifies the engine stays
// fully operable: the follow-up job completes and the instance reaches Completed. Several
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
		// manager's in-memory waiting list. The completion cancels the timer in the DB, but the
		// in-memory copy still fires at DueAt and must handle the Cancelled state gracefully.
		raceWindow := timer.DueAt.Add(-400 * time.Millisecond)
		require.True(t, time.Now().Before(raceWindow), "missed timer cancellation race window; retry this attempt")
		waitUntil(t, raceWindow)
		require.NoError(t, completeJob(t, mainJob.Key, nil))

		// Let the due date pass (plus a margin for the trigger to run) so the cancelled in-memory timer has fired by now.
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
