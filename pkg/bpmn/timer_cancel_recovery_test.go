package bpmn

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCancelTimer_BatchFlushFails_TimerIsRepolledIntoTimerManager verifies the recovery path of
// engine.cancelTimer: cancelling a timer removes its in-memory copy from the timer manager
// before the batch holding the DB state change is flushed. If that flush then fails, the DB
// timer is left in Created state while the timer manager no longer holds it in memory. The
// timer manager must pick the (now overdue) Created timer up again on its next poll and process
// it, so no timer is ever lost.
func TestCancelTimer_BatchFlushFails_TimerIsRepolledIntoTimerManager(t *testing.T) {
	store := &flakyFlushStorage{Storage: inmemory.NewStorage()}
	// Poll delay of 2s: the PT1S boundary timer is due before the first poll, so it is
	// registered in-memory immediately on creation, and the recovery re-poll happens ~2s
	// after the failed cancellation.
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(2*time.Second))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer-boundary-event-noninterrupting.bpmn")
	require.NoError(t, err)

	piKey, timers := createInstanceAndGetTimers(t, &engine, store.Storage, process)
	timerKey := timers[0].Key

	require.Eventually(t, func() bool {
		return timerManagerHasWaitingTimer(engine.timerManager, timerKey)
	}, 2*time.Second, 10*time.Millisecond, "boundary timer should be loaded into the timer manager's in-memory waiting list")

	job := findActiveJob(t, store.Storage, piKey, "simple-job")
	require.NotNil(t, job, "the service task job must be active")

	// The cancellation must happen while the boundary timer is still Created and waiting in
	// memory. If test setup already consumed the whole PT1S window (e.g. on a loaded CI runner),
	// the timer would fire first and the race could not be exercised.
	require.True(t, time.Now().Before(timers[0].DueAt.Add(-200*time.Millisecond)),
		"test setup took too long; boundary timer would fire before the job completion — cannot exercise the race")

	store.failFlush.Store(true)
	err = engine.JobCompleteByKey(t.Context(), job.Key, nil)
	require.ErrorContains(t, err, "injected flush failure")
	store.failFlush.Store(false)

	dbTimer, err := store.GetTimer(t.Context(), timerKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.TimerStateCreated, dbTimer.TimerState,
		"DB timer must stay in Created state when the cancelling batch fails to flush")
	assert.False(t, timerManagerHasWaitingTimer(engine.timerManager, timerKey),
		"in-memory timer must have been removed from the timer manager by the cancellation attempt")
	requireInstanceLockReleased(t, &engine, piKey, "process instance lock must be released after the failed job completion")

	require.Eventually(t, func() bool {
		dbTimer, err := store.GetTimer(t.Context(), timerKey)
		return err == nil && dbTimer.TimerState == runtime.TimerStateTriggered
	}, 10*time.Second, 50*time.Millisecond, "overdue Created timer should be re-polled into the timer manager and triggered")

	job = findActiveJob(t, store.Storage, piKey, "simple-job")
	require.NotNil(t, job, "the service task job must still be active after the failed completion")
	require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

	waitForProcessInstanceState(t, store, piKey, runtime.ActivityStateCompleted)
}

func TestCancelTimer_BatchFlushSucceeds_InFlightPollCannotReinsertTimer(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(time.Hour))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	timer := runtime.Timer{
		Key:        42,
		TimerState: runtime.TimerStateCreated,
		CreatedAt:  time.Now(),
		DueAt:      time.Now().Add(time.Hour),
	}
	storageBatch := store.NewBatch()
	require.NoError(t, storageBatch.SaveTimer(t.Context(), timer))
	require.NoError(t, storageBatch.Flush(t.Context()))
	engine.timerManager.addWaitingTimer(timer)

	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)
	require.NoError(t, engine.cancelTimer(t.Context(), &batch, timer))
	require.False(t, timerManagerHasWaitingTimer(engine.timerManager, timer.Key),
		"cancellation should immediately remove the existing in-memory timer")

	// This value represents a Created result captured by a poll before the cancellation commit.
	// Once the flush succeeds, its tombstone must reject that stale result without waiting for
	// another database operation.
	require.NoError(t, batch.Flush(t.Context()))
	persisted, err := store.GetTimer(t.Context(), timer.Key)
	require.NoError(t, err)
	require.Equal(t, runtime.TimerStateCancelled, persisted.TimerState)
	engine.timerManager.addWaitingTimer(timer)
	require.False(t, timerManagerHasWaitingTimer(engine.timerManager, timer.Key),
		"successful flush must reject a stale timer returned by an in-flight poll")

	require.NoError(t, engine.timerManager.pollTimers(time.Now().Add(2*time.Hour)))
	engine.timerManager.addWaitingTimer(timer)
	require.True(t, timerManagerHasWaitingTimer(engine.timerManager, timer.Key),
		"the cancellation tombstone should be cleared after a later successful poll")
}

// timerManagerHasWaitingTimer reports whether the timer with the given key is currently in the
// timer manager's in-memory waiting list.
func timerManagerHasWaitingTimer(tm *timerManager, timerKey int64) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for _, wt := range tm.waitingTimers {
		if wt.timer.Key == timerKey {
			return true
		}
	}
	return false
}

// flakyFlushStorage wraps the in-memory storage and injects batch flush failures on demand.
type flakyFlushStorage struct {
	*inmemory.Storage
	failFlush atomic.Bool
}

func (s *flakyFlushStorage) NewBatch() storage.Batch {
	return &flakyFlushBatch{Batch: s.Storage.NewBatch(), store: s}
}

// flakyFlushBatch delegates everything to the wrapped batch but fails Flush while armed.
type flakyFlushBatch struct {
	storage.Batch
	store *flakyFlushStorage
}

func (b *flakyFlushBatch) Flush(ctx context.Context) error {
	if b.store.failFlush.Load() {
		return fmt.Errorf("injected flush failure")
	}
	return b.Batch.Flush(ctx)
}
