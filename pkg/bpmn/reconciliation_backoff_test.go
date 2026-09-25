package bpmn

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestReconciliationBackoff(t *testing.T) {
	t.Run("exponential delay stops at its cap", func(t *testing.T) {
		require.Equal(t, 5*time.Second, reconciliationRetryDelay(5*time.Second, 20*time.Second, 1))
		require.Equal(t, 10*time.Second, reconciliationRetryDelay(5*time.Second, 20*time.Second, 2))
		require.Equal(t, 20*time.Second, reconciliationRetryDelay(5*time.Second, 20*time.Second, 3))
		require.Equal(t, 20*time.Second, reconciliationRetryDelay(5*time.Second, 20*time.Second, 10))
	})

	t.Run("retries technical failures without periodic scans while other instances proceed", func(t *testing.T) {
		store := &flakyReconciliationStorage{
			Storage:           inmemory.NewStorage(),
			failuresRemaining: map[int64]int{11: 2},
			calls:             make(chan reconciliationCall, 4),
		}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		metricsEngine, reader := newMetricsTestEngine(t)
		engine.metrics = metricsEngine.metrics
		manager := newReconciliationManager(&engine, 0, time.Minute, 1)
		manager.retryBaseDelay = 20 * time.Millisecond
		manager.retryMaxDelay = 80 * time.Millisecond
		t.Cleanup(manager.stop)
		manager.wake(11)
		manager.wake(22)
		manager.start()

		calls := make([]reconciliationCall, 0, 4)
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for len(calls) < 4 {
			select {
			case call := <-store.calls:
				calls = append(calls, call)
				if len(calls) == 1 {
					manager.wake(11) // A new wake cannot bypass an active cooldown or lose the scheduled retry.
				}
			case <-deadline.C:
				t.Fatalf("only %d of four expected recovery attempts occurred", len(calls))
			}
		}
		manager.stop()
		require.Equal(t, []int64{11, 22, 11, 11}, []int64{calls[0].key, calls[1].key, calls[2].key, calls[3].key})
		require.GreaterOrEqual(t, calls[2].at.Sub(calls[0].at), 15*time.Millisecond)
		require.GreaterOrEqual(t, calls[3].at.Sub(calls[2].at), 35*time.Millisecond)
		require.Empty(t, manager.retryByKey, "success must clear the retry state")
		require.Equal(t, int64(2), counterValue(t, reader, "reconciliation_failures"))
	})

	t.Run("scan errors do not execute on every tick and reset after success", func(t *testing.T) {
		store := &flakyReconciliationScanStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		manager := newReconciliationManager(&engine, time.Millisecond, time.Minute, 1)
		manager.retryBaseDelay = 25 * time.Millisecond
		manager.retryMaxDelay = 100 * time.Millisecond
		t.Cleanup(manager.stop)

		manager.reconcileNextBatch()
		require.Equal(t, int32(1), store.calls.Load())
		require.Equal(t, uint8(1), manager.scanFailures)
		manager.reconcileNextBatch()
		require.Equal(t, int32(1), store.calls.Load(), "scan must be suppressed during backoff")

		time.Sleep(30 * time.Millisecond)
		manager.reconcileNextBatch()
		require.Equal(t, int32(2), store.calls.Load())
		require.Zero(t, manager.scanFailures)
		require.True(t, manager.nextScan.IsZero())
	})
}

type reconciliationCall struct {
	key int64
	at  time.Time
}

type flakyReconciliationStorage struct {
	*inmemory.Storage
	mu                sync.Mutex
	failuresRemaining map[int64]int
	calls             chan reconciliationCall
}

func (store *flakyReconciliationStorage) FindProcessInstanceByKey(ctx context.Context, key int64) (runtime.ProcessInstance, error) {
	store.mu.Lock()
	fail := store.failuresRemaining[key] > 0
	if fail {
		store.failuresRemaining[key]--
	}
	store.mu.Unlock()
	select {
	case store.calls <- reconciliationCall{key: key, at: time.Now()}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if fail {
		return nil, errors.New("temporary process instance read failure")
	}
	return &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{Key: key, State: runtime.ActivityStateCompleted},
	}, nil
}

type flakyReconciliationScanStorage struct {
	*inmemory.Storage
	calls atomic.Int32
}

func (store *flakyReconciliationScanStorage) FindRecoverableRunningTokens(context.Context, int64, time.Time, int64) ([]runtime.ExecutionToken, error) {
	if store.calls.Add(1) == 1 {
		return nil, errors.New("temporary token scan failure")
	}
	return []runtime.ExecutionToken{}, nil
}
