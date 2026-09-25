package bpmn

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestReconciliationWake(t *testing.T) {
	t.Run("delivers every queued instance without a periodic scan", func(t *testing.T) {
		const instanceCount = 128 // More than the old channel's capacity of 64.
		store := &wakeRecordingStorage{Storage: inmemory.NewStorage(), calls: make(chan int64, instanceCount)}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		manager := newReconciliationManager(&engine, 0, time.Minute, 1)
		t.Cleanup(manager.stop)

		for key := int64(1); key <= instanceCount; key++ {
			manager.wake(key)
			manager.wake(key) // Duplicate wakeups for a queued instance are coalesced.
		}
		require.Len(t, manager.wakeQueue, instanceCount)
		manager.start()

		seen := make(map[int64]struct{}, instanceCount)
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		for len(seen) < instanceCount {
			select {
			case key := <-store.calls:
				if _, duplicate := seen[key]; duplicate {
					t.Fatalf("instance %d was processed twice", key)
				}
				seen[key] = struct{}{}
			case <-deadline.C:
				t.Fatalf("only %d of %d queued instances were processed", len(seen), instanceCount)
			}
		}
	})

	t.Run("retries a wakeup after the instance lock becomes free", func(t *testing.T) {
		const instanceKey int64 = 42
		store := &wakeRecordingStorage{Storage: inmemory.NewStorage(), calls: make(chan int64, 1)}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		manager := newReconciliationManager(&engine, 0, time.Minute, 1)
		t.Cleanup(manager.stop)

		engine.runningInstances.lockInstance(instanceKey)
		locked := true
		t.Cleanup(func() {
			if locked {
				engine.runningInstances.unlockInstance(instanceKey)
			}
		})
		manager.start()
		manager.wake(instanceKey)
		require.Eventually(t, func() bool {
			manager.wakeMu.Lock()
			defer manager.wakeMu.Unlock()
			return len(manager.wakeQueue) == 0
		}, time.Second, time.Millisecond, "manager did not attempt the queued wakeup")

		// Keep the lock beyond the first retry so the initial wake cannot be
		// mistaken for a successful delayed delivery.
		timer := time.NewTimer(2 * reconciliationBusyRetryDelay)
		defer timer.Stop()
		select {
		case key := <-store.calls:
			t.Fatalf("instance %d was loaded while its lock was held", key)
		case <-timer.C:
		}
		engine.runningInstances.unlockInstance(instanceKey)
		locked = false

		select {
		case key := <-store.calls:
			require.Equal(t, instanceKey, key)
		case <-time.After(2 * time.Second):
			t.Fatal("wakeup was lost after the instance lock became free")
		}
	})
}

type wakeRecordingStorage struct {
	*inmemory.Storage
	calls chan int64
}

func (store *wakeRecordingStorage) FindProcessInstanceByKey(ctx context.Context, key int64) (runtime.ProcessInstance, error) {
	select {
	case store.calls <- key:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{Key: key, State: runtime.ActivityStateCompleted},
	}, nil
}
