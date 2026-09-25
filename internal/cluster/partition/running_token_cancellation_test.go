package partition

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRunningTokenCancellation(t *testing.T) {
	t.Run("rqlite job continuation survives request cancellation after flush", func(t *testing.T) {
		partition, conf, clientMgr, testStore, server := prepareTestSetup(t, false)
		t.Cleanup(func() {
			require.NoError(t, partition.Stop())
			require.NoError(t, server.Close())
		})
		db := newTestDB(t, partition, conf, clientMgr, testStore, "test-running-token-cancellation")
		store := &cancelAfterRqliteFlushStorage{Storage: db}
		engine := bpmn.NewEngine(bpmn.EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		require.NoError(t, engine.Start(t.Context()))

		definition, err := engine.LoadFromFile(t.Context(), filepath.Join("..", "..", "..", "pkg", "bpmn", "test-cases", "simple_task.bpmn"))
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "before"})
		require.NoError(t, err)
		jobs, err := db.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		requestCtx, cancelRequest := context.WithCancel(t.Context())
		defer cancelRequest()
		store.cancelAfterNextFlush(cancelRequest)
		require.NoError(t, engine.JobCompleteByKey(requestCtx, jobs[0].Key, map[string]any{"variable_name": "after"}))
		require.ErrorIs(t, requestCtx.Err(), context.Canceled)

		persisted, err := db.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})
}

type cancelAfterRqliteFlushStorage struct {
	storage.Storage
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (store *cancelAfterRqliteFlushStorage) cancelAfterNextFlush(cancel context.CancelFunc) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cancel = cancel
}

func (store *cancelAfterRqliteFlushStorage) NewBatch() storage.Batch {
	return &cancelAfterRqliteFlushBatch{Batch: store.Storage.NewBatch(), afterFlush: store.afterFlush}
}

func (store *cancelAfterRqliteFlushStorage) afterFlush() {
	store.mu.Lock()
	cancel := store.cancel
	store.cancel = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

type cancelAfterRqliteFlushBatch struct {
	storage.Batch
	afterFlush func()
}

func (batch *cancelAfterRqliteFlushBatch) Flush(ctx context.Context) error {
	if err := batch.Batch.Flush(ctx); err != nil {
		return err
	}
	batch.afterFlush()
	return nil
}
