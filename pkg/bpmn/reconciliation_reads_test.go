package bpmn

import (
	"context"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestReconciliationReads(t *testing.T) {
	t.Run("refreshes storage with incomplete instance snapshots", func(t *testing.T) {
		store := &refreshRecordingStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		persistJobCompletionWithoutContinuation(t, &engine, store, job)
		store.refreshCalls = 0

		outcome, err := engine.continueProcessInstanceAfterCommit(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.True(t, outcome.resumedRunningTokens)
		require.Equal(t, 1, store.refreshCalls)
	})

	t.Run("skips refresh when storage returns a complete snapshot", func(t *testing.T) {
		store := &completeRefreshRecordingStorage{refreshRecordingStorage: &refreshRecordingStorage{Storage: inmemory.NewStorage()}}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		persistJobCompletionWithoutContinuation(t, &engine, store, job)
		store.refreshCalls = 0

		outcome, err := engine.continueProcessInstanceAfterCommit(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.True(t, outcome.resumedRunningTokens)
		require.Zero(t, store.refreshCalls)
	})
}

type refreshRecordingStorage struct {
	storage.Storage
	refreshCalls int
}

func (store *refreshRecordingStorage) RefreshProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	store.refreshCalls++
	return store.Storage.RefreshProcessInstance(ctx, instance)
}

type completeRefreshRecordingStorage struct {
	*refreshRecordingStorage
}

func (*completeRefreshRecordingStorage) CompleteProcessInstanceSnapshot() {}

func (store *completeRefreshRecordingStorage) FindProcessInstanceByKey(ctx context.Context, key int64) (runtime.ProcessInstance, error) {
	instance, err := store.Storage.FindProcessInstanceByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := store.Storage.RefreshProcessInstance(ctx, instance); err != nil {
		return nil, err
	}
	return instance, nil
}
