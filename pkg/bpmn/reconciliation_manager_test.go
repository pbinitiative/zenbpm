package bpmn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconciliationManager(t *testing.T) {
	t.Run("uses conservative defaults", func(t *testing.T) {
		engine := NewEngine()
		t.Cleanup(engine.Stop)

		interval, gracePeriod, batchSize := engine.reconciliationSettings()
		assert.Equal(t, 60*time.Second, interval)
		assert.Equal(t, 60*time.Second, gracePeriod)
		assert.Equal(t, int64(256), batchSize)
	})

	t.Run("job continuation survives request cancellation after commit", func(t *testing.T) {
		store := &cancelAfterFlushStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "before"})
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)

		requestCtx, cancelRequest := context.WithCancel(t.Context())
		store.cancelNextSuccessfulFlush(cancelRequest)
		require.NoError(t, engine.JobCompleteByKey(requestCtx, job.Key, map[string]any{"variable_name": "after"}))
		require.ErrorIs(t, requestCtx.Err(), context.Canceled)

		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})

	t.Run("message continuation survives request cancellation after commit", func(t *testing.T) {
		store := &cancelAfterFlushStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-intermediate-message-catch-event.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"inputFoo": "before"})
		require.NoError(t, err)
		subscriptions, err := store.FindProcessInstanceMessageSubscriptions(
			t.Context(),
			instance.ProcessInstance().Key,
			runtime.ActivityStateActive,
		)
		require.NoError(t, err)
		require.Len(t, subscriptions, 1)

		requestCtx, cancelRequest := context.WithCancel(t.Context())
		store.cancelNextSuccessfulFlush(cancelRequest)
		require.NoError(t, engine.PublishMessage(requestCtx, subscriptions[0], map[string]any{"foo": "after"}))
		require.ErrorIs(t, requestCtx.Err(), context.Canceled)

		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})

	t.Run("incident continuation survives request cancellation after commit", func(t *testing.T) {
		store := &cancelAfterFlushStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)

		callPath := CallPath{}
		taskAHandler := engine.NewTaskHandler().Id("task-a").Handler(callPath.TaskHandler)
		defer engine.RemoveHandler(taskAHandler)
		taskBHandler := engine.NewTaskHandler().Id("task-b").Handler(callPath.TaskHandler)
		defer engine.RemoveHandler(taskBHandler)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"price": 0})
		require.Error(t, err)
		incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		store.ProcessInstances[instance.ProcessInstance().Key].ProcessInstance().VariableHolder.SetLocalVariable("price", 50)

		requestCtx, cancelRequest := context.WithCancel(t.Context())
		store.cancelNextSuccessfulFlush(cancelRequest)
		require.NoError(t, engine.ResolveIncident(requestCtx, incidents[0].Key))
		require.ErrorIs(t, requestCtx.Err(), context.Canceled)

		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})

	t.Run("retrying an already completed job resumes its durable token", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)
		engine.reconciliationManager.stop()

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		persistJobCompletionWithoutContinuation(t, &engine, store, job)

		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, map[string]any{"variable_name": "value"}))

		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})

	t.Run("periodic recovery advances through limited keyset pages", func(t *testing.T) {
		store := &recordingRunningTokenStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		engine.reconciliationBatchSize = 1
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)
		engine.reconciliationManager.stop()

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		first, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "first"})
		require.NoError(t, err)
		second, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "second"})
		require.NoError(t, err)

		firstToken := persistJobCompletionWithoutContinuation(t, &engine, store, requireSinglePendingJob(t, store, first.ProcessInstance().Key))
		secondToken := persistJobCompletionWithoutContinuation(t, &engine, store, requireSinglePendingJob(t, store, second.ProcessInstance().Key))

		engine.reconciliationManager = newReconciliationManager(&engine, 10*time.Millisecond, time.Millisecond, 1)
		engine.reconciliationManager.start()

		require.Eventually(t, func() bool {
			firstPersisted, firstErr := store.GetTokenByKey(t.Context(), firstToken.Key)
			secondPersisted, secondErr := store.GetTokenByKey(t.Context(), secondToken.Key)
			return firstErr == nil && secondErr == nil &&
				firstPersisted.State == runtime.TokenStateCompleted &&
				secondPersisted.State == runtime.TokenStateCompleted
		}, time.Second, 5*time.Millisecond)
		engine.reconciliationManager.stop()

		firstPersisted, err := store.FindProcessInstanceByKey(t.Context(), first.ProcessInstance().Key)
		require.NoError(t, err)
		secondPersisted, err := store.FindProcessInstanceByKey(t.Context(), second.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, firstPersisted.ProcessInstance().State)
		assert.Equal(t, runtime.ActivityStateCompleted, secondPersisted.ProcessInstance().State)

		assertLimitedKeysetScans(t, store.scanCalls(), 1)
	})

	t.Run("periodic recovery waits until a running token exceeds the grace period", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)
		engine.reconciliationManager.stop()

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)
		token := persistJobCompletionWithoutContinuation(t, &engine, store, requireSinglePendingJob(t, store, instance.ProcessInstance().Key))

		manager := newReconciliationManager(&engine, time.Hour, time.Hour, 1)
		manager.reconcileNextBatch()
		persisted, err := store.GetTokenByKey(t.Context(), token.Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.TokenStateRunning, persisted.State)

		manager.gracePeriod = 0
		require.Eventually(t, func() bool {
			manager.reconcileNextBatch()
			persisted, findErr := store.GetTokenByKey(t.Context(), token.Key)
			return findErr == nil && persisted.State == runtime.TokenStateCompleted
		}, time.Second, 5*time.Millisecond)
	})

	t.Run("periodic recovery wraps immediately after a partial page", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)

		const processInstanceKey int64 = 42
		engine.runningInstances.lockInstance(processInstanceKey)
		defer engine.runningInstances.unlockInstance(processInstanceKey)
		require.NoError(t, store.SaveToken(t.Context(), runtime.ExecutionToken{
			Key:                200,
			ProcessInstanceKey: processInstanceKey,
			State:              runtime.TokenStateRunning,
		}))

		manager := newReconciliationManager(&engine, time.Hour, time.Hour, 2)
		manager.cursor = 100
		manager.reconcileNextBatch()

		assert.Zero(t, manager.cursor)
	})

	t.Run("startup recovery advances through limited keyset pages", func(t *testing.T) {
		store := &recordingRunningTokenStorage{Storage: inmemory.NewStorage()}
		seeder := NewEngine(EngineWithStorage(store))
		seeder.reconciliationInterval = time.Hour
		seeder.reconciliationBatchSize = 1
		require.NoError(t, seeder.Start(t.Context()))
		seeder.reconciliationManager.stop()
		t.Cleanup(seeder.Stop)

		definition, err := seeder.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		first, err := seeder.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "first"})
		require.NoError(t, err)
		second, err := seeder.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "second"})
		require.NoError(t, err)
		persistJobCompletionWithoutContinuation(t, &seeder, store, requireSinglePendingJob(t, store, first.ProcessInstance().Key))
		persistJobCompletionWithoutContinuation(t, &seeder, store, requireSinglePendingJob(t, store, second.ProcessInstance().Key))
		seeder.Stop()
		store.resetScanCalls()

		recoveryEngine := NewEngine(EngineWithStorage(store))
		recoveryEngine.reconciliationInterval = time.Hour
		recoveryEngine.reconciliationBatchSize = 1
		require.NoError(t, recoveryEngine.Start(t.Context()))
		t.Cleanup(recoveryEngine.Stop)

		firstPersisted, err := store.FindProcessInstanceByKey(t.Context(), first.ProcessInstance().Key)
		require.NoError(t, err)
		secondPersisted, err := store.FindProcessInstanceByKey(t.Context(), second.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Equal(t, runtime.ActivityStateCompleted, firstPersisted.ProcessInstance().State)
		assert.Equal(t, runtime.ActivityStateCompleted, secondPersisted.ProcessInstance().State)
		assertLimitedKeysetScans(t, store.scanCalls(), 1)
	})

	t.Run("failed nonblocking lock attempt does not leak cache entries", func(t *testing.T) {
		cache := newRunningInstanceCache()
		const instanceKey int64 = 42

		cache.lockInstance(instanceKey)
		assert.False(t, cache.tryLockInstanceOnce(instanceKey))
		cache.unlockInstance(instanceKey)

		cache.mu.Lock()
		defer cache.mu.Unlock()
		assert.Empty(t, cache.processInstances)
	})
}

func requireSinglePendingJob(t *testing.T, store storage.Storage, processInstanceKey int64) runtime.Job {
	t.Helper()
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	return jobs[0]
}

func assertLimitedKeysetScans(t *testing.T, calls []runningTokenScanCall, expectedLimit int64) {
	t.Helper()
	require.NotEmpty(t, calls)
	assert.Condition(t, func() bool {
		for _, call := range calls {
			if call.limit != expectedLimit {
				return false
			}
		}
		return true
	}, "every recovery query must use the configured limit")
	assert.Condition(t, func() bool {
		for _, call := range calls {
			if call.afterTokenKey > 0 {
				return true
			}
		}
		return false
	}, "recovery must advance its keyset cursor")
}

func persistJobCompletionWithoutContinuation(t *testing.T, engine *Engine, store storage.Storage, job runtime.Job) runtime.ExecutionToken {
	t.Helper()
	instance, err := store.FindProcessInstanceByKey(t.Context(), job.ProcessInstanceKey)
	require.NoError(t, err)
	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)

	task := instance.ProcessInstance().Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
	require.NotNil(t, task)
	tokens, err := engine.handleElementTransition(t.Context(), &batch, instance, task, job.Token)
	require.NoError(t, err)

	job.State = runtime.ActivityStateCompleted
	require.NoError(t, batch.SaveJob(t.Context(), job))
	for _, token := range tokens {
		require.NoError(t, batch.SaveToken(t.Context(), token))
	}
	require.NoError(t, batch.SaveProcessInstance(t.Context(), instance))
	require.NoError(t, batch.Flush(t.Context()))
	require.Len(t, tokens, 1)
	return tokens[0]
}

type cancelAfterFlushStorage struct {
	*inmemory.Storage
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (store *cancelAfterFlushStorage) cancelNextSuccessfulFlush(cancel context.CancelFunc) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cancel = cancel
}

func (store *cancelAfterFlushStorage) NewBatch() storage.Batch {
	return &cancelAfterFlushBatch{Batch: store.Storage.NewBatch(), afterFlush: store.afterFlush}
}

func (store *cancelAfterFlushStorage) RefreshProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.Storage.RefreshProcessInstance(ctx, instance)
}

func (store *cancelAfterFlushStorage) afterFlush() {
	store.mu.Lock()
	cancel := store.cancel
	store.cancel = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

type cancelAfterFlushBatch struct {
	storage.Batch
	afterFlush func()
}

func (batch *cancelAfterFlushBatch) Flush(ctx context.Context) error {
	if err := batch.Batch.Flush(ctx); err != nil {
		return err
	}
	batch.afterFlush()
	return nil
}

type runningTokenScanCall struct {
	afterTokenKey int64
	limit         int64
}

type recordingRunningTokenStorage struct {
	*inmemory.Storage
	mu    sync.Mutex
	calls []runningTokenScanCall
}

func (store *recordingRunningTokenStorage) FindRunningTokensAfter(ctx context.Context, afterTokenKey int64, limit int64) ([]runtime.ExecutionToken, error) {
	store.mu.Lock()
	store.calls = append(store.calls, runningTokenScanCall{afterTokenKey: afterTokenKey, limit: limit})
	store.mu.Unlock()
	return store.Storage.FindRunningTokensAfter(ctx, afterTokenKey, limit)
}

func (store *recordingRunningTokenStorage) FindRecoverableRunningTokens(
	ctx context.Context,
	afterTokenKey int64,
	runningBefore time.Time,
	limit int64,
) ([]runtime.ExecutionToken, error) {
	store.mu.Lock()
	store.calls = append(store.calls, runningTokenScanCall{afterTokenKey: afterTokenKey, limit: limit})
	store.mu.Unlock()
	return store.Storage.FindRecoverableRunningTokens(ctx, afterTokenKey, runningBefore, limit)
}

func (store *recordingRunningTokenStorage) scanCalls() []runningTokenScanCall {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]runningTokenScanCall(nil), store.calls...)
}

func (store *recordingRunningTokenStorage) resetScanCalls() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls = nil
}
