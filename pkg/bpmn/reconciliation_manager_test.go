package bpmn

import (
	"context"
	"errors"
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

	t.Run("publishes reconciliation manager before startup recovery", func(t *testing.T) {
		store := newBlockingStartupRecoveryStorage()
		engine := NewEngine(EngineWithStorage(store))
		startResult := make(chan error, 1)
		t.Cleanup(func() {
			store.unblock()
			engine.Stop()
		})

		go func() {
			startResult <- engine.Start(t.Context())
		}()

		select {
		case <-store.entered:
		case <-time.After(time.Second):
			t.Fatal("startup recovery was not reached")
		}
		publishedManager := engine.currentReconciliationManager()
		store.unblock()

		require.NoError(t, <-startResult)
		require.NotNil(t, publishedManager)
		assert.Same(t, publishedManager, engine.currentReconciliationManager())
	})

	t.Run("stop interrupts startup recovery", func(t *testing.T) {
		store := newBlockingStartupRecoveryStorage()
		engine := NewEngine(EngineWithStorage(store))
		startResult := make(chan error, 1)
		t.Cleanup(func() {
			store.unblock()
			engine.Stop()
		})

		go func() {
			startResult <- engine.Start(t.Context())
		}()
		select {
		case <-store.entered:
		case <-time.After(time.Second):
			t.Fatal("startup recovery was not reached")
		}

		stopDone := make(chan struct{})
		go func() {
			engine.Stop()
			close(stopDone)
		}()
		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatal("Stop did not interrupt startup recovery")
		}

		require.ErrorIs(t, <-startResult, context.Canceled)
		assert.Nil(t, engine.currentReconciliationManager())
	})

	t.Run("synchronizes manager replacement with wake delivery", func(t *testing.T) {
		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		t.Cleanup(engine.Stop)
		first := newReconciliationManager(&engine, time.Hour, time.Hour, 1)
		second := newReconciliationManager(&engine, time.Hour, time.Hour, 1)
		defer first.stop()
		defer second.stop()
		require.Nil(t, engine.swapReconciliationManager(first))

		var waitGroup sync.WaitGroup
		waitGroup.Add(2)
		start := make(chan struct{})
		go func() {
			defer waitGroup.Done()
			<-start
			for i := 0; i < 1_000; i++ {
				engine.wakeReconciliation(int64(i + 1))
			}
		}()
		go func() {
			defer waitGroup.Done()
			<-start
			for i := 0; i < 1_000; i++ {
				if i%2 == 0 {
					engine.swapReconciliationManager(second)
				} else {
					engine.swapReconciliationManager(first)
				}
			}
		}()
		close(start)
		waitGroup.Wait()
		engine.swapReconciliationManager(nil)
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
		engine.swapReconciliationManager(nil).stop()

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

	t.Run("technical failure during a completed-job retry remains retryable", func(t *testing.T) {
		readErr := errors.New("temporary token read failure")
		store := &retryReadFailureStorage{Storage: inmemory.NewStorage(), err: readErr}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		token := persistJobCompletionWithoutContinuation(t, &engine, store, job)

		store.failNextRead = true
		err = engine.JobCompleteByKey(t.Context(), job.Key, nil)
		require.ErrorIs(t, err, readErr)
		persisted, err := store.GetTokenByKey(t.Context(), token.Key)
		require.NoError(t, err)
		require.Equal(t, runtime.TokenStateRunning, persisted.State)

		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))
		persistedInstance, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Equal(t, runtime.ActivityStateCompleted, persistedInstance.ProcessInstance().State)
	})

	t.Run("periodic recovery parks a stranded branch at a parallel join beside a live job", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/fork-controlled-parallel-join.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)
		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, jobs, 2)

		strandedJob := jobs[0]
		liveJob := jobs[1]
		strandedToken := persistJobCompletionWithoutContinuation(t, &engine, store, strandedJob)
		manager := newReconciliationManager(&engine, time.Hour, -time.Second, 1)
		t.Cleanup(manager.stop)
		manager.reconcileNextBatch()

		persistedToken, err := store.GetTokenByKey(t.Context(), strandedToken.Key)
		require.NoError(t, err)
		require.Equal(t, runtime.TokenStateWaiting, persistedToken.State)
		pendingJobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, pendingJobs, 1)
		require.Equal(t, liveJob.Key, pendingJobs[0].Key)

		manager.reconcileNextBatch()
		pendingJobs, err = store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, pendingJobs, 1, "repeated scans must not duplicate the sibling job")

		require.NoError(t, engine.JobCompleteByKey(t.Context(), liveJob.Key, nil))
		joinedJob := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		require.Equal(t, "id-b-1", joinedJob.ElementId)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), joinedJob.Key, nil))
		persistedInstance, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Equal(t, runtime.ActivityStateCompleted, persistedInstance.ProcessInstance().State)
	})

	t.Run("periodic recovery advances through limited keyset pages", func(t *testing.T) {
		store := &recordingRunningTokenStorage{Storage: inmemory.NewStorage()}
		engine := NewEngine(EngineWithStorage(store))
		engine.reconciliationInterval = time.Hour
		engine.reconciliationBatchSize = 1
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)
		engine.swapReconciliationManager(nil).stop()

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		first, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "first"})
		require.NoError(t, err)
		second, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "second"})
		require.NoError(t, err)

		firstToken := persistJobCompletionWithoutContinuation(t, &engine, store, requireSinglePendingJob(t, store, first.ProcessInstance().Key))
		secondToken := persistJobCompletionWithoutContinuation(t, &engine, store, requireSinglePendingJob(t, store, second.ProcessInstance().Key))

		reconciliationManager := newReconciliationManager(&engine, 10*time.Millisecond, time.Millisecond, 1)
		require.Nil(t, engine.swapReconciliationManager(reconciliationManager))
		reconciliationManager.start()

		require.Eventually(t, func() bool {
			firstPersisted, firstErr := store.GetTokenByKey(t.Context(), firstToken.Key)
			secondPersisted, secondErr := store.GetTokenByKey(t.Context(), secondToken.Key)
			return firstErr == nil && secondErr == nil &&
				firstPersisted.State == runtime.TokenStateCompleted &&
				secondPersisted.State == runtime.TokenStateCompleted
		}, time.Second, 5*time.Millisecond)
		engine.swapReconciliationManager(nil).stop()

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
		engine.swapReconciliationManager(nil).stop()

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
		require.NoError(t, store.SaveProcessInstance(t.Context(), &runtime.DefaultProcessInstance{
			ProcessInstanceData: runtime.ProcessInstanceData{Key: processInstanceKey, State: runtime.ActivityStateActive},
		}))
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
		seeder.swapReconciliationManager(nil).stop()
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

type blockingStartupRecoveryStorage struct {
	*inmemory.Storage
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingStartupRecoveryStorage() *blockingStartupRecoveryStorage {
	return &blockingStartupRecoveryStorage{
		Storage: inmemory.NewStorage(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (store *blockingStartupRecoveryStorage) FindRunningTokensAfter(
	ctx context.Context,
	afterTokenKey int64,
	limit int64,
) ([]runtime.ExecutionToken, error) {
	store.enteredOnce.Do(func() {
		close(store.entered)
	})
	select {
	case <-store.release:
		return store.Storage.FindRunningTokensAfter(ctx, afterTokenKey, limit)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (store *blockingStartupRecoveryStorage) unblock() {
	store.releaseOnce.Do(func() {
		close(store.release)
	})
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

type retryReadFailureStorage struct {
	*inmemory.Storage
	failNextRead bool
	err          error
}

func (store *retryReadFailureStorage) GetActiveTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]runtime.ExecutionToken, error) {
	if store.failNextRead {
		store.failNextRead = false
		return nil, store.err
	}
	return store.Storage.GetActiveTokensForProcessInstance(ctx, processInstanceKey)
}
