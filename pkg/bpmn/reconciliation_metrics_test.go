package bpmn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestReconciliationMetrics(t *testing.T) {
	t.Run("counts only successful recovery", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		persistJobCompletionWithoutContinuation(t, &engine, store, job)

		metricsEngine, reader := newMetricsTestEngine(t)
		engine.metrics = metricsEngine.metrics
		manager := newReconciliationManager(&engine, time.Hour, -time.Second, 1)
		t.Cleanup(manager.stop)
		manager.reconcileNextBatch()
		require.Equal(t, int64(1), counterValue(t, reader, "reconciliation_recoveries"))
		require.Equal(t, int64(0), counterValue(t, reader, "reconciliation_failures"))
		require.Equal(t, uint64(1), histogramCount(t, reader, "reconciliation_scan_duration"))

		manager.resume(instance.ProcessInstance().Key)
		require.Equal(t, int64(1), counterValue(t, reader, "reconciliation_recoveries"), "completed instances must not be counted again")
	})

	t.Run("counts failed scan", func(t *testing.T) {
		scanErr := errors.New("transient scan error")
		store := &failedReconciliationScanStorage{Storage: inmemory.NewStorage(), err: scanErr}
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		metricsEngine, reader := newMetricsTestEngine(t)
		engine.metrics = metricsEngine.metrics
		manager := newReconciliationManager(&engine, time.Hour, time.Second, 1)
		t.Cleanup(manager.stop)
		manager.reconcileNextBatch()
		require.Equal(t, int64(1), counterValue(t, reader, "reconciliation_failures"))
		require.Equal(t, int64(0), counterValue(t, reader, "reconciliation_recoveries"))
		require.Equal(t, uint64(1), histogramCount(t, reader, "reconciliation_scan_duration"))
	})

	t.Run("disabled periodic scan retains explicit wakeup", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store), EngineWithReconciliation(time.Millisecond, time.Second, 1, false))
		metricsEngine, reader := newMetricsTestEngine(t)
		engine.metrics = metricsEngine.metrics
		require.NoError(t, engine.Start(t.Context()))
		t.Cleanup(engine.Stop)
		manager := engine.currentReconciliationManager()
		require.NotNil(t, manager)
		require.Zero(t, manager.interval)

		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
		require.NoError(t, err)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"variable_name": "value"})
		require.NoError(t, err)
		job := requireSinglePendingJob(t, store, instance.ProcessInstance().Key)
		persistJobCompletionWithoutContinuation(t, &engine, store, job)
		manager.wake(instance.ProcessInstance().Key)
		require.Eventually(t, func() bool {
			return counterValue(t, reader, "reconciliation_recoveries") == 1
		}, time.Second, time.Millisecond)
		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)
	})
}

type failedReconciliationScanStorage struct {
	*inmemory.Storage
	err error
}

func (s *failedReconciliationScanStorage) FindRecoverableRunningTokens(context.Context, int64, time.Time, int64) ([]runtime.ExecutionToken, error) {
	return nil, s.err
}
