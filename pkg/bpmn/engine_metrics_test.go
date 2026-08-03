package bpmn

import (
	"errors"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordTimerMetric(t *testing.T) {
	engine, reader := newMetricsTestEngine(t)
	ctx := t.Context()

	engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCreated})
	engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCreated})
	engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateTriggered})
	engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCancelled})

	assert.Equal(t, int64(2), counterValue(t, reader, "timers_scheduled"))
	assert.Equal(t, int64(1), counterValue(t, reader, "timers_fired"))
	assert.Equal(t, int64(1), counterValue(t, reader, "timers_cancelled"))
}

func TestRecordIncidentMetric(t *testing.T) {
	engine, reader := newMetricsTestEngine(t)
	ctx := t.Context()

	engine.recordIncidentMetric(ctx, bpmnruntime.Incident{ElementId: "task-1"})
	engine.recordIncidentMetric(ctx, bpmnruntime.Incident{ElementId: "task-1", ResolvedAt: new(time.Now())})

	assert.Equal(t, int64(1), counterValue(t, reader, "incidents_created"))
	assert.Equal(t, int64(1), counterValue(t, reader, "incidents_resolved"))
}

func TestRecordMessageCorrelationFailure(t *testing.T) {
	engine, reader := newMetricsTestEngine(t)
	ctx := t.Context()

	engine.recordMessageCorrelationFailure(ctx, "unknown", "subscription_not_found")
	engine.recordMessageCorrelationFailure(ctx, "order-received", "publish_failed")

	assert.Equal(t, int64(2), counterValue(t, reader, "message_correlation_failed"))
}

func TestMetricRecordersAreNilSafe(t *testing.T) {
	ctx := t.Context()
	for _, engine := range []*Engine{{}, {metrics: &otelPkg.EngineMetrics{}}} {
		assert.NotPanics(t, func() {
			engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCreated})
			engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateTriggered})
			engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCancelled})
			engine.recordIncidentMetric(ctx, bpmnruntime.Incident{})
			engine.recordMessageCorrelationFailure(ctx, "unknown", "subscription_not_found")
			engine.recordProcessInstanceEnd(ctx, &bpmnruntime.DefaultProcessInstance{})
		})
	}
}

func TestTimerScheduledMetricIsRecordedOnceThroughEngineBatch(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithPollTimerDelay(time.Hour))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-nested-interrupting.bpmn")
	require.NoError(t, err)
	metricsEngine, reader := newMetricsTestEngine(t)
	engine.metrics = metricsEngine.metrics

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	timers, err := store.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	require.NotEmpty(t, timers)

	assert.Equal(t, int64(len(timers)), counterValue(t, reader, "timers_scheduled"))
}

func TestInternalFailedJobRecordsFailureAndLifetime(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	metricsEngine, reader := newMetricsTestEngine(t)
	engine.metrics = metricsEngine.metrics
	handler := engine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.Fail("expected failure")
	})
	t.Cleanup(func() { engine.RemoveHandler(handler) })

	_, err = engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.Error(t, err)

	assert.Equal(t, int64(1), counterValue(t, reader, "jobs_created"))
	assert.Equal(t, int64(1), counterValue(t, reader, "jobs_failed"))
	assert.Equal(t, int64(0), counterValue(t, reader, "jobs_completed"))
	assert.Equal(t, uint64(1), histogramCount(t, reader, "job_lifetime"))
}

func TestMessagePublicationFailureRecordsProcessDuration(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	metricsEngine, reader := newMetricsTestEngine(t)
	engine.metrics = metricsEngine.metrics
	instance := &bpmnruntime.DefaultProcessInstance{ProcessInstanceData: bpmnruntime.ProcessInstanceData{
		Definition: &bpmnruntime.ProcessDefinition{BpmnProcessId: "message-process"},
		Key:        42,
		CreatedAt:  time.Now().Add(-time.Second),
		State:      bpmnruntime.ActivityStateActive,
	}}
	message := &bpmnruntime.TokenMessageSubscription{
		ProcessInstanceKey: instance.Key,
		Token: bpmnruntime.ExecutionToken{
			Key:                43,
			ElementInstanceKey: 44,
			ElementId:          "message-catch",
			ProcessInstanceKey: instance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		},
		MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
			Key:       45,
			ElementId: "message-catch",
			Name:      "message",
			State:     bpmnruntime.ActivityStateActive,
		},
	}
	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)

	err = handleMessagePublicationError(t.Context(), &batch, message, instance, errors.New("mapping failed"), "message failed")
	require.Error(t, err)

	assert.Equal(t, int64(1), counterValue(t, reader, "processes_completed"))
	assert.Equal(t, uint64(1), histogramCount(t, reader, "process_instance_duration"))
	persisted, findErr := store.FindProcessInstanceByKey(t.Context(), instance.Key)
	require.NoError(t, findErr)
	assert.Equal(t, bpmnruntime.ActivityStateFailed, persisted.ProcessInstance().State)
}

// newMetricsTestEngine returns an engine wired to an isolated manual-reader
// meter provider so counter values can be asserted deterministically.
func newMetricsTestEngine(t *testing.T) (*Engine, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(t.Context())
	})
	metrics, err := otelPkg.NewMetrics(provider.Meter("bpmn-engine-test"))
	require.NoError(t, err)
	return &Engine{metrics: metrics}, reader
}

// counterValue collects the current value of an int64 counter, summing over
// all attribute sets. Returns 0 when the instrument has no data points.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not an int64 sum", name)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func histogramCount(t *testing.T, reader *sdkmetric.ManualReader, name string) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var total uint64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "metric %s is not a float64 histogram", name)
			for _, dp := range histogram.DataPoints {
				total += dp.Count
			}
		}
	}
	return total
}
