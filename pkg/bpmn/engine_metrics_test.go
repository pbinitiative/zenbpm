package bpmn

import (
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
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
	// engines without configured metrics (or nil receivers) must not panic
	engine := &Engine{}
	ctx := t.Context()
	assert.NotPanics(t, func() {
		engine.recordTimerMetric(ctx, bpmnruntime.Timer{TimerState: bpmnruntime.TimerStateCreated})
		engine.recordIncidentMetric(ctx, bpmnruntime.Incident{})
		engine.recordMessageCorrelationFailure(ctx, "unknown", "subscription_not_found")
	})
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
