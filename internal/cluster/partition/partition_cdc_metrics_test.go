package partition

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type cdcMetricMeasurement struct {
	value int64
	attrs []attribute.KeyValue
}

type cdcRecordingGauge struct {
	noop.Int64Gauge
	mu           sync.Mutex
	measurements []cdcMetricMeasurement
}

func (g *cdcRecordingGauge) Record(_ context.Context, value int64, opts ...metric.RecordOption) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.measurements = append(g.measurements, cdcMetricMeasurement{
		value: value,
		attrs: cdcMetricAttributes(metric.NewRecordConfig(opts).Attributes()),
	})
}

func (g *cdcRecordingGauge) recorded() []cdcMetricMeasurement {
	g.mu.Lock()
	defer g.mu.Unlock()
	measurements := make([]cdcMetricMeasurement, len(g.measurements))
	copy(measurements, g.measurements)
	return measurements
}

type cdcRecordingCounter struct {
	noop.Int64Counter
	mu           sync.Mutex
	measurements []cdcMetricMeasurement
}

func (c *cdcRecordingCounter) Add(_ context.Context, value int64, opts ...metric.AddOption) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measurements = append(c.measurements, cdcMetricMeasurement{
		value: value,
		attrs: cdcMetricAttributes(metric.NewAddConfig(opts).Attributes()),
	})
}

func (c *cdcRecordingCounter) recorded() []cdcMetricMeasurement {
	c.mu.Lock()
	defer c.mu.Unlock()
	measurements := make([]cdcMetricMeasurement, len(c.measurements))
	copy(measurements, c.measurements)
	return measurements
}

func (c *cdcRecordingCounter) total() int64 {
	var total int64
	for _, measurement := range c.recorded() {
		total += measurement.value
	}
	return total
}

type stubCDCMetricsSource struct {
	stats         map[string]any
	statsErr      error
	highWatermark uint64
	retries       uint64
}

func (s *stubCDCMetricsSource) Stats() (map[string]any, error) {
	return s.stats, s.statsErr
}

func (s *stubCDCMetricsSource) HighWatermark() uint64 {
	return s.highWatermark
}

func (s *stubCDCMetricsSource) NumEndpointRetries() uint64 {
	return s.retries
}

func TestZenPartitionNodeCDCMetrics(t *testing.T) {
	t.Run("exports queue high-watermark and monotonic retries for a failing endpoint", func(t *testing.T) {
		var endpointHealthy atomic.Bool
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if endpointHealthy.Load() {
				w.WriteHeader(http.StatusOK)
				return
			}
			http.Error(w, "CDC endpoint unavailable", http.StatusInternalServerError)
		}))
		defer receiver.Close()

		cdcOutputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		cdcOutput := fmt.Sprintf(
			`{"endpoint":%q,"service_id":"metrics-source","table_filter":"^cdc_metrics_test$","max_batch_size":1,"max_batch_delay":%d,"transmit_min_backoff":%d,"transmit_max_backoff":%d,"transmit_timeout":%d}`,
			receiver.URL,
			5*time.Millisecond,
			20*time.Millisecond,
			20*time.Millisecond,
			100*time.Millisecond,
		)
		require.NoError(t, os.WriteFile(cdcOutputPath, []byte(cdcOutput), 0o600))

		partition, server := prepareCDCPartitionTestSetup(t, config.CDC{
			Enabled: true,
			Output:  cdcOutputPath,
		})
		defer func() {
			endpointHealthy.Store(true)
			stopErr := partition.Stop()
			serverErr := server.Close()
			require.NoError(t, stopErr)
			require.NoError(t, serverErr)
		}()

		queueGauge := &cdcRecordingGauge{}
		highWatermarkGauge := &cdcRecordingGauge{}
		retryCounter := &cdcRecordingCounter{}
		metricsNode := &ZenPartitionNode{
			PartitionId: partition.PartitionId,
			config:      partition.config,
			logger:      hclog.NewNullLogger(),
			metrics: partitionMetrics{
				cdcQueueLength:     queueGauge,
				cdcHighWatermark:   highWatermarkGauge,
				cdcEndpointRetries: retryCounter,
			},
		}

		_, err := partition.DB.ExecContext(context.Background(), `
			CREATE TABLE cdc_metrics_test (
				id INTEGER NOT NULL PRIMARY KEY,
				name TEXT NOT NULL
			)
		`)
		require.NoError(t, err)
		_, err = partition.DB.ExecContext(context.Background(), `INSERT INTO cdc_metrics_test(id, name) VALUES(1, 'first')`)
		require.NoError(t, err)
		_, err = partition.DB.ExecContext(context.Background(), `INSERT INTO cdc_metrics_test(id, name) VALUES(2, 'second')`)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			stats, statsErr := partition.cdcService.Stats()
			if statsErr != nil {
				return false
			}
			queueLength, ok := cdcQueueLength(stats)
			return ok && queueLength >= 2 && partition.cdcService.NumEndpointRetries() > 0
		}, 4*time.Second, 20*time.Millisecond)

		metricsNode.recordCDCMetrics(context.Background(), partition.cdcService)
		queuedMeasurements := queueGauge.recorded()
		require.Len(t, queuedMeasurements, 1)
		require.GreaterOrEqual(t, queuedMeasurements[0].value, int64(2))
		require.Positive(t, retryCounter.total())
		require.Len(t, highWatermarkGauge.recorded(), 1)
		require.Zero(t, highWatermarkGauge.recorded()[0].value)
		requireCDCMetricsAttributes(t, queuedMeasurements[0].attrs, partition.PartitionId, "test-rq-lite")
		requireCDCMetricsAttributes(t, highWatermarkGauge.recorded()[0].attrs, partition.PartitionId, "test-rq-lite")
		requireCDCMetricsAttributes(t, retryCounter.recorded()[0].attrs, partition.PartitionId, "test-rq-lite")

		endpointHealthy.Store(true)
		require.Eventually(t, func() bool {
			stats, statsErr := partition.cdcService.Stats()
			if statsErr != nil {
				return false
			}
			queueLength, ok := cdcQueueLength(stats)
			return ok && queueLength == 0 && partition.cdcService.HighWatermark() > 0
		}, 5*time.Second, 20*time.Millisecond)

		metricsNode.recordCDCMetrics(context.Background(), partition.cdcService)
		retriesAfterRecovery := partition.cdcService.NumEndpointRetries()
		require.Equal(t, int64(retriesAfterRecovery), retryCounter.total())
		highWatermarkMeasurements := highWatermarkGauge.recorded()
		require.Len(t, highWatermarkMeasurements, 2)
		require.Equal(t, int64(partition.cdcService.HighWatermark()), highWatermarkMeasurements[1].value)
		queueMeasurements := queueGauge.recorded()
		require.Len(t, queueMeasurements, 2)
		require.Zero(t, queueMeasurements[1].value)

		metricsNode.recordCDCMetrics(context.Background(), partition.cdcService)
		require.Equal(t, int64(retriesAfterRecovery), retryCounter.total(), "an unchanged absolute retry total must not be added again")
	})

	t.Run("keeps high-watermark and retry telemetry available when status collection fails", func(t *testing.T) {
		queueGauge := &cdcRecordingGauge{}
		highWatermarkGauge := &cdcRecordingGauge{}
		retryCounter := &cdcRecordingCounter{}
		partition := &ZenPartitionNode{
			PartitionId: 7,
			config:      &config.RqLite{NodeID: "node-seven"},
			logger:      hclog.NewNullLogger(),
			metrics: partitionMetrics{
				cdcQueueLength:     queueGauge,
				cdcHighWatermark:   highWatermarkGauge,
				cdcEndpointRetries: retryCounter,
			},
		}
		source := &stubCDCMetricsSource{
			statsErr:      errors.New("status unavailable"),
			highWatermark: 42,
			retries:       3,
		}

		partition.recordCDCMetrics(context.Background(), source)
		partition.recordCDCMetrics(context.Background(), source)

		require.Empty(t, queueGauge.recorded())
		require.Len(t, highWatermarkGauge.recorded(), 2)
		require.Equal(t, int64(42), highWatermarkGauge.recorded()[0].value)
		require.Equal(t, int64(3), retryCounter.total(), "the absolute retry value must be converted to a delta")
		requireCDCMetricsAttributes(t, highWatermarkGauge.recorded()[0].attrs, 7, "node-seven")
		requireCDCMetricsAttributes(t, retryCounter.recorded()[0].attrs, 7, "node-seven")
	})

	t.Run("does nothing when CDC is disabled", func(t *testing.T) {
		queueGauge := &cdcRecordingGauge{}
		highWatermarkGauge := &cdcRecordingGauge{}
		retryCounter := &cdcRecordingCounter{}
		partition := &ZenPartitionNode{
			metrics: partitionMetrics{
				cdcQueueLength:     queueGauge,
				cdcHighWatermark:   highWatermarkGauge,
				cdcEndpointRetries: retryCounter,
			},
		}

		partition.updateCDCMetrics(context.Background())

		require.Empty(t, queueGauge.recorded())
		require.Empty(t, highWatermarkGauge.recorded())
		require.Empty(t, retryCounter.recorded())
	})
}

func cdcMetricAttributes(set attribute.Set) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, set.Len())
	for i := 0; i < set.Len(); i++ {
		value, _ := set.Get(i)
		attrs = append(attrs, value)
	}
	return attrs
}

func requireCDCMetricsAttributes(t *testing.T, attrs []attribute.KeyValue, partition uint32, nodeID string) {
	t.Helper()
	set := attribute.NewSet(attrs...)

	partitionAttr, ok := set.Value("partition")
	require.True(t, ok)
	require.Equal(t, int64(partition), partitionAttr.AsInt64())
	nodeAttr, ok := set.Value("node_id")
	require.True(t, ok)
	require.Equal(t, nodeID, nodeAttr.AsString())
}
