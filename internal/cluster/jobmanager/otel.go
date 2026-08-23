package jobmanager

import (
	"errors"
	"fmt"
	"sync"

	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/metric"
)

const (
	jobManagerMeter string = "job-manager"
)

var (
	JobsDistributed metric.Int64Counter
	// JobActivationLatency measures time between job creation and its distribution to a worker, in ms.
	JobActivationLatency metric.Float64Histogram

	registerMetricsOnce sync.Once
	registerMetricsErr  error
)

// registerMetrics initializes the package level instruments exactly once.
// It is safe to call concurrently and from multiple JobManager instances;
// repeated calls return the result of the first registration.
func registerMetrics() error {
	registerMetricsOnce.Do(func() {
		var err error
		var errJoin error
		JobsDistributed, err = otel.Meter(jobManagerMeter).Int64Counter("jobs_distributed", metric.WithDescription("Number of jobs sent to the clients"))
		errJoin = errors.Join(errJoin, err)
		JobActivationLatency, err = otel.Meter(jobManagerMeter).Float64Histogram("job_activation_latency",
			metric.WithUnit("ms"),
			metric.WithDescription("Time between job creation and distribution to a worker, milliseconds"),
			metric.WithExplicitBucketBoundaries(otelPkg.LatencyBucketsMs()...))
		errJoin = errors.Join(errJoin, err)
		if errJoin != nil {
			registerMetricsErr = fmt.Errorf("failed to create otel instruments: %w", errJoin)
		}
	})
	return registerMetricsErr
}
