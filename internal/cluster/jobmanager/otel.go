package jobmanager

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/metric"
)

const (
	jobManagerMeter string = "job-manager"
)

var (
	JobsDistributed metric.Int64Counter
)

func registerMetrics() error {
	var err error
	var errJoin error
	JobsDistributed, err = otel.Meter(jobManagerMeter).Int64Counter("jobs_distributed", metric.WithDescription("Number of jobs sent to the clients"))
	errJoin = errors.Join(errJoin, err)
	errJoin = errors.Join(errJoin, err)
	if errJoin != nil {
		return fmt.Errorf("failed to create otel instruments: %w", err)
	}
	return nil
}
