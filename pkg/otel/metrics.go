package otel

import (
	"errors"

	"go.opentelemetry.io/otel/metric"
)

type EngineMetrics struct {
	ProcessesStarted metric.Int64Counter
	ProcessesEnded   metric.Int64Counter
	ProcessesRunning metric.Int64UpDownCounter
	JobsCreated      metric.Int64Counter
	JobsCompleted    metric.Int64Counter
	JobsFailed       metric.Int64Counter
}

func NewMetrics(meter metric.Meter) (*EngineMetrics, error) {
	var errJoin error

	processesStartedTotal, err := meter.Int64Counter("processes_started", metric.WithDescription("Number of processes started"))
	errJoin = errors.Join(errJoin, err)

	processesCompletedTotal, err := meter.Int64Counter("processes_completed", metric.WithDescription("Number of processes completed"))
	errJoin = errors.Join(errJoin, err)

	processesRunning, err := meter.Int64UpDownCounter("processes_running", metric.WithDescription("Number of processes currently running"))
	errJoin = errors.Join(errJoin, err)

	jobsCreated, err := meter.Int64Counter("jobs_created", metric.WithDescription("Number of jobs created"))
	errJoin = errors.Join(errJoin, err)

	jobsCompleted, err := meter.Int64Counter("jobs_completed", metric.WithDescription("Number of jobs completed"))
	errJoin = errors.Join(errJoin, err)

	jobsFailed, err := meter.Int64Counter("jobs_failed", metric.WithDescription("Number of jobs failed"))
	errJoin = errors.Join(errJoin, err)

	metrics := EngineMetrics{
		ProcessesStarted: processesStartedTotal,
		ProcessesEnded:   processesCompletedTotal,
		ProcessesRunning: processesRunning,
		JobsCreated:      jobsCreated,
		JobsCompleted:    jobsCompleted,
		JobsFailed:       jobsFailed,
	}
	return &metrics, errJoin
}
