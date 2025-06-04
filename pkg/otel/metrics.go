package otel

import (
	"errors"

	"go.opentelemetry.io/otel/metric"
)

type EngineMetrics struct {
	ProcessesStartedTotal metric.Int64Counter
	ProcessesEndedTotal   metric.Int64Counter
}

func NewMetrics(meter metric.Meter) (*EngineMetrics, error) {
	var errJoin error

	processesStartedTotal, err := meter.Int64Counter("processes_started_total", metric.WithDescription("Total number of processes started"))
	errJoin = errors.Join(errJoin, err)

	processesCompletedTotal, err := meter.Int64Counter("processes_completed_total", metric.WithDescription("Total number of processes completed"))
	errJoin = errors.Join(errJoin, err)

	metrics := EngineMetrics{
		ProcessesStartedTotal: processesStartedTotal,
		ProcessesEndedTotal:   processesCompletedTotal,
	}
	return &metrics, errJoin
}
