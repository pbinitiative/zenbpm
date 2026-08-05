package otel

import (
	"errors"

	"go.opentelemetry.io/otel/metric"
)

// latencyBucketsMs are histogram bucket boundaries (in milliseconds) suited to
// BPMN workloads: from sub-millisecond engine operations up to hour-long
// process instances. Kept package-private so callers cannot mutate the shared
// definition; use LatencyBucketsMs to obtain a copy.
var latencyBucketsMs = []float64{
	1, 5, 10, 25, 50, 100, 250, 500,
	1_000, 2_500, 5_000, 10_000, 30_000, 60_000,
	300_000, 900_000, 3_600_000,
}

// LatencyBucketsMs returns a fresh copy of the shared latency histogram bucket
// boundaries (in milliseconds) so that callers cannot mutate the canonical set.
func LatencyBucketsMs() []float64 {
	buckets := make([]float64, len(latencyBucketsMs))
	copy(buckets, latencyBucketsMs)
	return buckets
}

type EngineMetrics struct {
	ProcessesStarted metric.Int64Counter
	ProcessesEnded   metric.Int64Counter
	ProcessesRunning metric.Int64UpDownCounter
	JobsCreated      metric.Int64Counter
	JobsCompleted    metric.Int64Counter
	JobsFailed       metric.Int64Counter

	// IncidentsCreated counts incidents raised by the engine.
	IncidentsCreated metric.Int64Counter
	// IncidentsResolved counts incidents that were resolved.
	IncidentsResolved metric.Int64Counter

	// ProcessInstanceDuration measures time from instance creation to completion/failure, in ms.
	ProcessInstanceDuration metric.Float64Histogram
	// JobLifetime measures time from job creation to completion/failure, in ms.
	JobLifetime metric.Float64Histogram

	TimersScheduled metric.Int64Counter
	TimersFired     metric.Int64Counter
	TimersCancelled metric.Int64Counter

	MessagesCorrelated       metric.Int64Counter
	MessageCorrelationFailed metric.Int64Counter
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

	incidentsCreated, err := meter.Int64Counter("incidents_created", metric.WithDescription("Number of incidents created"))
	errJoin = errors.Join(errJoin, err)

	incidentsResolved, err := meter.Int64Counter("incidents_resolved", metric.WithDescription("Number of incidents resolved"))
	errJoin = errors.Join(errJoin, err)

	processInstanceDuration, err := meter.Float64Histogram("process_instance_duration",
		metric.WithUnit("ms"),
		metric.WithDescription("Time from process instance creation to completion or failure, milliseconds"),
		metric.WithExplicitBucketBoundaries(latencyBucketsMs...),
	)
	errJoin = errors.Join(errJoin, err)

	jobLifetime, err := meter.Float64Histogram("job_lifetime",
		metric.WithUnit("ms"),
		metric.WithDescription("Time from job creation to completion or failure, milliseconds"),
		metric.WithExplicitBucketBoundaries(latencyBucketsMs...),
	)
	errJoin = errors.Join(errJoin, err)

	timersScheduled, err := meter.Int64Counter("timers_scheduled", metric.WithDescription("Number of timers scheduled"))
	errJoin = errors.Join(errJoin, err)

	timersFired, err := meter.Int64Counter("timers_fired", metric.WithDescription("Number of timers fired"))
	errJoin = errors.Join(errJoin, err)

	timersCancelled, err := meter.Int64Counter("timers_cancelled", metric.WithDescription("Number of timers cancelled"))
	errJoin = errors.Join(errJoin, err)

	messagesCorrelated, err := meter.Int64Counter("messages_correlated", metric.WithDescription("Number of messages successfully correlated to subscriptions"))
	errJoin = errors.Join(errJoin, err)

	messageCorrelationFailed, err := meter.Int64Counter("message_correlation_failed", metric.WithDescription("Number of failed message correlations"))
	errJoin = errors.Join(errJoin, err)

	metrics := EngineMetrics{
		ProcessesStarted:         processesStartedTotal,
		ProcessesEnded:           processesCompletedTotal,
		ProcessesRunning:         processesRunning,
		JobsCreated:              jobsCreated,
		JobsCompleted:            jobsCompleted,
		JobsFailed:               jobsFailed,
		IncidentsCreated:         incidentsCreated,
		IncidentsResolved:        incidentsResolved,
		ProcessInstanceDuration:  processInstanceDuration,
		JobLifetime:              jobLifetime,
		TimersScheduled:          timersScheduled,
		TimersFired:              timersFired,
		TimersCancelled:          timersCancelled,
		MessagesCorrelated:       messagesCorrelated,
		MessageCorrelationFailed: messageCorrelationFailed,
	}
	return &metrics, errJoin
}
