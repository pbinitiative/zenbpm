package bpmn

import (
	"bytes"
	"cmp"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestJobIsCreatedWithRetriesFromTheDefinition(t *testing.T) {
	t.Run("literal", func(t *testing.T) {
		_, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		assert.Equal(t, int32(3), job.Retries)
		assert.Zero(t, job.Attempts)
		assert.Nil(t, job.RetryAt)
		assert.Empty(t, job.RetryBackoff)
		assert.Equal(t, job, reloadJob(t, store, job.Key))
	})
	t.Run("FEEL expression against the job's scope", func(t *testing.T) {
		_, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-expression.bpmn", map[string]any{"attemptsAllowed": 4})
		assert.Equal(t, int32(5), job.Retries)
	})
	t.Run("absent takes the engine default of one attempt", func(t *testing.T) {
		_, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-without-retries.bpmn", nil)
		assert.Equal(t, int32(1), job.Retries)
	})
	t.Run("absent takes a configured default", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.DefaultRetries = 3
		_, _, job := startRetriedJob(t, limits, "service-task-without-retries.bpmn", nil)
		assert.Equal(t, int32(3), job.Retries)
	})
	t.Run("capped at the maximum", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.MaxRetries = 2
		_, _, job := startRetriedJob(t, limits, "service-task-retries.bpmn", nil)
		assert.Equal(t, int32(2), job.Retries)
	})
	t.Run("an expression which is no integer ends in an incident at creation", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries-not-an-integer-expression.bpmn")
		require.NoError(t, err)

		instance, _ := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NotNil(t, instance)

		incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Contains(t, incidents[0].Message, "retries")
		assert.Contains(t, incidents[0].Message, "not an integer")
	})
	t.Run("an expression a handler in the engine never reads does not fail its task", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		engine.NewTaskHandler().Type("charge-card").Handler(func(job ActivatedJob) { job.Complete() })
		definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries-not-an-integer-expression.bpmn")
		require.NoError(t, err)

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)

		incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		assert.Empty(t, incidents)
		assertInstanceState(t, store, instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	})
	t.Run("a negative literal is refused at deployment", func(t *testing.T) {
		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		t.Cleanup(engine.Stop)
		_, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries-negative.bpmn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `element id="retried-task"`)
		assert.Contains(t, err.Error(), "must not be negative")
	})
}

// TestADefinitionStoredBeforeRetriesWereReadKeepsLoading shows the check of a
// literal retries value is a deployment check, not a parsing one. An engine
// which did not read the attribute yet accepted any value, so a definition it
// stored may carry one which does not parse: it must still load, or its
// running instances would be stuck and a corrected version of the process could
// not be deployed either. Only a job of the old definition ends in an incident.
func TestADefinitionStoredBeforeRetriesWereReadKeepsLoading(t *testing.T) {
	stored := serviceTaskRetriesDefinitionWith(t, `retries="${retries}"`)
	var definitions bpmn20.TDefinitions
	require.NoError(t, xml.Unmarshal(stored, &definitions), "what an older engine stored must parse")
	require.ErrorContains(t, definitions.ValidateForDeployment(), `retries "${retries}" is not an integer`)

	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	for name, deploy := range map[string]func() error{
		"LoadFromBytes": func() error {
			_, err := engine.LoadFromBytes(t.Context(), stored, engine.generateKey())
			return err
		},
		"DeployProcessDefinition": func() error {
			_, err := engine.DeployProcessDefinition(t.Context(), stored, engine.generateKey(), 1)
			return err
		},
		"ParseProcessDefinitionIdentity": func() error {
			_, err := ParseProcessDefinitionIdentity(stored)
			return err
		},
	} {
		assert.ErrorContains(t, deploy(), `retries "${retries}" is not an integer`, "%s: a new deployment is refused", name)
	}

	// stored the way an older engine stored it, or a restore copies it
	old, err := engine.ImportProcessDefinition(t.Context(), stored, engine.generateKey(), 1, false)
	require.NoError(t, err, "what is stored keeps loading")

	corrected, err := engine.LoadFromBytes(t.Context(), serviceTaskRetriesDefinitionWith(t, `retries="3"`), engine.generateKey())
	require.NoError(t, err, "a corrected version of the process can be deployed next to the old one")
	assert.Equal(t, int32(2), corrected.Version)

	instance, _ := engine.CreateInstanceByKey(t.Context(), old.Key, nil)
	require.NotNil(t, instance)
	assert.Contains(t, singleIncident(t, store, instance.ProcessInstance().Key).Message, `retries "${retries}" is not an integer`,
		"a job of the old definition is checked when it is created")
}

// TestJobRetryLimitsWhichNameOnlyWhatTheyChangeKeepTheOtherDefaults shows an
// engine given only a default number of retries keeps the default caps
// instead of capping every job at none.
func TestJobRetryLimitsWhichNameOnlyWhatTheyChangeKeepTheOtherDefaults(t *testing.T) {
	engine, store, job := startRetriedJob(t, JobRetryLimits{DefaultRetries: 3}, "service-task-without-retries.bpmn", nil)
	assert.Equal(t, int32(3), job.Retries)

	assertBackoffAfterFailure(t, engine, store, job.Key, new(time.Hour), time.Hour)
}

func TestFailWithRemainingRetriesKeepsTheJobActive(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "payment service unavailable", nil, map[string]any{"ignored": true}, nil, nil, nil))

	failed := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, failed.State)
	assert.Equal(t, int32(2), failed.Retries)
	assert.Equal(t, int32(1), failed.Attempts)
	assert.Nil(t, failed.RetryAt, "without a policy or a requested backoff the job is deliverable at once")
	require.NotNil(t, failed.LastFailureMessage)
	assert.Equal(t, "payment service unavailable", *failed.LastFailureMessage)
	assert.Nil(t, failed.OutputVariables, "the variables of a failure without an error code are not kept")
	assertNoIncidents(t, store, job.ProcessInstanceKey)
	assertInstanceState(t, store, job.ProcessInstanceKey, runtime.ActivityStateActive)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "payment service unavailable", nil, nil, nil, nil, nil))
	assert.Equal(t, int32(1), reloadJob(t, store, job.Key).Retries)
	assertNoIncidents(t, store, job.ProcessInstanceKey)
}

// TestRetriedFailuresAreMeasuredApartFromFailedJobs shows a failure which
// leaves the job for another attempt counts as a retry with its backoff, and
// jobs_failed keeps counting only the jobs which failed for good.
func TestRetriedFailuresAreMeasuredApartFromFailedJobs(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	metrics, err := otelPkg.NewMetrics(provider.Meter("job-retries-test"))
	require.NoError(t, err)
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), func(engine *Engine) { engine.metrics = metrics })
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, engine.JobFailByKey(t.Context(), jobs[0].Key, "down", nil, nil, nil, new(2*time.Hour), nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), jobs[0].Key, "down for good", nil, nil, new(int32(0)), nil, nil))

	assert.Equal(t, int64(1), counterValue(t, reader, "jobs_retried"))
	assert.Equal(t, int64(1), counterValue(t, reader, "jobs_failed"), "a retried failure does not fail the job")
	backoff := histogramPoint(t, reader, "job_retry_backoff")
	assert.Equal(t, uint64(1), backoff.Count)
	assert.InDelta(t, float64((2 * time.Hour).Milliseconds()), backoff.Sum, 1)
	assert.Zero(t, backoff.BucketCounts[len(backoff.BucketCounts)-1], "a backoff within jobs.maxRetryBackoff has a bucket of its own, not +Inf")
}

func TestFailWithoutRetriesLeftCreatesIncident(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

	for range 2 {
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "still down", nil, nil, nil, nil, nil))
	}
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down for good", nil, map[string]any{"reason": "timeout"}, nil, nil, nil))

	failed := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateFailed, failed.State)
	assert.Zero(t, failed.Retries)
	assert.Equal(t, int32(3), failed.Attempts)
	assert.Equal(t, map[string]any{"reason": "timeout"}, failed.OutputVariables, "the failed job keeps the variables of the failure which failed it, as it always did")
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), job.ProcessInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Contains(t, incidents[0].Message, "down for good")
	assert.Contains(t, incidents[0].Message, "retries exhausted")
}

func TestExhaustedRetriesAreLoggedAsAnIncidentOnlyOnceItIsCommitted(t *testing.T) {
	store := &failingFlushes{Storage: inmemory.NewStorage()}
	var logs bytes.Buffer
	engine := NewEngine(EngineWithStorage(store), EngineWithLogger(hclog.New(&hclog.LoggerOptions{Output: &logs})))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-without-retries.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	job := jobs[0]

	store.failFlushes.Store(true)
	err = engine.JobFailByKey(t.Context(), job.Key, "down for good", nil, nil, nil, nil, nil)
	store.failFlushes.Store(false)

	require.Error(t, err)
	assert.Equal(t, runtime.ActivityStateActive, reloadJob(t, store.Storage, job.Key).State, "nothing was committed")
	assertNoIncidents(t, store.Storage, job.ProcessInstanceKey)
	assert.NotContains(t, logs.String(), "incident created", "no incident exists to point operators at")

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down for good", nil, nil, nil, nil, nil))

	singleIncident(t, store.Storage, job.ProcessInstanceKey)
	assert.Contains(t, logs.String(), "retries exhausted, incident created")
}

func TestFailureSpanCarriesTheCommittedOutcome(t *testing.T) {
	engine, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	recorder := tracetest.NewSpanRecorder()
	engine.tracer = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test")

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, new(2*time.Second), nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, map[attribute.Key]attribute.Value{
		otelPkg.AttributeJobFailureOutcome: attribute.StringValue(otelPkg.JobFailureOutcomeRetry),
		otelPkg.AttributeJobAttempt:        attribute.IntValue(1),
		otelPkg.AttributeJobRetries:        attribute.IntValue(2),
		otelPkg.AttributeJobRetryBackoffMs: attribute.Int64Value(2000),
	}, retryAttributes(spans[0]))
	assert.Equal(t, map[attribute.Key]attribute.Value{
		otelPkg.AttributeJobFailureOutcome: attribute.StringValue(otelPkg.JobFailureOutcomeIncident),
		otelPkg.AttributeJobAttempt:        attribute.IntValue(2),
		otelPkg.AttributeJobRetries:        attribute.IntValue(0),
	}, retryAttributes(spans[1]))
	for _, span := range spans {
		for _, kv := range span.Attributes() {
			assert.NotEqual(t, "down", kv.Value.String(), "the worker's message is no span attribute")
		}
	}
}

func TestFailOfAJobWithoutRetriesAttributeCreatesIncidentAtOnce(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-without-retries.bpmn", nil)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "boom", nil, nil, nil, nil, nil))

	assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)
	assert.Equal(t, fmt.Sprintf("boom (job %d: 1 attempt, retries exhausted)", job.Key), singleIncident(t, store, job.ProcessInstanceKey).Message)
}

func TestExplicitRetriesOverrideTheDecrement(t *testing.T) {
	t.Run("a worker may top up", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-without-retries.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "try more often", nil, nil, new(int32(5)), nil, nil))

		failed := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateActive, failed.State)
		assert.Equal(t, int32(5), failed.Retries)
	})
	t.Run("capped at the maximum", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.MaxRetries = 4
		engine, store, job := startRetriedJob(t, limits, "service-task-without-retries.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "try more often", nil, nil, new(int32(50)), nil, nil))

		assert.Equal(t, int32(4), reloadJob(t, store, job.Key).Retries)
	})
	t.Run("zero creates the incident at once", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "hopeless", nil, nil, new(int32(0)), nil, nil))

		assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)
	})
}

func TestNegativeRetriesAreRefused(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

	err := engine.JobFailByKey(t.Context(), job.Key, "boom", nil, nil, new(int32(-1)), nil, nil)
	require.ErrorIs(t, err, ErrInvalidJobRequest)
	err = engine.JobFailByKey(t.Context(), job.Key, "boom", nil, nil, nil, new(-time.Second), nil)
	require.ErrorIs(t, err, ErrInvalidJobRequest)

	assert.Equal(t, job, reloadJob(t, store, job.Key), "a refused request leaves the job as it was")
}

func TestRetryBackoffKeepsTheJobOutOfActivateJobs(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	require.Len(t, activatedJobKeys(t, engine), 1)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, new(time.Hour), nil))

	inBackoff := reloadJob(t, store, job.Key)
	require.NotNil(t, inBackoff.RetryAt)
	assert.WithinDuration(t, time.Now().Add(time.Hour), *inBackoff.RetryAt, 5*time.Second)
	assert.Empty(t, activatedJobKeys(t, engine), "a job waiting out its backoff is not handed out")

	inBackoff.RetryAt = new(time.Now().Add(-time.Millisecond))
	require.NoError(t, store.SaveJob(t.Context(), inBackoff))
	assert.Equal(t, []int64{job.Key}, activatedJobKeys(t, engine), "once the backoff has passed the job is handed out again")
}

func TestRetryBackoffPolicyIsAppliedPerAttempt(t *testing.T) {
	t.Run("entry per attempt, the last repeating, a request overriding one attempt", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retry-backoff-policy.bpmn", nil)
		require.Equal(t, []time.Duration{time.Second, 3 * time.Second}, job.RetryBackoff)

		assertBackoffAfterFailure(t, engine, store, job.Key, nil, time.Second)
		assertBackoffAfterFailure(t, engine, store, job.Key, new(10*time.Second), 10*time.Second)
		assertBackoffAfterFailure(t, engine, store, job.Key, nil, 3*time.Second)
	})
	t.Run("FEEL expression evaluated at job creation", func(t *testing.T) {
		_, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retry-backoff-expression.bpmn",
			map[string]any{"backoffs": []any{"PT5S", "PT1M"}})
		assert.Equal(t, []time.Duration{5 * time.Second, time.Minute}, job.RetryBackoff)
	})
	t.Run("the engine default where the definition names none", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.DefaultRetries = 3
		limits.DefaultRetryBackoff = []time.Duration{2 * time.Second}
		engine, store, job := startRetriedJob(t, limits, "service-task-without-retries.bpmn", nil)

		assertBackoffAfterFailure(t, engine, store, job.Key, nil, 2*time.Second)
		assert.Empty(t, reloadJob(t, store, job.Key).RetryBackoff, "the job keeps no policy of its own")
	})
	t.Run("every backoff is capped", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.MaxRetryBackoff = 2 * time.Second
		engine, store, job := startRetriedJob(t, limits, "service-task-retry-backoff-policy.bpmn", nil)

		assertBackoffAfterFailure(t, engine, store, job.Key, new(time.Hour), 2*time.Second)
		assertBackoffAfterFailure(t, engine, store, job.Key, nil, 2*time.Second)
	})
	t.Run("an unparsable literal is refused at deployment", func(t *testing.T) {
		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		t.Cleanup(engine.Stop)
		_, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retry-backoff-invalid.bpmn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `element id="retried-task"`)
		assert.Contains(t, err.Error(), "retryBackoff")
	})
}

func TestARetryBackoffExpressionWhichIsNoDurationListEndsInAnIncidentAtCreation(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retry-backoff-not-a-duration-list.bpmn")
	require.NoError(t, err, "an expression is checked when the job is created, not at deployment")

	instance, _ := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NotNil(t, instance)

	incident := singleIncident(t, store, instance.ProcessInstance().Key)
	assert.Contains(t, incident.Message, "retryBackoff")
	assert.Contains(t, incident.Message, "is not a duration string")
	assert.Empty(t, activatedJobKeys(t, &engine), "no job is left to hand out")
}

func TestEveryJobProducingElementSpendsItsRetriesByItsPolicy(t *testing.T) {
	for _, fixture := range []string{
		"send-task-retries.bpmn",
		"business-rule-task-retries.bpmn",
		"message-throw-event-retries.bpmn",
		"message-end-event-retries.bpmn",
	} {
		t.Run(fixture, func(t *testing.T) {
			engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), fixture, nil)
			require.Equal(t, int32(3), job.Retries)
			require.Equal(t, []time.Duration{time.Hour}, job.RetryBackoff)

			assertBackoffAfterFailure(t, engine, store, job.Key, nil, time.Hour)
			assertBackoffAfterFailure(t, engine, store, job.Key, nil, time.Hour)
			retried := reloadJob(t, store, job.Key)
			assert.Equal(t, runtime.ActivityStateActive, retried.State)
			assert.Equal(t, int32(1), retried.Retries)
			assertNoIncidents(t, store, job.ProcessInstanceKey)
			assert.Empty(t, activatedJobKeys(t, engine), "a job waiting out its backoff is not handed out")

			require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down for good", nil, nil, nil, nil, nil))

			assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)
			assert.Contains(t, singleIncident(t, store, job.ProcessInstanceKey).Message, "3 attempts, retries exhausted")
		})
	}
}

func TestAJobInterruptedDuringItsBackoffIsNeverHandedOutAgain(t *testing.T) {
	for name, interrupt := range map[string]func(t *testing.T, engine *Engine, job runtime.Job){
		"by cancelling the instance": func(t *testing.T, engine *Engine, job runtime.Job) {
			require.NoError(t, engine.CancelInstanceByKey(t.Context(), job.ProcessInstanceKey))
		},
		"by an interrupting boundary event": func(t *testing.T, engine *Engine, _ runtime.Job) {
			require.NoError(t, engine.PublishMessageByName(t.Context(), "order-cancelled", new("order-1"), nil))
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-message-boundary.bpmn", map[string]any{"orderId": "order-1"})
			require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
			require.NotNil(t, reloadJob(t, store, job.Key).RetryAt, "the job waits out its backoff")

			interrupt(t, engine, job)

			terminated := reloadJob(t, store, job.Key)
			assert.Equal(t, runtime.ActivityStateTerminated, terminated.State)
			terminated.RetryAt = new(time.Now().Add(-time.Millisecond))
			require.NoError(t, store.SaveJob(t.Context(), terminated))
			assert.Empty(t, activatedJobKeys(t, engine), "the backoff has passed, but the job has ended")
		})
	}
}

func TestFailWithErrorCodeIgnoresRetries(t *testing.T) {
	t.Run("a caught error spends no retry", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-error-boundary.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "declined", new("PAYMENT_DECLINED"), nil, new(int32(7)), nil, nil))

		handled := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateTerminated, handled.State)
		assert.Equal(t, int32(3), handled.Retries)
		assert.Zero(t, handled.Attempts)
		assert.Nil(t, handled.LastFailureMessage)
		assertInstanceState(t, store, job.ProcessInstanceKey, runtime.ActivityStateCompleted)
		failures, err := store.FindJobFailures(t.Context(), job.Key)
		require.NoError(t, err)
		assert.Empty(t, failures)
	})
	t.Run("an uncaught error creates the incident without spending a retry", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "unexpected", new("NOBODY_CATCHES_THIS"), nil, nil, nil, nil))

		failed := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateFailed, failed.State)
		assert.Equal(t, int32(3), failed.Retries)
		assert.Nil(t, failed.LastFailureMessage)
		incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), job.ProcessInstanceKey)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, &job.Key, incidents[0].JobKey)
		assert.NotContains(t, incidents[0].Message, "retries exhausted")
	})
	t.Run("invalid retries are refused next to an error code, before the error is routed", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-error-boundary.bpmn", nil)

		err := engine.JobFailByKey(t.Context(), job.Key, "declined", new("PAYMENT_DECLINED"), nil, new(int32(-1)), nil, nil)

		require.ErrorIs(t, err, ErrInvalidJobRequest)
		assert.Equal(t, job, reloadJob(t, store, job.Key), "the refused request changes nothing")
		assertInstanceState(t, store, job.ProcessInstanceKey, runtime.ActivityStateActive)
	})
	t.Run("an empty code, as clients before retries send it, spends a retry", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-error-boundary.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", new(""), nil, nil, nil, nil))

		failed := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateActive, failed.State)
		assert.Equal(t, int32(2), failed.Retries)
	})
}

func TestResolveIncidentRestoresRetries(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))
	incident := singleIncident(t, store, job.ProcessInstanceKey)

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	resolved := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, int32(3), resolved.Retries, "the definition's retries are restored")
	assert.Zero(t, resolved.Attempts, "a resolution starts a fresh series")
	assert.Nil(t, resolved.RetryAt)
	assertInstanceState(t, store, job.ProcessInstanceKey, runtime.ActivityStateActive)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down again", nil, nil, nil, nil, nil))
	assert.Equal(t, runtime.ActivityStateActive, reloadJob(t, store, job.Key).State, "the restored retries are spent before the next incident")
}

// TestResolvingAnIncidentTheJobDidNotRaiseKeepsItsSeries shows the incident
// of something else on the token of a job waiting out its backoff, such as a
// boundary event which failed, leaves the job's attempts and backoff alone
// when it is resolved: only the job's own incident starts a fresh series.
func TestResolvingAnIncidentTheJobDidNotRaiseKeepsItsSeries(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, new(time.Hour), nil))
	inBackoff := reloadJob(t, store, job.Key)
	require.NotNil(t, inBackoff.RetryAt)
	raiseIncidentOnTheTokenOf(t, engine, store, inBackoff)

	require.NoError(t, engine.ResolveIncident(t.Context(), singleIncident(t, store, job.ProcessInstanceKey).Key))

	resolved := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, int32(1), resolved.Attempts, "the series goes on")
	assert.Equal(t, int32(2), resolved.Retries)
	require.NotNil(t, resolved.RetryAt, "the backoff goes on")
	assert.Equal(t, *inBackoff.RetryAt, *resolved.RetryAt)
	assert.Empty(t, activatedJobKeys(t, engine))
}

func TestResolveIncidentReevaluatesARetriesExpression(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-expression.bpmn", map[string]any{"attemptsAllowed": 0})
	require.Equal(t, int32(1), job.Retries)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
	incident := singleIncident(t, store, job.ProcessInstanceKey)

	_, _, err := engine.ModifyInstance(t.Context(), job.ProcessInstanceKey, nil, nil, map[string]any{"attemptsAllowed": 4})
	require.NoError(t, err)
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

	assert.Equal(t, int32(5), reloadJob(t, store, job.Key).Retries,
		"the expression is evaluated again against the variables at resolution time")
}

// TestResolveIncidentOfAnUncaughtErrorRestoresTheRetries shows a resolution
// starts a fresh series whatever the incident was about: retries left over
// when an error code nothing caught failed the job do not carry over.
func TestResolveIncidentOfAnUncaughtErrorRestoresTheRetries(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "unexpected", new("NOBODY_CATCHES_THIS"), nil, nil, nil, nil))
	failed := reloadJob(t, store, job.Key)
	require.Equal(t, runtime.ActivityStateFailed, failed.State)
	require.Equal(t, int32(2), failed.Retries)

	require.NoError(t, engine.ResolveIncident(t.Context(), singleIncident(t, store, job.ProcessInstanceKey).Key))

	resolved := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, int32(3), resolved.Retries, "the definition's retries are restored")
	assert.Zero(t, resolved.Attempts)
}

// TestResolveIncidentRestartsTheJobItNamesAmongJobsSharingItsToken shows a
// resolution restarts the job its incident names, not whichever job on the
// incident's token it finds first: jobs may share a token, and the store here
// lists a sibling on the same token before the job the incident names.
func TestResolveIncidentRestartsTheJobItNamesAmongJobsSharingItsToken(t *testing.T) {
	store := &pendingJobsNamedLast{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	job := jobs[0]
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))
	sibling := reloadJob(t, store.Storage, job.Key)
	sibling.Key = store.GenerateId()
	sibling.State = runtime.ActivityStateActive
	sibling.Attempts = 1
	sibling.Retries = 2
	require.NoError(t, store.SaveJob(t.Context(), sibling))
	store.last = job.Key

	require.NoError(t, engine.ResolveIncident(t.Context(), singleIncident(t, store.Storage, job.ProcessInstanceKey).Key))

	resolved := reloadJob(t, store.Storage, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, int32(3), resolved.Retries, "the definition's retries are restored")
	assert.Zero(t, resolved.Attempts)
	untouched := reloadJob(t, store.Storage, sibling.Key)
	assert.Equal(t, int32(1), untouched.Attempts, "the sibling keeps its series")
	assert.Equal(t, int32(2), untouched.Retries)
}

// TestResolveIncidentRefusesAJobItNamesWhichDoesNotWaitOnItsToken shows a
// resolution whose incident names a job which is no longer pending, or which
// waits on another token than the incident's, is refused and changes nothing:
// continuing the token without the job would pass a task nobody works on.
func TestResolveIncidentRefusesAJobItNamesWhichDoesNotWaitOnItsToken(t *testing.T) {
	tests := []struct {
		name   string
		change func(job *runtime.Job, store *inmemory.Storage)
		reason string
	}{
		{
			name:   "the job no longer waits",
			change: func(job *runtime.Job, _ *inmemory.Storage) { job.State = runtime.ActivityStateCompleted },
			reason: "is not a pending job",
		},
		{
			name:   "the job waits on another token",
			change: func(job *runtime.Job, store *inmemory.Storage) { job.Token.Key = store.GenerateId() },
			reason: "instead of the incident's token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
			require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))
			incident := singleIncident(t, store, job.ProcessInstanceKey)
			tokenBefore, err := store.GetTokenByKey(t.Context(), incident.Token.Key)
			require.NoError(t, err)
			inconsistent := reloadJob(t, store, job.Key)
			tt.change(&inconsistent, store)
			require.NoError(t, store.SaveJob(t.Context(), inconsistent))

			err = engine.ResolveIncident(t.Context(), incident.Key)

			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("names job %d", job.Key))
			assert.Contains(t, err.Error(), tt.reason)
			assert.Nil(t, singleIncident(t, store, job.ProcessInstanceKey).ResolvedAt, "the incident stays open")
			tokenAfter, err := store.GetTokenByKey(t.Context(), incident.Token.Key)
			require.NoError(t, err)
			assert.Equal(t, tokenBefore.State, tokenAfter.State, "the token is not continued")
		})
	}
}

// TestResolveIncidentWhoseRetriesNoLongerEvaluateNamesTheWayOut shows a
// resolution which cannot evaluate the retries of the job it leaves waiting
// changes nothing and names what to do, and that the way it names works.
func TestResolveIncidentWhoseRetriesNoLongerEvaluateNamesTheWayOut(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries-expression.bpmn", map[string]any{"attemptsAllowed": 0})
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
	incident := singleIncident(t, store, job.ProcessInstanceKey)
	_, _, err := engine.ModifyInstance(t.Context(), job.ProcessInstanceKey, nil, nil, map[string]any{"attemptsAllowed": "many"})
	require.NoError(t, err)

	err = engine.ResolveIncident(t.Context(), incident.Key)

	require.ErrorIs(t, err, ErrIncidentNotResolvable)
	assert.ErrorContains(t, err, fmt.Sprintf("POST /v1/jobs/%d/retries", job.Key))
	assert.Nil(t, singleIncident(t, store, job.ProcessInstanceKey).ResolvedAt, "the incident stays open")
	assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)

	require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, nil))
	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	resolved := reloadJob(t, store, job.Key)
	assert.Equal(t, runtime.ActivityStateActive, resolved.State)
	assert.Equal(t, int32(2), resolved.Retries, "the operator's retries are kept")
}

// TestAFailureNamingItsAttemptSpendsItOnce shows a failure repeated after a
// timeout, or reported late for an attempt which failed already, changes
// nothing once it names its attempt: neither the retries nor the history move,
// and a late report does not create the incident of the attempt now running.
func TestAFailureNamingItsAttemptSpendsItOnce(t *testing.T) {
	t.Run("a repeated failure", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, new(int32(1))))

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, new(int32(1))))

		repeated := reloadJob(t, store, job.Key)
		assert.Equal(t, int32(1), repeated.Attempts)
		assert.Equal(t, int32(2), repeated.Retries)
		failures, err := store.FindJobFailures(t.Context(), job.Key)
		require.NoError(t, err)
		assert.Len(t, failures, 1)
	})
	t.Run("a late failure of an earlier attempt while the last one runs", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, new(int32(1))))
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, new(int32(2))))

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "late", nil, nil, nil, nil, new(int32(1))))

		running := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateActive, running.State, "the last attempt is not taken from its worker")
		assert.Equal(t, int32(1), running.Retries)
		assertNoIncidents(t, store, job.ProcessInstanceKey)
	})
	t.Run("a repeat of the failure which exhausted the retries", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, new(int32(1))))

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, new(int32(1))),
			"answered as recorded, not as a conflict")
		singleIncident(t, store, job.ProcessInstanceKey)
	})
	t.Run("without an attempt every failure spends one", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))

		assert.Equal(t, int32(2), reloadJob(t, store, job.Key).Attempts)
	})
}

// TestARepeatedExhaustingFailureRacingTheFirstIsAnsweredAsRecorded shows a
// repeat of the failure which exhausts the retries, which read the job before
// the first committed, is answered as recorded and not as a conflict.
func TestARepeatedExhaustingFailureRacingTheFirstIsAnsweredAsRecorded(t *testing.T) {
	store := &pausingJobReads{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	jobKey := jobs[0].Key

	paused, resume := store.pauseNextJobRead(t)
	repeated := make(chan error, 1)
	go func() {
		repeated <- engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, new(int32(0)), nil, new(int32(1)))
	}()
	awaitPausedRead(t, paused)
	require.NoError(t, engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, new(int32(0)), nil, new(int32(1))))
	resume()

	require.NoError(t, <-repeated)
	failed := reloadJob(t, store.Storage, jobKey)
	assert.Equal(t, runtime.ActivityStateFailed, failed.State)
	assert.Equal(t, int32(1), failed.Attempts)
	singleIncident(t, store.Storage, failed.ProcessInstanceKey)
}

// TestAFailureRacingTheEndOfTheInstance shows a failure which read an active
// job before its instance ended is told apart once it finds the instance
// ended: the repeat of a failure recorded before the end is answered as
// recorded, as it is when it arrives after the end, and a failure the job
// never recorded is a conflict.
func TestAFailureRacingTheEndOfTheInstance(t *testing.T) {
	ends := []struct {
		name     string
		end      func(t *testing.T, engine *Engine, jobKey int64, instanceKey int64)
		expected error
	}{
		{
			name: "the failure was recorded, then the next attempt completed the instance",
			end: func(t *testing.T, engine *Engine, jobKey int64, _ int64) {
				require.NoError(t, engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, nil, nil, new(int32(1))))
				require.NoError(t, engine.JobCompleteByKey(t.Context(), jobKey, nil))
			},
			expected: nil,
		},
		{
			name: "the failure was recorded, then the instance was cancelled",
			end: func(t *testing.T, engine *Engine, jobKey int64, instanceKey int64) {
				require.NoError(t, engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, nil, nil, new(int32(1))))
				require.NoError(t, engine.CancelInstanceByKey(t.Context(), instanceKey))
			},
			expected: nil,
		},
		{
			name: "the job completed the instance without the failure",
			end: func(t *testing.T, engine *Engine, jobKey int64, _ int64) {
				require.NoError(t, engine.JobCompleteByKey(t.Context(), jobKey, nil))
			},
			expected: ErrJobInTerminalState,
		},
	}
	for _, tt := range ends {
		t.Run(tt.name, func(t *testing.T) {
			store := &pausingJobReads{Storage: inmemory.NewStorage()}
			engine := NewEngine(EngineWithStorage(store))
			t.Cleanup(engine.Stop)
			definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/service-task-retries.bpmn")
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.NoError(t, err)
			jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			jobKey := jobs[0].Key

			paused, resume := store.pauseNextJobRead(t)
			failed := make(chan error, 1)
			go func() {
				failed <- engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, nil, nil, new(int32(1)))
			}()
			awaitPausedRead(t, paused)
			tt.end(t, &engine, jobKey, instance.ProcessInstance().Key)
			resume()

			err = <-failed
			if tt.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.expected)
			}
			failures, err := store.FindJobFailures(t.Context(), jobKey)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(failures), 1, "the racing failure records nothing")
		})
	}
}

// TestAFailureNamingAnAttemptNeverHandedOutIsRefused shows an attempt below 1,
// or beyond the one the job waits for, is refused and changes nothing, with an
// error code as well.
func TestAFailureNamingAnAttemptNeverHandedOutIsRefused(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

	for _, attempt := range []int32{0, 2} {
		err := engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, new(attempt))

		var invalid *InvalidJobRequestError
		require.ErrorAs(t, err, &invalid, "attempt %d", attempt)
		assert.Contains(t, invalid.Reason, "attempt")
	}
	require.ErrorIs(t, engine.JobFailByKey(t.Context(), job.Key, "down", new("CODE"), nil, nil, nil, new(int32(0))), ErrInvalidJobRequest)
	assert.Zero(t, reloadJob(t, store, job.Key).Attempts)
}

func TestRetriesExpressionResultIsCappedBeforeConversion(t *testing.T) {
	capped, err := retriesFromExpressionResult(float64(1e19))
	require.NoError(t, err)
	assert.Equal(t, int32(math.MaxInt32), capped, "a float beyond int64 must not wrap")

	_, err = retriesFromExpressionResult(float64(-1e19))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative")

	capped, err = retriesFromExpressionResult(int64(math.MaxInt64))
	require.NoError(t, err)
	assert.Equal(t, int32(math.MaxInt32), capped)
}

// TestRetryStateSurvivesAnEngineRestart shows everything a retry depends on
// lives in storage: an engine started on the same storage, as after a restart
// or a leader change, keeps the job out of delivery until its deadline and
// continues the series where the previous engine left it.
func TestRetryStateSurvivesAnEngineRestart(t *testing.T) {
	first, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retry-backoff-policy.bpmn", nil)
	require.NoError(t, first.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
	first.Stop()

	restarted := NewEngine(EngineWithStorage(store))
	t.Cleanup(restarted.Stop)

	inBackoff := reloadJob(t, store, job.Key)
	assert.Equal(t, int32(3), inBackoff.Retries)
	assert.Equal(t, int32(1), inBackoff.Attempts)
	require.NotNil(t, inBackoff.RetryAt)
	assert.Empty(t, activatedJobKeys(t, &restarted), "the persisted deadline keeps the job out of delivery")

	assertBackoffAfterFailure(t, &restarted, store, job.Key, nil, 3*time.Second)
	continued := reloadJob(t, store, job.Key)
	assert.Equal(t, int32(2), continued.Attempts, "the series continues")
	assert.Equal(t, int32(2), continued.Retries)
	failures, err := store.FindJobFailures(t.Context(), job.Key)
	require.NoError(t, err)
	assert.Len(t, failures, 2)
}

func TestAttemptsAndFailureHistoryAreRecorded(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "first", nil, nil, nil, new(time.Minute), nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "second", nil, nil, nil, nil, nil))
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "third", nil, nil, nil, nil, nil))

	failures, err := store.FindJobFailures(t.Context(), job.Key)
	require.NoError(t, err)
	require.Len(t, failures, 3)
	incident := singleIncident(t, store, job.ProcessInstanceKey)
	assert.Equal(t, int32(3), failures[0].Attempt, "newest first")
	assert.Equal(t, "third", failures[0].Message)
	assert.Nil(t, failures[0].RetryAt)
	assert.Equal(t, &incident.Key, failures[0].IncidentKey, "the failure which exhausted the retries names the incident")
	assert.Equal(t, int32(2), failures[1].Attempt)
	assert.Nil(t, failures[1].RetryAt)
	assert.Nil(t, failures[1].IncidentKey)
	assert.Equal(t, int32(1), failures[2].Attempt)
	assert.Equal(t, "first", failures[2].Message)
	require.NotNil(t, failures[2].RetryAt)
	assert.WithinDuration(t, failures[2].FailedAt.Add(time.Minute), *failures[2].RetryAt, time.Second)
	for _, failure := range failures {
		assert.Equal(t, job.ProcessInstanceKey, failure.ProcessInstanceKey)
	}

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	assert.Zero(t, reloadJob(t, store, job.Key).Attempts)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "fourth", nil, nil, nil, nil, nil))
	failures, err = store.FindJobFailures(t.Context(), job.Key)
	require.NoError(t, err)
	require.Len(t, failures, 4, "the history outlives the resolution")
	assert.Equal(t, int32(1), failures[0].Attempt, "the series after the resolution counts from one")
}

func TestIncidentOfAnExhaustedJobNamesTheJob(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
	for range 3 {
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "java.net.ConnectException: refused", nil, nil, nil, nil, nil))
	}

	incident := singleIncident(t, store, job.ProcessInstanceKey)
	assert.Equal(t, &job.Key, incident.JobKey)
	assert.Equal(t, fmt.Sprintf("java.net.ConnectException: refused (job %d: 3 attempts, retries exhausted)", job.Key), incident.Message)

	t.Run("a failure without a message says so", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-without-retries.bpmn", nil)

		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "", nil, nil, nil, nil, nil))

		assert.Equal(t, fmt.Sprintf("job %d failed without a message (1 attempt, retries exhausted)", job.Key),
			singleIncident(t, store, job.ProcessInstanceKey).Message)
	})
}

func TestUpdateJobRetries(t *testing.T) {
	t.Run("an active job", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)

		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 7, nil))

		assert.Equal(t, int32(7), reloadJob(t, store, job.Key).Retries)
	})
	t.Run("a job in backoff becomes deliverable at once", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, new(time.Hour), nil))
		require.Empty(t, activatedJobKeys(t, engine))

		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, nil))

		assert.Nil(t, reloadJob(t, store, job.Key).RetryAt)
		assert.Equal(t, []int64{job.Key}, activatedJobKeys(t, engine))
	})
	t.Run("a job may be moved into a backoff", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		retryAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)

		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, &retryAt))

		assert.Equal(t, retryAt, *reloadJob(t, store, job.Key).RetryAt)
		assert.Empty(t, activatedJobKeys(t, engine))
	})
	t.Run("a failed job keeps its incident and the resolution keeps the updated retries", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))
		incident := singleIncident(t, store, job.ProcessInstanceKey)

		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 9, nil))
		assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)
		assert.Nil(t, singleIncident(t, store, job.ProcessInstanceKey).ResolvedAt, "the incident stays open")

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
		resolved := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateActive, resolved.State)
		assert.Equal(t, int32(9), resolved.Retries)
		assert.False(t, resolved.RetriesSetByOperator, "the resolution consumed the update")
	})
	t.Run("a retryAt set on a failed job survives the resolution", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))
		incident := singleIncident(t, store, job.ProcessInstanceKey)
		retryAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, &retryAt))

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))

		resolved := reloadJob(t, store, job.Key)
		assert.Equal(t, runtime.ActivityStateActive, resolved.State)
		assert.Equal(t, int32(2), resolved.Retries)
		require.NotNil(t, resolved.RetryAt)
		assert.Equal(t, retryAt, *resolved.RetryAt, "the operator's deadline is kept")
		assert.Empty(t, activatedJobKeys(t, engine), "the job waits for the operator's deadline")
	})
	t.Run("retries set on an active job are spent by the series which exhausts them", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, nil))
		for range 2 {
			require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil))
		}
		require.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)

		require.NoError(t, engine.ResolveIncident(t.Context(), singleIncident(t, store, job.ProcessInstanceKey).Key))

		assert.Equal(t, int32(3), reloadJob(t, store, job.Key).Retries, "the definition's retries are restored")
	})
	t.Run("retries set on an active job end with an incident of an error code nothing caught", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 10, new(time.Now().Add(time.Hour))))
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "unexpected", new("NOBODY_CATCHES_THIS"), nil, nil, nil, nil))
		require.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)

		require.NoError(t, engine.ResolveIncident(t.Context(), singleIncident(t, store, job.ProcessInstanceKey).Key))

		resolved := reloadJob(t, store, job.Key)
		assert.Equal(t, int32(3), resolved.Retries, "the definition's retries are restored")
		assert.Nil(t, resolved.RetryAt, "the operator's deadline ended with the series")
		assert.Equal(t, []int64{job.Key}, activatedJobKeys(t, engine))
	})
	t.Run("a completed job is a conflict", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

		err := engine.UpdateJobRetries(t.Context(), job.Key, 2, nil)

		require.ErrorIs(t, err, ErrJobInTerminalState)
		assert.Equal(t, int32(3), reloadJob(t, store, job.Key).Retries)
	})
	t.Run("retries out of bounds are refused", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.MaxRetries = 5
		engine, _, job := startRetriedJob(t, limits, "service-task-retries.bpmn", nil)

		require.ErrorIs(t, engine.UpdateJobRetries(t.Context(), job.Key, 0, nil), ErrInvalidJobRequest)
		require.ErrorIs(t, engine.UpdateJobRetries(t.Context(), job.Key, 6, nil), ErrInvalidJobRequest)
	})
	t.Run("a retryAt beyond the backoff cap is refused", func(t *testing.T) {
		limits := DefaultJobRetryLimits()
		limits.MaxRetryBackoff = time.Hour
		engine, store, job := startRetriedJob(t, limits, "service-task-retries.bpmn", nil)

		err := engine.UpdateJobRetries(t.Context(), job.Key, 2, new(time.Now().Add(2*time.Hour)))

		require.ErrorIs(t, err, ErrInvalidJobRequest)
		assert.Contains(t, err.Error(), "jobs.maxRetryBackoff")
		assert.Nil(t, reloadJob(t, store, job.Key).RetryAt, "nothing changed")
		require.NoError(t, engine.UpdateJobRetries(t.Context(), job.Key, 2, new(time.Now().Add(30*time.Minute))), "within the cap")
	})
	t.Run("an unknown job is not found", func(t *testing.T) {
		engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
		t.Cleanup(engine.Stop)

		require.ErrorIs(t, engine.UpdateJobRetries(t.Context(), 42, 2, nil), storage.ErrNotFound)
	})
}

// TestFailOfAJobWhichNoLongerWaitsIsAConflict shows a failure reported for a
// job which already failed, completed or was terminated, as a worker repeating
// a request after a timeout does, is a conflict and not a technical failure.
func TestFailOfAJobWhichNoLongerWaitsIsAConflict(t *testing.T) {
	t.Run("already failed", func(t *testing.T) {
		engine, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))

		err := engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil)

		require.ErrorIs(t, err, ErrJobInTerminalState)
	})
	t.Run("already completed", func(t *testing.T) {
		engine, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

		err := engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, nil, nil, nil)

		require.ErrorIs(t, err, ErrJobInTerminalState)
	})
}

func TestUserTaskJobSpendsRetriesLikeAnyJob(t *testing.T) {
	engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "user-task-retries.bpmn", nil)

	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "form service down", nil, nil, nil, nil, nil))
	assert.Equal(t, int32(1), reloadJob(t, store, job.Key).Retries)
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "form service down", nil, nil, nil, nil, nil))

	assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State)
}

// TestAssignmentKeepsTheRetryStateCommittedMeanwhile shows an assignment which
// read the job before a failure or a retry update committed does not write its
// stale copy of the retry state back over theirs.
func TestAssignmentKeepsTheRetryStateCommittedMeanwhile(t *testing.T) {
	changes := []struct {
		name   string
		change func(t *testing.T, engine *Engine, jobKey int64)
		check  func(t *testing.T, job runtime.Job)
	}{
		{
			name: "a failure into a backoff",
			change: func(t *testing.T, engine *Engine, jobKey int64) {
				require.NoError(t, engine.JobFailByKey(t.Context(), jobKey, "form service down", nil, nil, nil, new(time.Hour), nil))
			},
			check: func(t *testing.T, job runtime.Job) {
				assert.Equal(t, int32(1), job.Retries)
				assert.Equal(t, int32(1), job.Attempts)
				assert.NotNil(t, job.RetryAt)
				assert.Equal(t, new("form service down"), job.LastFailureMessage)
			},
		},
		{
			name: "a retry update",
			change: func(t *testing.T, engine *Engine, jobKey int64) {
				require.NoError(t, engine.UpdateJobRetries(t.Context(), jobKey, 9, nil))
			},
			check: func(t *testing.T, job runtime.Job) {
				assert.Equal(t, int32(9), job.Retries)
			},
		},
	}
	for _, tt := range changes {
		t.Run(tt.name, func(t *testing.T) {
			store := &pausingJobReads{Storage: inmemory.NewStorage()}
			engine := NewEngine(EngineWithStorage(store))
			t.Cleanup(engine.Stop)
			definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/user-task-retries.bpmn")
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.NoError(t, err)
			jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			jobKey := jobs[0].Key

			paused, resume := store.pauseNextJobRead(t)
			assigned := make(chan error, 1)
			go func() { assigned <- engine.JobAssignByKey(t.Context(), jobKey, new("jane")) }()
			awaitPausedRead(t, paused)
			tt.change(t, &engine, jobKey)
			resume()
			require.NoError(t, <-assigned)

			job, err := store.FindJobByJobKey(t.Context(), jobKey)
			require.NoError(t, err)
			assert.Equal(t, new("jane"), job.Assignee)
			tt.check(t, job)
		})
	}
}

// TestAssignmentOfAJobDeletedMeanwhileIsNotFound shows the read of the job
// under the instance lock keeps saying the job is gone, so that the caller
// answers not found rather than a technical failure.
func TestAssignmentOfAJobDeletedMeanwhileIsNotFound(t *testing.T) {
	store := &jobVanishingAfterFirstRead{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/user-task-retries.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	store.vanish.Store(true)

	err = engine.JobAssignByKey(t.Context(), jobs[0].Key, new("jane"))

	require.ErrorIs(t, err, storage.ErrNotFound)
}

// TestCompleteOfAJobWhichNoLongerWaitsIsAConflict shows a completion of a job
// which was terminated or failed with an incident is a conflict, while a job
// completed before is answered as completed again.
func TestCompleteOfAJobWhichNoLongerWaitsIsAConflict(t *testing.T) {
	t.Run("terminated with its instance", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.CancelInstanceByKey(t.Context(), job.ProcessInstanceKey))
		require.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, job.Key).State)

		err := engine.JobCompleteByKey(t.Context(), job.Key, map[string]any{"result": "late"})

		require.ErrorIs(t, err, ErrJobInTerminalState)
		assert.Equal(t, runtime.ActivityStateTerminated, reloadJob(t, store, job.Key).State, "nothing changed")
	})
	t.Run("failed with an incident", func(t *testing.T) {
		engine, store, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "down", nil, nil, new(int32(0)), nil, nil))

		err := engine.JobCompleteByKey(t.Context(), job.Key, nil)

		require.ErrorIs(t, err, ErrJobInTerminalState)
		assert.Equal(t, runtime.ActivityStateFailed, reloadJob(t, store, job.Key).State, "the incident stays")
	})
	t.Run("completed before is completed again", func(t *testing.T) {
		engine, _, job := startRetriedJob(t, DefaultJobRetryLimits(), "service-task-retries.bpmn", nil)
		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))

		require.NoError(t, engine.JobCompleteByKey(t.Context(), job.Key, nil))
	})
}

// TestCompleteRacingTheEndOfTheInstance shows a completion which read an
// active job before its instance ended is told apart once it holds the
// instance: a duplicate of the completion which ended the instance is done, a
// job terminated with its instance is a conflict, neither a technical failure.
func TestCompleteRacingTheEndOfTheInstance(t *testing.T) {
	ends := []struct {
		name     string
		end      func(t *testing.T, engine *Engine, jobKey int64, instanceKey int64)
		expected error
	}{
		{
			name: "another completion of the job ended the instance",
			end: func(t *testing.T, engine *Engine, jobKey int64, _ int64) {
				require.NoError(t, engine.JobCompleteByKey(t.Context(), jobKey, nil))
			},
			expected: nil,
		},
		{
			name: "the instance was cancelled",
			end: func(t *testing.T, engine *Engine, _ int64, instanceKey int64) {
				require.NoError(t, engine.CancelInstanceByKey(t.Context(), instanceKey))
			},
			expected: ErrJobInTerminalState,
		},
	}
	for _, tt := range ends {
		t.Run(tt.name, func(t *testing.T) {
			store := &pausingJobReads{Storage: inmemory.NewStorage()}
			engine := NewEngine(EngineWithStorage(store))
			t.Cleanup(engine.Stop)
			definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/user-task-retries.bpmn")
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.NoError(t, err)
			jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			jobKey := jobs[0].Key

			paused, resume := store.pauseNextJobRead(t)
			completed := make(chan error, 1)
			go func() { completed <- engine.JobCompleteByKey(t.Context(), jobKey, nil) }()
			awaitPausedRead(t, paused)
			tt.end(t, &engine, jobKey, instance.ProcessInstance().Key)
			resume()

			err = <-completed
			if tt.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.expected)
			}
		})
	}
}

// TestARepeatedCompletionRacingTheFirstIsDoneWhileTheInstanceContinues shows
// a completion which read an active job before another completion of it
// committed is as done as any duplicate once it holds the instance, although
// the instance moved on to its next task instead of ending.
func TestARepeatedCompletionRacingTheFirstIsDoneWhileTheInstanceContinues(t *testing.T) {
	store := &pausingJobReads{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/user-task-followed-by-another.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	jobKey := jobs[0].Key

	paused, resume := store.pauseNextJobRead(t)
	repeated := make(chan error, 1)
	go func() { repeated <- engine.JobCompleteByKey(t.Context(), jobKey, nil) }()
	awaitPausedRead(t, paused)
	require.NoError(t, engine.JobCompleteByKey(t.Context(), jobKey, nil))
	resume()

	require.NoError(t, <-repeated)
	pending, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the repeated completion must not complete the next task as well")
	assert.Equal(t, "second-task", pending[0].ElementId)
	assert.Equal(t, runtime.ActivityStateCompleted, reloadJob(t, store.Storage, jobKey).State)
}

// TestUpdateJobRetriesRacingTheEndOfTheInstanceIsAConflict shows an update
// which read an active job before its instance ended is refused as a conflict
// once it holds the instance, not reported as a technical failure.
func TestUpdateJobRetriesRacingTheEndOfTheInstanceIsAConflict(t *testing.T) {
	ends := []struct {
		name string
		end  func(t *testing.T, engine *Engine, jobKey int64, instanceKey int64)
	}{
		{
			name: "the job completes",
			end: func(t *testing.T, engine *Engine, jobKey int64, _ int64) {
				require.NoError(t, engine.JobCompleteByKey(t.Context(), jobKey, nil))
			},
		},
		{
			name: "the instance is cancelled",
			end: func(t *testing.T, engine *Engine, _ int64, instanceKey int64) {
				require.NoError(t, engine.CancelInstanceByKey(t.Context(), instanceKey))
			},
		},
	}
	for _, tt := range ends {
		t.Run(tt.name, func(t *testing.T) {
			store := &pausingJobReads{Storage: inmemory.NewStorage()}
			engine := NewEngine(EngineWithStorage(store))
			t.Cleanup(engine.Stop)
			definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/user-task-retries.bpmn")
			require.NoError(t, err)
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.NoError(t, err)
			jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			jobKey := jobs[0].Key

			paused, resume := store.pauseNextJobRead(t)
			updated := make(chan error, 1)
			go func() { updated <- engine.UpdateJobRetries(t.Context(), jobKey, 5, nil) }()
			awaitPausedRead(t, paused)
			tt.end(t, &engine, jobKey, instance.ProcessInstance().Key)
			resume()

			err = <-updated
			require.ErrorIs(t, err, ErrJobInTerminalState)
			assert.NotEqual(t, int32(5), reloadJob(t, store.Storage, jobKey).Retries, "the ended job is left alone")
		})
	}
}

// pausingJobReads lets a test hold one job read after it read the job, so that
// another change can commit before the reader acts on what it read.
type pausingJobReads struct {
	*inmemory.Storage
	mu     sync.Mutex
	paused chan struct{}
	resume chan struct{}
}

// pauseNextJobRead arms the pause. The returned resume may be called more
// than once and is called when the test ends, so a failed assertion in
// between leaves no reader blocked.
func (s *pausingJobReads) pauseNextJobRead(t *testing.T) (paused <-chan struct{}, resume func()) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = make(chan struct{})
	s.resume = make(chan struct{})
	resumeCh := s.resume
	var once sync.Once
	resume = func() { once.Do(func() { close(resumeCh) }) }
	t.Cleanup(resume)
	return s.paused, resume
}

// jobVanishingAfterFirstRead answers the first read of a job and reports the
// job gone from the second read on, as a deletion between two reads would.
type jobVanishingAfterFirstRead struct {
	*inmemory.Storage
	vanish atomic.Bool
	reads  atomic.Int32
}

// pendingJobsNamedLast lists the pending jobs of an instance with the job named
// last at the end, so that a test does not depend on the order of a map.
type pendingJobsNamedLast struct {
	*inmemory.Storage
	last int64
}

func (s *pendingJobsNamedLast) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error) {
	jobs, err := s.Storage.FindPendingProcessInstanceJobs(ctx, processInstanceKey)
	slices.SortStableFunc(jobs, func(a, b runtime.Job) int {
		return cmp.Compare(boolToOrder(a.Key == s.last), boolToOrder(b.Key == s.last))
	})
	return jobs, err
}

func boolToOrder(last bool) int {
	if last {
		return 1
	}
	return 0
}

func (s *jobVanishingAfterFirstRead) FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error) {
	if s.vanish.Load() && s.reads.Add(1) > 1 {
		return runtime.Job{}, storage.ErrNotFound
	}
	return s.Storage.FindJobByJobKey(ctx, jobKey)
}

func (s *pausingJobReads) FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error) {
	job, err := s.Storage.FindJobByJobKey(ctx, jobKey)
	s.mu.Lock()
	paused, resume := s.paused, s.resume
	s.paused, s.resume = nil, nil
	s.mu.Unlock()
	if paused != nil {
		close(paused)
		select {
		case <-resume:
		case <-ctx.Done():
			return runtime.Job{}, ctx.Err()
		}
	}
	return job, err
}

// retryAttributes picks the attributes of a failure span which describe its retry outcome.
func retryAttributes(span sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	picked := map[attribute.Key]attribute.Value{}
	for _, kv := range span.Attributes() {
		switch kv.Key {
		case otelPkg.AttributeJobFailureOutcome, otelPkg.AttributeJobAttempt, otelPkg.AttributeJobRetries, otelPkg.AttributeJobRetryBackoffMs:
			picked[kv.Key] = kv.Value
		}
	}
	return picked
}

// failingFlushes refuses to commit any batch while failFlushes is set, the
// way a storage outage does: nothing of the batch reaches the storage.
type failingFlushes struct {
	*inmemory.Storage
	failFlushes atomic.Bool
}

func (s *failingFlushes) NewBatch() storage.Batch {
	return &failingFlushBatch{Batch: s.Storage.NewBatch(), store: s}
}

type failingFlushBatch struct {
	storage.Batch
	store *failingFlushes
}

func (b *failingFlushBatch) Flush(ctx context.Context) error {
	if b.store.failFlushes.Load() {
		return errors.New("injected commit failure")
	}
	return b.Batch.Flush(ctx)
}

// awaitPausedRead waits for the armed read to reach its pause, and fails the
// test instead of hanging when a regression keeps the read from happening.
func awaitPausedRead(t *testing.T, paused <-chan struct{}) {
	t.Helper()
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the job read did not reach its pause")
	}
}

// startRetriedJob deploys a fixture of test-cases/job_retries, starts an
// instance and returns the job of its task "retried-task" as it was saved.
// histogramPoint collects the single data point of a float64 histogram.
func histogramPoint(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "metric %s is not a float64 histogram", name)
			require.Len(t, histogram.DataPoints, 1)
			return histogram.DataPoints[0]
		}
	}
	require.FailNow(t, "metric not recorded", name)
	return metricdata.HistogramDataPoint[float64]{}
}

func startRetriedJob(t *testing.T, limits JobRetryLimits, fixture string, variables map[string]any) (*Engine, *inmemory.Storage, runtime.Job) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store), EngineWithJobRetryLimits(limits))
	t.Cleanup(engine.Stop)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/job_retries/"+fixture)
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, variables)
	require.NoError(t, err)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "retried-task", jobs[0].ElementId)
	return &engine, store, jobs[0]
}

// raiseIncidentOnTheTokenOf fails the job's token and its instance with an
// incident which does not name the job, the way a boundary event of the task
// which cannot correlate its message does.
func raiseIncidentOnTheTokenOf(t *testing.T, engine *Engine, store *inmemory.Storage, job runtime.Job) {
	t.Helper()
	instance, err := store.FindProcessInstanceByKey(t.Context(), job.ProcessInstanceKey)
	require.NoError(t, err)
	token, err := store.GetTokenByKey(t.Context(), job.Token.Key)
	require.NoError(t, err)
	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)
	require.NoError(t, batch.WriteTokenIncident(t.Context(), token, instance, errors.New("boundary message correlation failed")))
	require.NoError(t, batch.Flush(t.Context()))
}

func reloadJob(t *testing.T, store *inmemory.Storage, jobKey int64) runtime.Job {
	t.Helper()
	job, err := store.FindJobByJobKey(t.Context(), jobKey)
	require.NoError(t, err)
	return job
}

func activatedJobKeys(t *testing.T, engine *Engine) []int64 {
	t.Helper()
	activated, err := engine.ActivateJobs(t.Context(), "charge-card")
	require.NoError(t, err)
	keys := make([]int64, len(activated))
	for i, job := range activated {
		keys[i] = job.Key()
	}
	return keys
}

// assertBackoffAfterFailure fails the job once and checks when it is handed out next.
func assertBackoffAfterFailure(t *testing.T, engine *Engine, store *inmemory.Storage, jobKey int64, requested *time.Duration, expected time.Duration) {
	t.Helper()
	before := time.Now()
	require.NoError(t, engine.JobFailByKey(t.Context(), jobKey, "down", nil, nil, nil, requested, nil))
	job := reloadJob(t, store, jobKey)
	require.NotNil(t, job.RetryAt, "attempt %d must wait", job.Attempts)
	waited := job.RetryAt.Sub(before)
	assert.GreaterOrEqual(t, waited, expected, "attempt %d", job.Attempts)
	assert.Less(t, waited, expected+time.Second, "attempt %d", job.Attempts)
}

func assertNoIncidents(t *testing.T, store *inmemory.Storage, processInstanceKey int64) {
	t.Helper()
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

func assertInstanceState(t *testing.T, store *inmemory.Storage, processInstanceKey int64, expected runtime.ActivityState) {
	t.Helper()
	instance, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, expected, instance.ProcessInstance().State)
}

func singleIncident(t *testing.T, store *inmemory.Storage, processInstanceKey int64) runtime.Incident {
	t.Helper()
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	return incidents[0]
}

// serviceTaskRetriesDefinitionWith is the service-task-retries fixture with its
// retries attribute replaced by the given one.
func serviceTaskRetriesDefinitionWith(t *testing.T, retriesAttribute string) []byte {
	t.Helper()
	fixture, err := os.ReadFile("./test-cases/job_retries/service-task-retries.bpmn")
	require.NoError(t, err)
	require.Contains(t, string(fixture), `retries="3"`)
	return []byte(strings.Replace(string(fixture), `retries="3"`, retriesAttribute, 1))
}
