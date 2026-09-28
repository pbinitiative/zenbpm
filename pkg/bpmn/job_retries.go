package bpmn

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ErrInvalidJobRequest is wrapped into the error of a job request the engine
// refuses because of what it asks for, such as a negative number of retries.
// The request is wrong, not the engine: repeating it gives the same answer.
var ErrInvalidJobRequest = errors.New("invalid job request")

// ErrJobInTerminalState is wrapped into the error of a request which needs a
// job that is still waiting for a worker or for an operator, but finds it
// completing, completed or terminated.
var ErrJobInTerminalState = errors.New("job no longer waits for a worker or an operator")

// JobRetryLimits are the engine-wide defaults and caps of the retries of jobs.
type JobRetryLimits struct {
	// DefaultRetries initialises the retries of a job whose task definition names none.
	DefaultRetries int32
	// MaxRetries caps the retries of a job, whatever sets them.
	MaxRetries int32
	// DefaultRetryBackoff is the backoff policy of a job whose task definition names
	// none: the n-th failure waits the n-th entry, the last entry repeats. Empty
	// means a failed job is handed out again at once.
	DefaultRetryBackoff []time.Duration
	// MaxRetryBackoff caps every backoff a job waits.
	MaxRetryBackoff time.Duration
}

// DefaultJobRetryLimits are the limits of an engine which is not configured:
// one attempt, so the first failure without an error code creates an incident,
// no backoff, at most 100 retries and a day of backoff.
func DefaultJobRetryLimits() JobRetryLimits {
	return JobRetryLimits{
		DefaultRetries:  1,
		MaxRetries:      100,
		MaxRetryBackoff: 24 * time.Hour,
	}
}

// EngineWithJobRetryLimits sets the defaults and caps of the retries of jobs.
// A zero field takes the value of DefaultJobRetryLimits, so that limits which
// name only what they change do not cap every job at no retries and no backoff.
func EngineWithJobRetryLimits(limits JobRetryLimits) EngineOption {
	return func(engine *Engine) {
		engine.jobRetryLimits = limits.withDefaults()
	}
}

// withDefaults fills the zero fields of the limits from DefaultJobRetryLimits.
// An empty DefaultRetryBackoff already is the default: a failed job is handed
// out again at once.
func (limits JobRetryLimits) withDefaults() JobRetryLimits {
	defaults := DefaultJobRetryLimits()
	if limits.DefaultRetries == 0 {
		limits.DefaultRetries = defaults.DefaultRetries
	}
	if limits.MaxRetries == 0 {
		limits.MaxRetries = defaults.MaxRetries
	}
	if limits.MaxRetryBackoff == 0 {
		limits.MaxRetryBackoff = defaults.MaxRetryBackoff
	}
	return limits
}

// initialRetries evaluates the retries attribute of the element's task
// definition: a literal, a FEEL expression evaluated against variables, or,
// when absent, the engine default. The result is capped at MaxRetries.
func (engine *Engine) initialRetries(element bpmn20.InternalTask, variables map[string]any) (int32, error) {
	raw := strings.TrimSpace(element.GetTaskDefinition().Retries)
	if raw == "" {
		return min(engine.jobRetryLimits.DefaultRetries, engine.jobRetryLimits.MaxRetries), nil
	}
	if !extensions.IsExpression(raw) {
		retries, err := extensions.ParseRetries(raw)
		if err != nil {
			return 0, err
		}
		return min(retries, engine.jobRetryLimits.MaxRetries), nil
	}
	result, err := engine.evaluateExpression(raw, variables)
	if err != nil {
		return 0, fmt.Errorf("failed to evaluate retries: %w", err)
	}
	retries, err := retriesFromExpressionResult(result)
	if err != nil {
		return 0, fmt.Errorf("retries expression %q: %w", raw, err)
	}
	return min(retries, engine.jobRetryLimits.MaxRetries), nil
}

func retriesFromExpressionResult(result any) (int32, error) {
	var retries int64
	switch value := result.(type) {
	case int64:
		retries = value
	case int:
		retries = int64(value)
	case int32:
		retries = int64(value)
	case float64:
		if value != math.Trunc(value) {
			return 0, fmt.Errorf("evaluated to %v, which is not an integer", value)
		}
		if value < 0 {
			return 0, fmt.Errorf("evaluated to %v, which is negative", value)
		}
		// converting a float beyond the range of int64 is not defined, so the
		// cap is applied before the conversion
		retries = int64(min(value, math.MaxInt32))
	case string:
		return extensions.ParseRetries(value)
	default:
		return 0, fmt.Errorf("evaluated to %v (%T), which is not an integer", result, result)
	}
	if retries < 0 {
		return 0, fmt.Errorf("evaluated to %d, which is negative", retries)
	}
	return int32(min(retries, math.MaxInt32)), nil
}

// retryBackoffPolicy evaluates the retryBackoff attribute of the element's
// task definition: a literal duration or list of durations, or a FEEL
// expression evaluated against variables whose result is such a string or a
// list of duration strings. Absent, the policy is empty and the engine default
// applies when the job fails.
func (engine *Engine) retryBackoffPolicy(element bpmn20.InternalTask, variables map[string]any) ([]time.Duration, error) {
	raw := strings.TrimSpace(element.GetTaskDefinition().RetryBackoff)
	if raw == "" {
		return nil, nil
	}
	if !extensions.IsExpression(raw) {
		return extensions.ParseRetryBackoff(raw)
	}
	result, err := engine.evaluateExpression(raw, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate retryBackoff: %w", err)
	}
	switch value := result.(type) {
	case string:
		return extensions.ParseRetryBackoff(value)
	case []any:
		policy := make([]time.Duration, len(value))
		for i, entry := range value {
			text, ok := entry.(string)
			if !ok {
				return nil, fmt.Errorf("retryBackoff expression %q: entry %v (%T) is not a duration string", raw, entry, entry)
			}
			backoff, err := extensions.ParseBackoffDuration(text)
			if err != nil {
				return nil, fmt.Errorf("retryBackoff expression %q: %w", raw, err)
			}
			policy[i] = backoff
		}
		return policy, nil
	default:
		return nil, fmt.Errorf("retryBackoff expression %q evaluated to %v (%T), which is neither a duration string nor a list of them", raw, result, result)
	}
}

// backoffForAttempt is how long the job waits after its n-th failure: the
// backoff the worker asked for, else the n-th entry of the job's policy, else
// of the engine's default policy, the last entry of a policy repeating. Every
// backoff is capped at MaxRetryBackoff.
func (engine *Engine) backoffForAttempt(job runtime.Job, attempt int32, requested *time.Duration) time.Duration {
	policy := job.RetryBackoff
	if len(policy) == 0 {
		policy = engine.jobRetryLimits.DefaultRetryBackoff
	}
	var backoff time.Duration
	switch {
	case requested != nil:
		backoff = *requested
	case len(policy) > 0:
		backoff = policy[min(int(attempt), len(policy))-1]
	}
	return min(backoff, engine.jobRetryLimits.MaxRetryBackoff)
}

// failJobWithoutErrorCode spends one attempt of the job. With retries left the
// job stays active and is handed out again after its backoff, and the variables
// are dropped: the next attempt starts from the same input. Without retries the
// job fails with an incident naming the attempts and keeps the variables as its
// output, as a failed job always did. Either way the failure is recorded. It
// reports whether the job was left for another attempt.
func (engine *Engine) failJobWithoutErrorCode(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	message string,
	variables map[string]interface{},
	retries *int32,
	retryBackoff *time.Duration,
) (retried bool, err error) {
	remaining := max(job.Retries-1, 0)
	if retries != nil {
		remaining = min(*retries, engine.jobRetryLimits.MaxRetries)
	}
	now := time.Now()
	job.Attempts++
	job.Retries = remaining
	job.LastFailureMessage = &message
	failure := runtime.JobFailure{
		Key:                engine.generateKey(),
		JobKey:             job.Key,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Attempt:            job.Attempts,
		FailedAt:           now,
		Message:            message,
	}
	if remaining == 0 {
		job.RetryAt = nil
		if err := engine.raiseJobIncident(ctx, batch, job, exhaustedRetriesIncidentMessage(job, message), variables, &failure); err != nil {
			return false, err
		}
		engine.logger.Error(fmt.Sprintf("job %d of type %s in instance %d failed after %s, retries exhausted, incident created: %s",
			job.Key, job.Type, job.ProcessInstanceKey, attemptsPhrase(job.Attempts), message))
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String(otelPkg.AttributeJobFailureOutcome, otelPkg.JobFailureOutcomeIncident),
			attribute.Int(otelPkg.AttributeJobAttempt, int(job.Attempts)),
			attribute.Int(otelPkg.AttributeJobRetries, 0),
		)
		return false, nil
	}

	backoff := engine.backoffForAttempt(job, job.Attempts, retryBackoff)
	job.RetryAt = nil
	if backoff > 0 {
		job.RetryAt = new(now.Add(backoff))
	}
	failure.RetryAt = job.RetryAt
	if err := batch.SaveJob(ctx, job); err != nil {
		return false, err
	}
	if err := batch.SaveJobFailure(ctx, failure); err != nil {
		return false, err
	}
	if err := batch.Flush(ctx); err != nil {
		return false, fmt.Errorf("failed to record failure %d of job %d: %w", job.Attempts, job.Key, err)
	}

	nextDelivery := "at once"
	if job.RetryAt != nil {
		nextDelivery = "not before " + job.RetryAt.Format(time.RFC3339Nano)
	}
	engine.logger.Warn(fmt.Sprintf("job %d of type %s in instance %d failed (attempt %d/%d): %s; next delivery %s",
		job.Key, job.Type, job.ProcessInstanceKey, job.Attempts, job.Attempts+remaining, message, nextDelivery))
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(otelPkg.AttributeJobFailureOutcome, otelPkg.JobFailureOutcomeRetry),
		attribute.Int(otelPkg.AttributeJobAttempt, int(job.Attempts)),
		attribute.Int(otelPkg.AttributeJobRetries, int(remaining)),
		attribute.Int64(otelPkg.AttributeJobRetryBackoffMs, backoff.Milliseconds()),
	)
	if engine.metrics != nil {
		typeAttribute := metric.WithAttributes(attribute.String("type", job.Type))
		engine.metrics.JobsRetried.Add(ctx, 1, typeAttribute)
		engine.metrics.JobRetryBackoff.Record(ctx, float64(backoff)/float64(time.Millisecond), typeAttribute)
	}
	return true, nil
}

// exhaustedRetriesIncidentMessage names the job and its attempts next to the
// worker's message; a worker which sent none gets that said instead of an
// incident which starts with a blank.
func exhaustedRetriesIncidentMessage(job runtime.Job, message string) string {
	if message == "" {
		return fmt.Sprintf("job %d failed without a message (%s, retries exhausted)", job.Key, attemptsPhrase(job.Attempts))
	}
	return fmt.Sprintf("%s (job %d: %s, retries exhausted)", message, job.Key, attemptsPhrase(job.Attempts))
}

func attemptsPhrase(attempts int32) string {
	if attempts == 1 {
		return "1 attempt"
	}
	return fmt.Sprintf("%d attempts", attempts)
}

// UpdateJobRetries sets the remaining retries of an active or failed job and
// when it is handed out next; a nil retryAt, or one in the past, makes it
// deliverable at once. A retryAt further ahead than MaxRetryBackoff allows is
// refused like any other backoff beyond the cap would be lowered: a mistyped
// year would otherwise park the job without an incident. A failed job keeps
// its incident open: resolving it then keeps the retries and the retryAt set
// here instead of restoring the definition's retries.
func (engine *Engine) UpdateJobRetries(ctx context.Context, jobKey int64, retries int32, retryAt *time.Time) (retErr error) {
	if retries < 1 || retries > engine.jobRetryLimits.MaxRetries {
		return fmt.Errorf("%w: retries of job %d must be between 1 and %d (jobs.maxRetries), got %d",
			ErrInvalidJobRequest, jobKey, engine.jobRetryLimits.MaxRetries, retries)
	}
	if latest := time.Now().Add(engine.jobRetryLimits.MaxRetryBackoff); retryAt != nil && retryAt.After(latest) {
		return fmt.Errorf("%w: retryAt of job %d must not be later than %s from now (jobs.maxRetryBackoff), got %s",
			ErrInvalidJobRequest, jobKey, engine.jobRetryLimits.MaxRetryBackoff, retryAt.Format(time.RFC3339))
	}
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d, err: %s", jobKey, err)
	}
	// checked before the batch as well: the instance of a completed job may be
	// completed itself, and a batch cannot be opened for it
	if err := jobRetriesAreUpdatable(job); err != nil {
		return err
	}
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey)
	}
	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		if errors.Is(err, ErrInstanceAlreadyTerminal) {
			// the job ended with its instance between the check above and the lock
			return fmt.Errorf("%w: cannot update the retries of job %d: %w", ErrJobInTerminalState, jobKey, err)
		}
		return fmt.Errorf("failed to create engine batch for job %d: %w", jobKey, err)
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	// refresh: the batch holds the instance, so nothing changes the job from here on
	job, err = engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to find job with key %d: %w", jobKey, err)
	}
	if err := jobRetriesAreUpdatable(job); err != nil {
		return err
	}
	now := time.Now()
	job.Retries = retries
	job.RetryAt = nil
	if retryAt != nil && retryAt.After(now) {
		job.RetryAt = retryAt
	}
	job.RetriesUpdatedAt = &now
	if err := batch.SaveJob(ctx, job); err != nil {
		return err
	}
	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("failed to update retries of job %d: %w", jobKey, err)
	}
	return nil
}

// jobRetriesAreUpdatable refuses a job which no longer waits for a worker or an operator.
func jobRetriesAreUpdatable(job runtime.Job) error {
	if job.State != runtime.ActivityStateActive && job.State != runtime.ActivityStateFailed {
		return fmt.Errorf("%w: cannot update the retries of job %d in state %s", ErrJobInTerminalState, job.Key, job.State)
	}
	return nil
}
