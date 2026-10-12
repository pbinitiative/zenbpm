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

// InvalidJobRequestError is the error of a job request the engine refuses for
// what it asks. It wraps ErrInvalidJobRequest; Reason names the field and the
// value, and is what the requester is told, without the wrapping of the layers
// the error passes on its way.
type InvalidJobRequestError struct {
	Reason string
}

func (e *InvalidJobRequestError) Error() string {
	return ErrInvalidJobRequest.Error() + ": " + e.Reason
}

func (e *InvalidJobRequestError) Unwrap() error {
	return ErrInvalidJobRequest
}

func invalidJobRequestf(format string, args ...any) error {
	return &InvalidJobRequestError{Reason: fmt.Sprintf(format, args...)}
}

// ErrJobInTerminalState is wrapped into the error of a request which needs a
// job that is still waiting for a worker or for an operator, but finds it
// completing, completed or terminated.
var ErrJobInTerminalState = errors.New("job no longer waits for a worker or an operator")

// ErrDeliverySuperseded is wrapped into the error of a failure naming a
// delivery of the job which another delivery superseded before the failure was
// recorded: the job was handed out again after the lock of the named delivery
// lapsed or its leader changed, and the newer delivery decides. Nothing changes.
var ErrDeliverySuperseded = errors.New("job was handed out again after the delivery the request names")

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
		DefaultRetries:  extensions.DefaultJobRetries,
		MaxRetries:      extensions.MaxJobRetries,
		MaxRetryBackoff: extensions.MaxJobRetryBackoff,
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
		return 0, fmt.Errorf("evaluated to %s, which is not an integer", shortDescriptionOf(result))
	}
	if retries < 0 {
		return 0, fmt.Errorf("evaluated to %d, which is negative", retries)
	}
	// the caller caps at jobs.maxRetries; this cap only keeps the conversion in range
	if retries > math.MaxInt32 {
		return math.MaxInt32, nil
	}
	return int32(retries), nil
}

// retryBackoffPolicy evaluates the retryBackoff attribute of the element's
// task definition: a literal duration or list of durations, or a FEEL
// expression evaluated against variables whose result is such a string, a FEEL
// duration or a list of duration strings and FEEL durations. Absent, the policy
// is empty and the engine default applies when the job fails.
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
	if text, ok := result.(string); ok {
		return extensions.ParseRetryBackoff(text)
	}
	entries, isList := result.([]any)
	if !isList {
		if !isFeelDuration(result) {
			return nil, fmt.Errorf("retryBackoff expression %q evaluated to %s, which is neither a duration nor a list of durations", raw, shortDescriptionOf(result))
		}
		entries = []any{result}
	}
	policy := make([]time.Duration, len(entries))
	for i, entry := range entries {
		backoff, err := backoffOfExpressionEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("retryBackoff expression %q: %w", raw, err)
		}
		policy[i] = backoff
	}
	return policy, nil
}

// backoffOfExpressionEntry reads one backoff a retryBackoff expression
// produced: an ISO-8601 duration string or a FEEL duration.
func backoffOfExpressionEntry(entry any) (time.Duration, error) {
	if text, ok := entry.(string); ok {
		return extensions.ParseBackoffDuration(text)
	}
	if isFeelDuration(entry) {
		return backoffOfFeelDuration(entry.(map[string]any))
	}
	return 0, fmt.Errorf("entry %s is not a duration string or a FEEL duration", shortDescriptionOf(entry))
}

// isFeelDuration reports whether an expression result is a FEEL duration such
// as duration("PT10S"): the FEEL runtime hands it over as the fields of the
// Luxon Duration which holds it.
func isFeelDuration(result any) bool {
	fields, ok := result.(map[string]any)
	return ok && fields["isLuxonDuration"] == true
}

// backoffOfFeelDuration converts a FEEL duration the way ParseBackoffDuration
// converts an ISO-8601 one: a week counts seven days and a day 24 hours, years
// and months are refused because their length depends on the date, and a
// duration beyond what time.Duration holds saturates at its maximum. A fraction
// of a second is dropped, as the job keeps its policy in whole seconds. A
// duration which names no unit, such as duration("PT"), is refused like the
// string "PT", and so is one which does not carry its units the way the FEEL
// runtime hands them over, as a context made to look like a duration may: it
// would otherwise read as no backoff at all.
func backoffOfFeelDuration(duration map[string]any) (time.Duration, error) {
	if invalid := duration["invalid"]; invalid != nil {
		return 0, fmt.Errorf("FEEL duration is invalid: %s", shortDescriptionOf(invalid))
	}
	units, ok := duration["values"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("FEEL duration carries its units as %s instead of a context of units and amounts", shortDescriptionOf(duration["values"]))
	}
	if len(units) == 0 {
		return 0, errors.New("FEEL duration names no amount of any unit")
	}
	seconds := 0.0
	for unit, amount := range units {
		count, ok := amount.(float64)
		if !ok {
			whole, isWhole := amount.(int64)
			if !isWhole {
				return 0, fmt.Errorf("FEEL duration has %s %s, which is not a number", shortDescriptionOf(amount), unit)
			}
			count = float64(whole)
		}
		if math.IsNaN(count) || math.IsInf(count, 0) {
			return 0, fmt.Errorf("FEEL duration has %v %s, which is not a finite number", count, unit)
		}
		switch unit {
		case "years", "quarters", "months":
			if count != 0 {
				return 0, fmt.Errorf("FEEL duration uses years or months, whose length depends on the date: use weeks, days, hours, minutes or seconds")
			}
		case "weeks":
			seconds += count * 7 * 24 * 60 * 60
		case "days":
			seconds += count * 24 * 60 * 60
		case "hours":
			seconds += count * 60 * 60
		case "minutes":
			seconds += count * 60
		case "seconds":
			seconds += count
		case "milliseconds":
			seconds += count / 1000
		default:
			return 0, fmt.Errorf("FEEL duration has the unknown unit %q", unit)
		}
	}
	if seconds < 0 {
		return 0, fmt.Errorf("FEEL duration of %v seconds must not be negative", seconds)
	}
	// converting a float beyond the range of int64 is not defined, so the
	// saturation is applied before the conversion
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64), nil
	}
	return time.Duration(seconds) * time.Second, nil
}

// shortDescriptionOf names an expression result an error message quotes: its
// value and type, the value cut short, as an expression may produce a large
// list or context, and a FEEL date or duration is an object of many fields.
func shortDescriptionOf(value any) string {
	const maxLength = 80
	text := []rune(fmt.Sprintf("%v", value))
	if len(text) > maxLength {
		text = append(text[:maxLength], []rune("...")...)
	}
	return fmt.Sprintf("%s (%T)", string(text), value)
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
// output, as a failed job always did. Either way the failure is recorded,
// with the delivery it names, if any. It reports whether the job was left for
// another attempt.
func (engine *Engine) failJobWithoutErrorCode(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	message string,
	variables map[string]interface{},
	retries *int32,
	retryBackoff *time.Duration,
	deliveryToken *int64,
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
		DeliveryToken:      deliveryToken,
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
// deliverable at once; checkOperatorRetries names what is refused. A failed
// job keeps its incident open: resolving it then keeps the retries and the
// retryAt set here instead of restoring the definition's retries. To set them
// and resolve the incident at once, see WithJobRetries.
func (engine *Engine) UpdateJobRetries(ctx context.Context, jobKey int64, retries int32, retryAt *time.Time) (retErr error) {
	if err := engine.checkOperatorRetries(jobKey, retries, retryAt); err != nil {
		return err
	}
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d, err: %s", jobKey, err)
	}
	job, _, batch, err := engine.lockInstanceOfJob(ctx, job, "update the retries of", ensureJobRetriesAreUpdatable)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	setOperatorRetries(&job, retries, retryAt, time.Now())
	if err := batch.SaveJob(ctx, job); err != nil {
		return err
	}
	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("failed to update retries of job %d: %w", jobKey, err)
	}
	return nil
}

// checkOperatorRetries refuses retries an operator sets for a job outside 1 to
// MaxRetries, and a retryAt further ahead than MaxRetryBackoff allows: a
// mistyped year would otherwise park the job without an incident.
func (engine *Engine) checkOperatorRetries(jobKey int64, retries int32, retryAt *time.Time) error {
	if retries < 1 || retries > engine.jobRetryLimits.MaxRetries {
		return invalidJobRequestf("retries of job %d must be between 1 and %d (jobs.maxRetries), got %d",
			jobKey, engine.jobRetryLimits.MaxRetries, retries)
	}
	if latest := time.Now().Add(engine.jobRetryLimits.MaxRetryBackoff); retryAt != nil && retryAt.After(latest) {
		return invalidJobRequestf("retryAt of job %d must not be later than %s from now (jobs.maxRetryBackoff), got %s",
			jobKey, engine.jobRetryLimits.MaxRetryBackoff, retryAt.Format(time.RFC3339))
	}
	return nil
}

// setOperatorRetries gives the job the retries an operator set and hands it
// out at retryAt, or at once when retryAt is nil or not after now. The next
// resolution of the job's own incident keeps both instead of restoring the
// retries of the task definition.
func setOperatorRetries(job *runtime.Job, retries int32, retryAt *time.Time, now time.Time) {
	job.Retries = retries
	job.RetryAt = nil
	if retryAt != nil && retryAt.After(now) {
		job.RetryAt = retryAt
	}
	job.RetriesSetByOperator = true
}

// ensureJobRetriesAreUpdatable refuses a job which no longer waits for a worker or an operator.
func ensureJobRetriesAreUpdatable(job runtime.Job) error {
	if job.State != runtime.ActivityStateActive && job.State != runtime.ActivityStateFailed {
		return fmt.Errorf("%w: cannot update the retries of job %d in state %s", ErrJobInTerminalState, job.Key, job.State)
	}
	return nil
}
