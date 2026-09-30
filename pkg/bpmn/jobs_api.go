package bpmn

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// JobAssignByKey sets (or clears) the assignee of a job. Pass nil to unassign.
//
// Saving a job writes all of it, its retry state included, so the job is read
// again under the lock of its process instance, the lock failures and retry
// updates hold: a copy read before one of them committed would undo it.
func (engine *Engine) JobAssignByKey(ctx context.Context, jobKey int64, assignee *string) error {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}
	engine.runningInstances.lockInstance(job.ProcessInstanceKey)
	defer engine.runningInstances.unlockInstance(job.ProcessInstanceKey)
	job, err = engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		// not turned into an engine error, so that a job deleted meanwhile is still not found
		return fmt.Errorf("failed to refresh job with key %d: %w", jobKey, err)
	}
	job.Assignee = assignee
	if err := engine.persistence.SaveJob(ctx, job); err != nil {
		return newEngineErrorf("failed to save job assignee for key: %d", jobKey)
	}
	return nil
}

// JobFailByKey reports that a worker could not finish an external job.
//
// With an error code it is a BPMN error: a matching error boundary event or
// error event sub-process takes over, otherwise the job fails with an incident.
// Retries are neither consulted nor spent.
//
// Without an error code (nil or empty) one attempt of the job is spent. retries
// is what remains afterwards (nil: one less than now) and retryBackoff how long
// the job waits before it is handed out again (nil: the policy of the task
// definition, else the engine's default). With retries remaining the job stays
// active, no incident is created and the variables are dropped; at zero the
// job fails with an incident and keeps the variables as its output.
//
// attempt, when given, is the attempt of the delivery the failure belongs to,
// and makes a failure without an error code count once: a failure of an
// attempt the job recorded already, repeated after a timeout or reported late
// by a worker whose lock lapsed, changes nothing and returns nil, whatever
// state the job is in now. An attempt beyond the one the job waits for is
// refused with ErrInvalidJobRequest. Without attempt every failure spends one.
//
// retries, retryBackoff and attempt are validated first, whatever the error
// code: an invalid value is refused with ErrInvalidJobRequest and changes nothing.
func (engine *Engine) JobFailByKey(
	ctx context.Context,
	jobKey int64,
	message string,
	errorCode *string,
	variables map[string]interface{},
	retries *int32,
	retryBackoff *time.Duration,
	attempt *int32,
) (retErr error) {
	if attempt != nil && *attempt < 1 {
		return invalidJobRequestf("attempt of job %d must be at least 1, got %d", jobKey, *attempt)
	}
	if retries != nil && *retries < 0 {
		return invalidJobRequestf("retries of job %d must not be negative, got %d", jobKey, *retries)
	}
	if retryBackoff != nil && *retryBackoff < 0 {
		return invalidJobRequestf("retry backoff of job %d must not be negative, got %s", jobKey, *retryBackoff)
	}
	// clients built before retries existed send an empty code when they mean none
	if errorCode != nil && *errorCode == "" {
		errorCode = nil
	}
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d, err: %s", jobKey, err)
	}
	if errorCode == nil && attempt != nil && failureAlreadyRecorded(job, *attempt) {
		engine.logger.Debug("failure of an attempt the job recorded already, nothing changes", "job", job.Key, "attempt", *attempt, "attempts", job.Attempts)
		return nil
	}

	ctx, failJobSpan := engine.tracer.Start(ctx, fmt.Sprintf("job:%s", job.Type), trace.WithAttributes(
		attribute.Int64("key", job.Key),
	))
	defer func() {
		if retErr != nil {
			failJobSpan.RecordError(retErr)
			failJobSpan.SetStatus(codes.Error, retErr.Error())
		}
		failJobSpan.End()
	}()

	// a failure of the same attempt which committed meanwhile, even the one
	// which exhausted the retries, answers this one as recorded below
	ensureJobTakesTheFailure := func(job runtime.Job) error {
		if errorCode == nil && attempt != nil && failureAlreadyRecorded(job, *attempt) {
			return nil
		}
		return ensureJobStillWaits(job)
	}
	job, instance, batch, err := engine.lockInstanceOfJob(ctx, job, "fail", ensureJobTakesTheFailure)
	if err != nil {
		if errorCode == nil && attempt != nil && errors.Is(err, ErrInstanceAlreadyTerminal) {
			return engine.failureRacedTheEndOfTheInstance(ctx, jobKey, *attempt, err)
		}
		return err
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()
	if errorCode == nil {
		if attempt != nil && failureAlreadyRecorded(job, *attempt) {
			batch.Clear(ctx)
			return nil
		}
		if attempt != nil && *attempt > job.Attempts+1 {
			return invalidJobRequestf("attempt %d of job %d was never handed out: the job waits for attempt %d", *attempt, jobKey, job.Attempts+1)
		}
	}

	failJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)

	retried := false
	defer func() {
		if retErr == nil && !retried {
			engine.metrics.JobsFailed.Add(ctx, 1, metric.WithAttributes(
				attribute.String("type", job.Type),
				attribute.Bool("internal", false),
			))
			engine.recordJobLifetime(ctx, job, "failed")
		}
	}()

	if errorCode == nil {
		retried, err = engine.failJobWithoutErrorCode(ctx, &batch, job, message, variables, retries, retryBackoff)
		if err != nil {
			return fmt.Errorf("failed to fail job %d: %w", job.Key, err)
		}
		return nil
	}

	target, err := engine.findErrorCatchTarget(ctx, &batch, instance, job.Token, errorCode)
	if err != nil {
		return err
	}

	if target != nil {
		switch {
		case target.boundary != nil:
			if handled, err := engine.processBoundaryErrorEvent(ctx, &batch, job, instance, target.boundary, variables); err != nil {
				return err
			} else if handled {
				return nil
			}
		case target.eventSubprocess != nil:
			if handled, err := engine.processErrorEventSubprocessForJob(ctx, &batch, job, target.eventSubprocess, variables); err != nil {
				if errors.Is(err, ErrMaxProcessInstanceNestingDepthExceeded) {
					batch.discardWrites()
					if incidentErr := engine.failJobWithIncident(ctx, &batch, job, err.Error(), errorCode, variables); incidentErr != nil {
						return errors.Join(err, incidentErr)
					}
					return nil
				}
				return err
			} else if handled {
				return nil
			}
		}
	}

	err = engine.failJobWithIncident(ctx, &batch, job, message, errorCode, variables)
	if err != nil {
		return fmt.Errorf("failed to fail job %+v: %w", job, err)
	}
	return nil
}

func (engine *Engine) processBoundaryErrorEvent(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	instance runtime.ProcessInstance,
	boundaryMatch *boundaryErrorMatch,
	variables map[string]interface{},
) (bool, error) {

	job.State = runtime.ActivityStateTerminated
	if err := batch.SaveJob(ctx, job); err != nil {
		return false, err
	}

	boundaryInstance, tokens, err := engine.prepareBoundaryErrorTransition(ctx, batch, instance, boundaryMatch, variables, true)
	if err != nil {
		return false, err
	}

	if err := batch.saveTokensAndFlush(ctx, tokens); err != nil {
		return false, fmt.Errorf("failed to fail job %+v by boundary error handling: %w", job, err)
	}

	return true, engine.RunProcessInstance(ctx, boundaryInstance, tokens)
}

func (engine *Engine) failJobWithIncident(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	message string,
	errorCode *string,
	variables map[string]interface{},
) error {
	code := ptr.Deref(errorCode, "")
	return engine.raiseJobIncident(ctx, batch, job, fmt.Sprintf("%s: %s", message, code), variables, nil)
}

// raiseJobIncident fails the job, creates an incident naming it and flushes the
// batch. A failure, when given, is recorded carrying the key of the incident.
func (engine *Engine) raiseJobIncident(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	incidentMessage string,
	variables map[string]interface{},
	failure *runtime.JobFailure,
) error {
	job.State = runtime.ActivityStateFailed
	// every incident of the job ends its series: retries an operator set before
	// are forgotten, and resolving the incident restores the definition's
	// unless the operator sets new ones meanwhile
	job.RetriesSetByOperator = false
	if variables != nil {
		job.OutputVariables = variables
	}
	if err := batch.SaveJob(ctx, job); err != nil {
		return err
	}

	incident := createNewIncidentFromToken(errors.New(incidentMessage), job.Token, engine)
	incident.JobKey = &job.Key
	if err := batch.SaveIncident(ctx, incident); err != nil {
		return err
	}
	if failure != nil {
		failure.IncidentKey = &incident.Key
		if err := batch.SaveJobFailure(ctx, *failure); err != nil {
			return err
		}
	}

	return batch.Flush(ctx)
}

func (engine *Engine) ActivateJobs(ctx context.Context, jobType string) ([]ActivatedJob, error) {
	jobs, err := engine.persistence.FindActiveJobsByType(ctx, jobType)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("failed to find active jobs by type"), err)
	}

	activatedJobs := make([]ActivatedJob, 0)
	for _, job := range jobs {

		processInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
		if err != nil {
			return nil, fmt.Errorf("failed to find process instance for job key: %d: %w", job.Key, err)
		}
		localVars := maps.Clone(job.InputVariables)
		if localVars == nil {
			localVars = make(map[string]any)
		}
		aj := &activatedJob{
			processInstanceInfo: processInstance,
			key:                 job.Key,
			processInstanceKey:  job.ProcessInstanceKey,
			elementId:           job.ElementId,
			createdAt:           job.CreatedAt,
			localVariables:      localVars,
			outputVariables:     map[string]interface{}{},
		}
		activatedJobs = append(activatedJobs, aj)
	}
	return activatedJobs, nil
}

func (engine *Engine) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]interface{}) (retErr error) {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}

	if job.State == runtime.ActivityStateCompleted {
		return engine.repeatedCompletion(ctx, job)
	}

	// checked before the batch as well: a terminated or failed job may belong
	// to an instance which ended, and a batch cannot be opened for it
	if err := ensureJobStillWaits(job); err != nil {
		return err
	}

	ctx, completeJobSpan := engine.tracer.Start(ctx, fmt.Sprintf("job:%s", job.Type), trace.WithAttributes(
		attribute.Int64("key", job.Key),
	))
	completeJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)
	defer func() {
		if retErr != nil {
			completeJobSpan.RecordError(retErr)
			completeJobSpan.SetStatus(codes.Error, retErr.Error())
		}
		completeJobSpan.End()
	}()

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		if errors.Is(err, ErrInstanceAlreadyTerminal) {
			return engine.completionRacedTheEndOfTheInstance(ctx, jobKey, err)
		}
		return newEngineErrorf("failed to create engine batch")
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	if err := validateExternalTriggerInstanceState(instance, fmt.Sprintf("complete job %d", jobKey)); err != nil {
		return err
	}

	//refresh token
	job, err = engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}
	if job.State == runtime.ActivityStateCompleted {
		// a repeated completion which raced the one that committed while the instance keeps running
		batch.Clear(ctx)
		return engine.repeatedCompletion(ctx, job)
	}
	if err := ensureJobStillWaits(job); err != nil {
		return err
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	variableHolder.SetLocalVariables(job.InputVariables)

	task := instance.ProcessInstance().Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
	if task == nil {
		return errors.Join(newEngineErrorf("failed to find task element for job: %+v", job))
	}
	outputVariables, err := variableHolder.PropagateOnlyMappedOutputs(task.GetOutputMapping(), variables, engine.evaluateExpression)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to map output variables for job: %+v", job))
	}
	err = batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                job.Token.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		ElementId:          job.Token.ElementId,
		ElementType:        string(task.GetType()),
		ExecutionTokenKey:  job.Token.Key,
		OutputVariables:    outputVariables,
		CompletedAt:        new(time.Now()),
	})
	if err != nil {
		return err
	}

	err = engine.cancelBoundarySubscriptions(ctx, &batch, instance.ProcessInstance().Key, job.Token)
	if err != nil {
		return fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.handleElementTransition(ctx, &batch, instance, task, job.Token)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}

	job.State = runtime.ActivityStateCompleted
	job.OutputVariables = variables
	if err := batch.SaveJob(ctx, job); err != nil {
		return fmt.Errorf("failed to save completed job %d: %w", job.Key, err)
	}

	messageEndEventHandled := false
	activity, err := engine.getExecutionTokenActivity(ctx, instance, job.Token)
	if err != nil {
		return fmt.Errorf("failed to get execution token activity: %w", err)
	}
	switch element := activity.Element().(type) {
	case *bpmn20.TEndEvent:
		tokens, err = engine.handleExternalEndEventContinuation(ctx, &batch, instance, element, job.Token, tokens)
		if err != nil {
			return fmt.Errorf("failed to handle message end event continuation %w", err)
		}
		messageEndEventHandled = true
	}

	for _, token := range tokens {
		if err := batch.SaveToken(ctx, token); err != nil {
			return fmt.Errorf("failed to save token %d: %w", token.Key, err)
		}
	}
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	if instance.ProcessInstance().State == runtime.ActivityStateCompleted && instance.Type() != runtime.ProcessTypeDefault {
		err := engine.handleParentProcessContinuation(ctx, &batch, instance, task)
		if err != nil {
			return fmt.Errorf("failed to handle parent process continuation for job %+v: %w", job, err)
		}

		err = batch.Flush(ctx)
		if err != nil {
			return fmt.Errorf("failed to complete job %+v: %w", job, err)
		}

		engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))
		engine.recordJobLifetime(ctx, job, "completed")
		return nil
	}

	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}

	engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))
	engine.recordJobLifetime(ctx, job, "completed")

	if !messageEndEventHandled {
		// The job completion has already been durably flushed above. A successfully persisted
		// incident is a domain outcome and must not invite a retry of the completed job, while a
		// technical continuation failure must remain observable so recovery can be triggered.
		outcome, runErr := engine.continueProcessInstanceAfterCommit(ctx, instance.ProcessInstance().Key)
		if runErr != nil {
			if !outcome.isPersistedIncidentOnly() {
				return fmt.Errorf("failed to continue process instance %d after completing job %d: %w",
					instance.ProcessInstance().Key, job.Key, runErr)
			}
			engine.logger.Warn("failed to run process instance after completing job",
				"job", job.Key, "processInstance", instance.ProcessInstance().Key, "err", runErr)
		}
	}
	return nil
}

// repeatedCompletion answers a completion of a job completed before: it is as
// done as the first one. A duplicate completion can heal any stranded Running
// sibling, not only the completed job's token. The token embedded in the job
// can be a stale snapshot, so persisted tokens are checked before the instance
// lock is taken; the continuation reloads them under the lock if there is
// Running work.
func (engine *Engine) repeatedCompletion(ctx context.Context, job runtime.Job) error {
	continuationCtx, cancelContinuation := engine.continuationContext(ctx)
	activeTokens, readErr := engine.persistence.GetActiveTokensForProcessInstance(continuationCtx, job.ProcessInstanceKey)
	cancelContinuation()
	if readErr != nil {
		engine.wakeReconciliation(job.ProcessInstanceKey)
		return fmt.Errorf("failed to check running tokens for process instance %d after retrying completed job %d: %w",
			job.ProcessInstanceKey, job.Key, readErr)
	}
	needsContinuation := false
	for _, token := range activeTokens {
		if token.State == runtime.TokenStateRunning {
			needsContinuation = true
			break
		}
	}
	if !needsContinuation {
		return nil
	}
	engine.logger.Debug("job is already completed; checking whether its process instance needs to continue", "job", job.Key, "processInstance", job.ProcessInstanceKey)
	outcome, runErr := engine.continueProcessInstanceAfterCommit(ctx, job.ProcessInstanceKey)
	if runErr != nil {
		if !outcome.isPersistedIncidentOnly() {
			return fmt.Errorf("failed to continue process instance %d after retrying completed job %d: %w",
				job.ProcessInstanceKey, job.Key, runErr)
		}
		engine.logger.Warn("failed to continue process instance for an already completed job",
			"job", job.Key, "processInstance", job.ProcessInstanceKey, "err", runErr)
	}
	return nil
}

// completionRacedTheEndOfTheInstance classifies a completion which read an
// active job and then found its instance ended: a repeated completion whose
// first report ended the instance is as done as any duplicate, everything else
// is a conflict.
func (engine *Engine) completionRacedTheEndOfTheInstance(ctx context.Context, jobKey int64, batchErr error) error {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to refresh job with key %d after its instance ended: %w", jobKey, err)
	}
	if job.State == runtime.ActivityStateCompleted {
		engine.logger.Debug("job is already completed and its process instance ended", "job", job.Key, "processInstance", job.ProcessInstanceKey)
		return nil
	}
	return fmt.Errorf("%w: cannot complete job %d: %w", ErrJobInTerminalState, jobKey, batchErr)
}

// failureRacedTheEndOfTheInstance classifies a failure naming its attempt which
// read an active job and then found its instance ended: the repeat of a
// failure the job recorded before the end is answered as recorded, as it is
// when it arrives after the end, everything else is a conflict.
func (engine *Engine) failureRacedTheEndOfTheInstance(ctx context.Context, jobKey int64, attempt int32, batchErr error) error {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to refresh job with key %d after its instance ended: %w", jobKey, err)
	}
	if failureAlreadyRecorded(job, attempt) {
		engine.logger.Debug("failure of an attempt the job recorded before its process instance ended, nothing changes",
			"job", job.Key, "attempt", attempt, "attempts", job.Attempts, "processInstance", job.ProcessInstanceKey)
		return nil
	}
	return batchErr
}

// lockInstanceOfJob opens a batch on the instance of a job read before, which
// holds the instance, and reads the job again under it: nothing changes the job
// from then on, so a copy read before a failure or a retry update committed is
// never saved over it. ensure refuses a job the request does not suit, before
// the batch as well, since the instance of a job which ended may have ended
// itself and a batch cannot be opened for it. action names the request in the
// error of a job which ended with its instance meanwhile.
func (engine *Engine) lockInstanceOfJob(
	ctx context.Context,
	job runtime.Job,
	action string,
	ensure func(runtime.Job) error,
) (runtime.Job, runtime.ProcessInstance, EngineBatch, error) {
	if err := ensure(job); err != nil {
		return runtime.Job{}, nil, EngineBatch{}, err
	}
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return runtime.Job{}, nil, EngineBatch{}, newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey)
	}
	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		if errors.Is(err, ErrInstanceAlreadyTerminal) {
			// the job ended with its instance between the check above and the lock
			return runtime.Job{}, nil, EngineBatch{}, fmt.Errorf("%w: cannot %s job %d: %w", ErrJobInTerminalState, action, job.Key, err)
		}
		return runtime.Job{}, nil, EngineBatch{}, fmt.Errorf("failed to create engine batch for job %d: %w", job.Key, err)
	}
	refreshed, err := engine.persistence.FindJobByJobKey(ctx, job.Key)
	if err != nil {
		err = fmt.Errorf("failed to find job with key %d: %w", job.Key, err)
	} else {
		err = ensure(refreshed)
	}
	if err != nil {
		batch.Clear(ctx)
		return runtime.Job{}, nil, EngineBatch{}, err
	}
	return refreshed, instance, batch, nil
}

// failureAlreadyRecorded reports whether a failure without an error code
// names an attempt whose failure the job recorded already.
func failureAlreadyRecorded(job runtime.Job, attempt int32) bool {
	return attempt <= job.Attempts
}

// ensureJobStillWaits refuses a job which is completed, terminated or failed
// with an error wrapping ErrJobInTerminalState: with retries a repeated
// failure is an expected event, a conflict rather than a technical failure.
func ensureJobStillWaits(job runtime.Job) error {
	var ended string
	switch job.State {
	case runtime.ActivityStateCompleted:
		ended = "completed"
	case runtime.ActivityStateTerminated:
		ended = "terminated"
	case runtime.ActivityStateFailed:
		ended = "failed"
	default:
		return nil
	}
	return fmt.Errorf("%w: job %d is already %s", ErrJobInTerminalState, job.Key, ended)
}

// recordJobLifetime records the time between job creation and its terminal state, in milliseconds.
func (engine *Engine) recordJobLifetime(ctx context.Context, job runtime.Job, outcome string) {
	if engine.metrics == nil || engine.metrics.JobLifetime == nil || job.CreatedAt.IsZero() {
		return
	}
	engine.metrics.JobLifetime.Record(ctx, float64(time.Since(job.CreatedAt))/float64(time.Millisecond), metric.WithAttributes(
		attribute.String("type", job.Type),
		attribute.String("outcome", outcome),
	))
}
