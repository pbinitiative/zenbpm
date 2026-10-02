package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// ErrIncidentNotResolvable is wrapped into the error of a resolution which the
// state of the process instance does not allow as it stands; the message names
// what to change before the resolution is repeated.
var ErrIncidentNotResolvable = errors.New("incident cannot be resolved as things stand")

// ErrIncidentAlreadyResolved is wrapped into the error of a resolution of an
// incident which is resolved already, by another request or by an earlier
// attempt of the same one whose answer was lost. Nothing is changed, so
// retries given with the resolution are not applied either.
var ErrIncidentAlreadyResolved = errors.New("incident already resolved")

// ResolveIncidentOption is an optional argument of ResolveIncident.
type ResolveIncidentOption func(*incidentResolution)

type incidentResolution struct {
	jobRetries *operatorRetries
}

// operatorRetries are the retries an operator sets for a job, and when it is
// handed out next; a nil retryAt means at once.
type operatorRetries struct {
	retries int32
	retryAt *time.Time
}

// WithJobRetries sets the retries of the job the incident leaves waiting, and
// when it is handed out next, in the same transaction that resolves the
// incident: either both happen or neither does. The values are checked as
// UpdateJobRetries checks them. A job whose own incident is resolved starts
// its new series with them instead of evaluating the retries of the task
// definition, so the resolution succeeds even where those no longer evaluate;
// a job whose incident it did not raise gets them as UpdateJobRetries sets
// them, and a job UpdateJobRetries would refuse with ErrJobInTerminalState
// refuses them the same way. An incident which leaves no job waiting refuses
// them with ErrInvalidJobRequest.
func WithJobRetries(retries int32, retryAt *time.Time) ResolveIncidentOption {
	return func(resolution *incidentResolution) {
		resolution.jobRetries = &operatorRetries{retries: retries, retryAt: retryAt}
	}
}

func createNewIncidentFromToken(err error, token runtime.ExecutionToken, engine *Engine) runtime.Incident {
	return runtime.Incident{
		Key:                engine.generateKey(),
		ElementInstanceKey: token.ElementInstanceKey,
		ElementId:          token.ElementId,
		ProcessInstanceKey: token.ProcessInstanceKey,
		Type:               incidentTypeFromError(err),
		Message:            err.Error(),
		CreatedAt:          time.Now(),
		Token:              token,
		ResolvedAt:         nil,
	}
}

func incidentTypeFromError(err error) runtime.IncidentType {
	if errors.Is(err, ErrMaxProcessInstanceFlowNodeCountExceeded) {
		return runtime.IncidentTypeMaxProcessInstanceFlowNodeCountExceeded
	}
	return runtime.IncidentTypeUnspecified
}

func (engine *Engine) retryEventSubprocessSubscriptionIncident(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, incident runtime.Incident) error {
	processDefinition := instance.ProcessInstance().Definition
	if processDefinition == nil {
		return fmt.Errorf("process instance %d has no process definition", instance.ProcessInstance().Key)
	}

	subProcess, startEvent := processDefinition.Definitions.Process.GetSubprocessAndStartEventById(incident.ElementId)
	if subProcess == nil || startEvent == nil {
		return fmt.Errorf("failed to find event subprocess start event %s in process definition %d", incident.ElementId, processDefinition.Key)
	}

	return engine.createStartEventSubscriptions(ctx, batch, subProcess.TProcess, *processDefinition, &instance)
}

func (engine *Engine) reevaluateJobInputVariables(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, job *runtime.Job) error {
	element := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(job.ElementId)
	task, ok := element.(bpmn20.InternalTask)
	if !ok {
		return fmt.Errorf("failed to find task %s for job %d", job.ElementId, job.Key)
	}

	flowElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, job.ElementInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find flow element instance %d for job %d: %w", job.ElementInstanceKey, job.Key, err)
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if activity, ok := element.(bpmn20.Activity); ok && activity.GetMultiInstance() != nil {
		inputElementName := activity.GetMultiInstance().LoopCharacteristics.InputElementName
		inputElement, ok := flowElementInstance.InputVariables[inputElementName]
		if !ok {
			return fmt.Errorf("failed to find multi-instance input variable %s for job %d", inputElementName, job.Key)
		}
		variableHolder.SetLocalVariable(inputElementName, inputElement)
	}

	flowElementInput := variableHolder.ExecutionScopeSnapshot()
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(task.GetInputMapping(), engine.evaluateExpression); err != nil {
		return fmt.Errorf("failed to evaluate input variables for job %d: %w", job.Key, err)
	}
	job.InputVariables = variableHolder.LocalVariables()

	flowElementInstance.InputVariables = flowElementInput
	if err := batch.SaveFlowElementInstance(ctx, flowElementInstance); err != nil {
		return fmt.Errorf("failed to update input variables for flow element instance %d: %w", flowElementInstance.Key, err)
	}
	return nil
}

// restartJobRetries starts a fresh series of attempts for a job whose own
// incident is resolved: no attempts, no backoff, and the retries of the task
// definition re-evaluated, whatever the incident was about. Retries an operator
// set since the last series ended, through UpdateJobRetries or with the
// resolution itself (WithJobRetries), are kept instead, and so is the moment
// the operator chose for the next delivery while it lies ahead; the series
// which followed an update did not exhaust them, or the update would have
// been forgotten with the incident. Retries which no longer
// evaluate refuse the resolution with ErrIncidentNotResolvable: the job would
// otherwise wait without the retries its definition asks for.
func (engine *Engine) restartJobRetries(instance runtime.ProcessInstance, incidentKey int64, job *runtime.Job) error {
	job.Attempts = 0
	if job.RetriesSetByOperator {
		job.RetriesSetByOperator = false
		if !job.IsWaitingOutBackoff(time.Now()) {
			job.RetryAt = nil
		}
		return nil
	}
	job.RetryAt = nil
	task := instance.ProcessInstance().Definition.Definitions.Process.GetInternalTaskById(job.ElementId)
	if task == nil {
		return fmt.Errorf("failed to find task %s for job %d", job.ElementId, job.Key)
	}
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	variableHolder.SetLocalVariables(job.InputVariables)
	retries, err := engine.initialRetries(task, variableHolder.ExecutionScopeSnapshot())
	if err != nil {
		return fmt.Errorf("%w: the retries of job %d no longer evaluate (%w); correct the variables they read "+
			"and resolve the incident again, or resolve it with the job's retries given "+
			"(\"retries\" in the body of POST /v1/incidents/%d/resolve)",
			ErrIncidentNotResolvable, job.Key, err, incidentKey)
	}
	job.Retries = retries
	return nil
}

// ResolveIncident resolves an incident and continues the token it failed. The
// job the incident leaves waiting, if any, is handed out again; one whose own
// incident it is starts a fresh series of attempts (see restartJobRetries).
// WithJobRetries gives that job retries of the operator's own at the same time.
func (engine *Engine) ResolveIncident(ctx context.Context, key int64, options ...ResolveIncidentOption) (retErr error) {
	var resolution incidentResolution
	for _, option := range options {
		option(&resolution)
	}

	ctx, resoveIncidentSpan := engine.tracer.Start(ctx, fmt.Sprintf("incident:%d", key))
	defer func() {
		if retErr != nil {
			resoveIncidentSpan.RecordError(retErr)
			resoveIncidentSpan.SetStatus(codes.Error, retErr.Error())
		}
		resoveIncidentSpan.End()
	}()

	incident, err := engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find incident with key %d", key), err)
	}

	resoveIncidentSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeIncidentKey, incident.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, incident.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, incident.Token.Key),
	)
	if retries := resolution.jobRetries; retries != nil {
		// a trace shows that an operator replaced the retries of the task definition
		resoveIncidentSpan.SetAttributes(attribute.Int(otelPkg.AttributeJobRetries, int(retries.retries)))
	}

	if incident.ResolvedAt != nil {
		return incidentAlreadyResolved(incident)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find process instance with key %d", incident.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return newEngineErrorf("failed to create engine batch")
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	//refresh
	incident, err = engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find incident with key %d", key), err)
	}
	if incident.ResolvedAt != nil {
		return incidentAlreadyResolved(incident)
	}

	if incident.Token.Key == 0 {
		if resolution.jobRetries != nil {
			return incidentWithoutJobRefusesRetries(key)
		}
		if err := engine.retryEventSubprocessSubscriptionIncident(ctx, &batch, instance, incident); err != nil {
			return fmt.Errorf("failed to recreate event subprocess subscription for incident %d: %w", key, err)
		}
		incident.ResolvedAt = new(time.Now())
		if err := batch.SaveIncident(ctx, incident); err != nil {
			return fmt.Errorf("failed to save resolved incident %d: %w", incident.Key, err)
		}
		if err := batch.Flush(ctx); err != nil {
			return newEngineErrorf("failed to complete incident with key: %d", key)
		}
		return nil
	}

	// FindIncidentByKey keeps dangling token-bound incidents readable for history/listing purposes,
	// but resolving such an incident requires the current persisted token.
	incident.Token, err = engine.persistence.GetTokenByKey(ctx, incident.Token.Key)
	if err != nil {
		return fmt.Errorf("failed to find execution token %d for incident %d: %w", incident.Token.Key, key, err)
	}
	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find jobs for token key: %d", incident.Token.Key)
	}
	// Checking for linked jobs as these need to be resolved as well
	job, err := jobOfIncident(jobs, incident)
	if err != nil {
		return err
	}
	if job != nil {
		resoveIncidentSpan.SetAttributes(attribute.Int64(otelPkg.AttributeJobKey, job.Key))
	}
	if retries := resolution.jobRetries; retries != nil {
		if job == nil {
			return incidentWithoutJobRefusesRetries(key)
		}
		if err := engine.checkOperatorRetries(job.Key, retries.retries, retries.retryAt); err != nil {
			return err
		}
		if err := ensureJobRetriesAreUpdatable(*job); err != nil {
			return err
		}
		// a job which did not raise the incident keeps them as UpdateJobRetries
		// leaves them; the restart of a failed job below keeps them for its new series
		setOperatorRetries(job, retries.retries, retries.retryAt, time.Now())
	}

	incident.ResolvedAt = new(time.Now())
	err = batch.SaveIncident(ctx, incident)
	if err != nil {
		return err
	}

	// A token blocked by the flow node count guard gets a fresh execution budget: the operator
	// resolved the incident after fixing the loop's exit condition, so the instance-wide counter
	// is reset to zero. Other incidents must not alter the flow node counter.
	if incident.Type == runtime.IncidentTypeMaxProcessInstanceFlowNodeCountExceeded {
		if err := batch.ResetProcessInstanceFlowNodeCount(ctx, incident.ProcessInstanceKey); err != nil {
			return fmt.Errorf("failed to reset flow node count of process instance %d: %w", incident.ProcessInstanceKey, err)
		}
	}

	// TODO: the same thing has to happen for other waiting subscriptions
	if job != nil {
		if err := engine.reevaluateJobInputVariables(ctx, &batch, instance, job); err != nil {
			return err
		}
		// an incident the job did not raise itself, such as a boundary event
		// which failed to correlate, leaves the job's series and backoff alone
		if job.State == runtime.ActivityStateFailed {
			if err := engine.restartJobRetries(instance, incident.Key, job); err != nil {
				return err
			}
		}
		incident.Token.State = runtime.TokenStateWaiting
		job.State = runtime.ActivityStateActive
		err := batch.SaveJob(ctx, *job)
		if err != nil {
			return err
		}
	} else {
		incident.Token.State = runtime.TokenStateRunning
	}
	err = batch.SaveToken(ctx, incident.Token)
	if err != nil {
		return err
	}

	instance.ProcessInstance().State = runtime.ActivityStateActive
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return newEngineErrorf("failed to complete incident with key: %d", key)
	}

	// Reload every persisted Running token under the instance lock. This includes sibling branches
	// that may have been stranded while the instance was Failed and avoids executing a stale snapshot.
	outcome, runErr := engine.continueProcessInstanceAfterCommit(ctx, instance.ProcessInstance().Key)
	if runErr != nil {
		if !outcome.isPersistedFlowNodeCountReplacement(incident.Type) {
			return fmt.Errorf("failed to continue process instance %d after resolving incident %d: %w",
				instance.ProcessInstance().Key, incident.Key, runErr)
		}
		engine.logger.Warn("process instance raised a replacement incident after incident resolution",
			"incident", incident.Key, "processInstance", instance.ProcessInstance().Key, "err", runErr)
	}
	return nil
}

// incidentAlreadyResolved is the refusal of a resolution of an incident which
// is resolved already.
func incidentAlreadyResolved(incident runtime.Incident) error {
	return fmt.Errorf("%w: incident %d was resolved at %s; nothing was changed",
		ErrIncidentAlreadyResolved, incident.Key, incident.ResolvedAt.Format(time.RFC3339))
}

// incidentWithoutJobRefusesRetries is the refusal of retries given with the
// resolution of an incident which leaves no job waiting, such as one of an
// expression: silently dropping them would hide the requester's mistake.
func incidentWithoutJobRefusesRetries(incidentKey int64) error {
	return invalidJobRequestf("incident %d leaves no job waiting; retries can only be given with the resolution of the incident of a job",
		incidentKey)
}

// jobOfIncident is the pending job the resolution of an incident leaves
// waiting: the job the incident names, or, for an incident which names none,
// the job on its token, if any. Several jobs may share a token, so a job the
// incident names is never guessed from the token. A named job which does not
// wait, or waits on another token, is refused: resolving the incident without
// it would continue the token past a job nobody hands out again.
func jobOfIncident(jobs []runtime.Job, incident runtime.Incident) (*runtime.Job, error) {
	if incident.JobKey == nil {
		for i := range jobs {
			if jobs[i].Token.Key == incident.Token.Key {
				return &jobs[i], nil
			}
		}
		return nil, nil
	}
	for i := range jobs {
		if jobs[i].Key != *incident.JobKey {
			continue
		}
		if jobs[i].Token.Key != incident.Token.Key {
			return nil, fmt.Errorf("incident %d names job %d, which waits on token %d instead of the incident's token %d",
				incident.Key, jobs[i].Key, jobs[i].Token.Key, incident.Token.Key)
		}
		return &jobs[i], nil
	}
	return nil, fmt.Errorf("incident %d names job %d, which is not a pending job of process instance %d",
		incident.Key, *incident.JobKey, incident.ProcessInstanceKey)
}

func (engine *Engine) resolveIncidentsForToken(ctx context.Context, batch *EngineBatch, tokenKey int64) error {

	incidents, err := engine.persistence.FindIncidentsByExecutionTokenKey(ctx, tokenKey)
	if err != nil {
		return fmt.Errorf("failed to find incidents for execution token %d: %w", tokenKey, err)
	}

	for _, incident := range incidents {
		if incident.ResolvedAt != nil {
			continue
		}

		incident.ResolvedAt = new(time.Now())
		err = batch.SaveIncident(ctx, incident)
		if err != nil {
			return fmt.Errorf("failed to save changes to incident %d: %w", incident.Key, err)
		}
	}

	return nil
}
