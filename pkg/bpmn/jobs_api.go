package bpmn

import (
	"context"
	"errors"
	"fmt"

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
func (engine *Engine) JobAssignByKey(ctx context.Context, jobKey int64, assignee *string) error {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}
	job.Assignee = assignee
	if err := engine.persistence.SaveJob(ctx, job); err != nil {
		return newEngineErrorf("failed to save job assignee for key: %d", jobKey)
	}
	return nil
}

// JobFailByKey is used to mark external jobs as failed
func (engine *Engine) JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]interface{}) (retErr error) {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return newEngineErrorf("failed to find job with key: %d, err: %s", jobKey, err)
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

	if job.State == runtime.ActivityStateFailed {
		return newEngineErrorf("job %d is already failed", job.Key)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey)
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
	job, err = engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}
	switch job.State {
	case runtime.ActivityStateCompleted:
		return newEngineErrorf("job already completed: %d", job.Key)
	case runtime.ActivityStateTerminated:
		return newEngineErrorf("job already terminated: %d", job.Key)
	case runtime.ActivityStateFailed:
		return newEngineErrorf("job already failed: %d", job.Key)
	default:
		// do nothing
	}

	failJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)

	defer func() {
		if retErr == nil {
			engine.metrics.JobsFailed.Add(ctx, 1, metric.WithAttributes(
				attribute.String("type", job.Type),
				attribute.Bool("internal", false),
			))
		}
	}()

	if errorCode != nil {
		boundaryMatch, err := engine.findMatchingBoundaryErrorEvent(ctx, instance, job.Token, errorCode)
		if err != nil {
			return err
		}

		if boundaryMatch != nil {
			if handled, err := engine.processBoundaryErrorEvent(ctx, &batch, job, instance, boundaryMatch, variables); err != nil {
				return err
			} else if handled {
				return nil
			}
		}
	}

	err = engine.failJobWithIncident(ctx, &batch, job, instance, message, errorCode)
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

	boundaryInstance, tokens, err := engine.handleBoundaryError(ctx, batch, instance, boundaryMatch)
	if err != nil {
		return false, err
	}

	variableHolder := runtime.NewVariableHolder(&boundaryInstance.ProcessInstance().VariableHolder, nil)
	_, err = variableHolder.PropagateOutputVariablesToParent(boundaryMatch.event.GetOutputMapping(), variables, engine.evaluateExpression)
	if err != nil {
		return false, fmt.Errorf("failed to propagate boundary error variables to process instance %d: %w", boundaryInstance.ProcessInstance().Key, err)
	}
	if err := batch.SaveProcessInstance(ctx, boundaryInstance); err != nil {
		return false, fmt.Errorf("failed to save process instance %d after boundary error handling: %w", boundaryInstance.ProcessInstance().Key, err)
	}

	for _, token := range tokens {
		err = batch.SaveToken(ctx, token)
		if err != nil {
			return false, err
		}
	}

	if err := batch.Flush(ctx); err != nil {
		return false, fmt.Errorf("failed to fail job %+v by boundary error handling: %w", job, err)
	}

	return true, engine.RunProcessInstance(ctx, boundaryInstance, tokens)
}

func (engine *Engine) failJobWithIncident(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	instance runtime.ProcessInstance,
	message string, errorCode *string,
) error {

	job.State = runtime.ActivityStateFailed
	err := batch.SaveJob(ctx, job)
	if err != nil {
		return err
	}

	job.Token.State = runtime.TokenStateFailed
	err = batch.SaveToken(ctx, job.Token)
	if err != nil {
		return err
	}

	instance.ProcessInstance().State = runtime.ActivityStateFailed
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	code := ptr.Deref(errorCode, "")
	err = batch.SaveIncident(ctx, createNewIncidentFromToken(fmt.Errorf("%s: %s", message, code), job.Token, engine))

	if err != nil {
		return err
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
		variableHolder := runtime.NewVariableHolder(&processInstance.ProcessInstance().VariableHolder, nil)

		task := processInstance.ProcessInstance().Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
		if task == nil {
			return nil, errors.Join(newEngineErrorf("failed to find task element for job: %+v", job), err)
		}
		if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(task.GetInputMapping(), engine.evaluateExpression, nil); err != nil {
			job.State = runtime.ActivityStateFailed
			perr := engine.persistence.SaveJob(ctx, job)
			if perr != nil {
				return nil, errors.Join(fmt.Errorf("failed to save failed job"), err, perr)
			}
			return nil, fmt.Errorf("failed to evaluate variables: %w", err)
		}
		aj := &activatedJob{
			processInstanceInfo: processInstance,
			key:                 job.Key,
			processInstanceKey:  job.ProcessInstanceKey,
			elementId:           job.ElementId,
			createdAt:           job.CreatedAt,
			localVariables:      variableHolder.LocalVariables(),
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
		engine.logger.Error("job %d is already completed", job.Key)
		return nil
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
		return newEngineErrorf("failed to create engine batch")
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	//refresh token
	job, err = engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return newEngineErrorf("failed to find job with key: %d", jobKey)
	}
	switch job.State {
	case runtime.ActivityStateCompleted:
		return newEngineErrorf("job already completed: %d", job.Key)
	case runtime.ActivityStateTerminated:
		return newEngineErrorf("job already terminated: %d", job.Key)
	case runtime.ActivityStateFailed:
		return newEngineErrorf("job already failed: %d", job.Key)
	default:
		// do nothing
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	task := instance.ProcessInstance().Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
	if task == nil {
		return errors.Join(newEngineErrorf("failed to find task element for job: %+v", job))
	}
	outputVariables, err := variableHolder.PropagateOutputVariablesToParent(task.GetOutputMapping(), variables, engine.evaluateExpression)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to map output variables for job: %+v", job))
	}
	err = batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             job.Token.ElementInstanceKey,
		OutputVariables: outputVariables,
	})
	if err != nil {
		return err
	}

	err = engine.cancelBoundarySubscriptions(ctx, &batch, instance.ProcessInstance().Key, &job.Token)
	if err != nil {
		return fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.handleElementTransition(ctx, &batch, instance, task, job.Token)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}

	job.State = runtime.ActivityStateCompleted
	batch.SaveJob(ctx, job)

	messageEndEventHandled := false
	activity, err := engine.getExecutionTokenActivity(ctx, instance, job.Token)
	switch element := activity.Element().(type) {
	case *bpmn20.TEndEvent:
		tokens, err = engine.handleExternalEndEventContinuation(ctx, instance, element, job.Token, tokens)
		if err != nil {
			return fmt.Errorf("failed to handle message end event continuation %w", err)
		}
		messageEndEventHandled = true
	}

	for _, token := range tokens {
		batch.SaveToken(ctx, token)
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
		return nil
	} else {
		err = batch.Flush(ctx)
		if err != nil {
			return fmt.Errorf("failed to complete job %+v: %w", job, err)
		}

		engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))

		if !messageEndEventHandled {
			err := engine.RunProcessInstance(ctx, instance, tokens)
			if err != nil {
				return fmt.Errorf("failed to run process instance %d: %w", instance.ProcessInstance().Key, err)
			}
		}
		return err
	}
}
