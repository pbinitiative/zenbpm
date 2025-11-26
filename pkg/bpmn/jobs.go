package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) createInternalTask(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element bpmn20.InternalTask, currentToken runtime.ExecutionToken) (state runtime.ActivityState, retErr error) {
	jobVarHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := jobVarHolder.EvaluateAndSetInputMappings(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate input variables: %w", err)
	}
	job := runtime.Job{
		ElementId:          currentToken.ElementId,
		ElementInstanceKey: currentToken.ElementInstanceKey,
		ProcessInstanceKey: currentToken.ProcessInstanceKey,
		Key:                engine.generateKey(),
		Type:               element.GetTaskType(),
		State:              runtime.ActivityStateActive,
		Variables:          jobVarHolder.LocalVariables(),
		CreatedAt:          time.Now(),
		Token:              currentToken,
	}
	err := batch.SaveJob(ctx, job)
	if err != nil {
		job.State = runtime.ActivityStateFailed
		return job.State, fmt.Errorf("failed to create job: %w", err)
	}

	handler := engine.findTaskHandler(element)
	if handler == nil {
		engine.metrics.JobsCreated.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", false)))
		return job.State, nil
	}
	engine.metrics.JobsCreated.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
	// if we have the handler handle the task directly
	// TODO: pull this out into function that will be called by API as well
	if job.State != runtime.ActivityStateCompleting {
		job.State = runtime.ActivityStateActive
		var failReason string = ""
		activatedJob := &activatedJob{
			processInstanceInfo: instance,
			failHandler: func(reason string) {
				job.State = runtime.ActivityStateFailing
				failReason = reason
			},
			completeHandler:          func() { job.State = runtime.ActivityStateCompleting },
			key:                      engine.generateKey(),
			processInstanceKey:       instance.Key,
			bpmnProcessId:            instance.Definition.BpmnProcessId,
			processDefinitionVersion: instance.Definition.Version,
			processDefinitionKey:     instance.Definition.Key,
			elementId:                job.ElementId,
			createdAt:                job.CreatedAt,
			variableHolder:           jobVarHolder,
		}
		ctx, internalCompleteSpan := engine.tracer.Start(ctx, fmt.Sprintf("job:%s", activatedJob.ElementId()), trace.WithAttributes(
			attribute.Int64("key", activatedJob.key),
		))
		defer func() {
			if retErr != nil {
				internalCompleteSpan.RecordError(retErr)
				internalCompleteSpan.SetStatus(codes.Error, retErr.Error())
			}
			internalCompleteSpan.End()
		}()
		handler(activatedJob)
		switch job.State {
		case runtime.ActivityStateFailing:
			engine.handleIncident(ctx, job.Token, newEngineErrorf("failing internal job with message: %s", failReason), internalCompleteSpan)
			job.State = runtime.ActivityStateFailed
			instance.State = runtime.ActivityStateFailed

		case runtime.ActivityStateCompleting:
			if err = jobVarHolder.PropagateLocalVariables(element.GetOutputMapping(), engine.evaluateExpression); err != nil {
				engine.handleIncident(ctx, job.Token, newEngineErrorf("failing internal job with message: %s", err), internalCompleteSpan)
				job.State = runtime.ActivityStateFailed
				instance.State = runtime.ActivityStateFailed
			} else {
				job.State = runtime.ActivityStateCompleted
			}
		}
		batch.SaveJob(ctx, job)
		err = batch.Flush(ctx)
		if err != nil {
			return runtime.ActivityStateFailed, fmt.Errorf("failed to add save job into batch: %w", err)
		}
		engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
	}
	return job.State, nil
}

func (engine *Engine) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]interface{}) error {
	instance, tokens, err := engine.completeJob(ctx, jobKey, variables)
	if err != nil {
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}

	err = engine.runProcessInstance(ctx, instance, tokens)
	if err != nil {
		return fmt.Errorf("failed to run process instance %d: %w", instance.Key, err)
	}
	return nil
}

func (engine *Engine) completeJob(
	ctx context.Context,
	jobKey int64,
	variables map[string]interface{},
) (
	instance *runtime.ProcessInstance,
	tokens []runtime.ExecutionToken,
	retErr error,
) {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find job with key: %d", jobKey), err)
	}
	ctx, completeJobSpan := engine.tracer.Start(ctx, fmt.Sprintf("job:%s", job.Type), trace.WithAttributes(
		attribute.Int64("key", job.Key),
	))
	defer func() {
		if retErr != nil {
			completeJobSpan.RecordError(retErr)
			completeJobSpan.SetStatus(codes.Error, retErr.Error())
		}
		completeJobSpan.End()
	}()
	if job.State == runtime.ActivityStateCompleted {
		return nil, nil, newEngineErrorf("job %d is already completed", jobKey)
	}
	completeJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)

	inst, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey), err)
	}
	instance = &inst

	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, variables)
	task := instance.Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
	if task == nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find task element for job: %+v", job), err)
	}

	if err = variableHolder.PropagateLocalVariables(task.GetOutputMapping(), engine.evaluateExpression); err != nil {
		job.State = runtime.ActivityStateFailed
		instance.State = runtime.ActivityStateFailed
	}
	// TODO: variable mapping needs to be implemented
	job.State = runtime.ActivityStateCompleted
	batch := engine.persistence.NewBatch()
	batch.SaveJob(ctx, job)
	batch.SaveProcessInstance(ctx, *instance)

	err = engine.cancelBoundarySubscriptions(ctx, batch, instance, &job.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", instance.Key, err)
	}

	currentToken := job.Token

	tokens, err = engine.handleSimpleTransition(ctx, batch, instance, task, currentToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to complete job %+v: %w", job, err)
	}
	for _, token := range tokens {
		batch.SaveToken(ctx, token)
	}
	err = batch.Flush(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to complete job %+v: %w", job, err)
	}
	engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))
	return instance, tokens, nil
}

// JobFailByKey is used to mark external jobs as failed
func (engine *Engine) JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]interface{}) error {
	instance, tokens, err := engine.failJob(ctx, jobKey, message, errorCode, variables)
	if err != nil {
		return fmt.Errorf("failed to fail job %d: %w", jobKey, err)
	}

	if len(tokens) > 0 {
		err = engine.runProcessInstance(ctx, instance, tokens)
		if err != nil {
			return fmt.Errorf("failed to run process instance %d: %w", instance.Key, err)
		}
		return nil
	}
	return nil
}

func (engine *Engine) failJob(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]interface{}) (
	instance *runtime.ProcessInstance,
	tokens []runtime.ExecutionToken,
	retErr error,
) {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find job with key: %d", jobKey), err)
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
		return nil, nil, newEngineErrorf("job %d is already failed", jobKey)
	}
	failJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)

	inst, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return nil, nil, errors.Join(newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey), err)
	}
	instance = &inst

	engine.handleIncident(ctx, job.Token, newEngineErrorf("failing job with message: %s, error code: %s", message, ptr.Deref(errorCode, "n/a")), failJobSpan)

	// TODO: variable mapping needs to be implemented
	job.State = runtime.ActivityStateFailed
	instance.State = runtime.ActivityStateFailed
	batch := engine.persistence.NewBatch()
	batch.SaveJob(ctx, job)
	batch.SaveProcessInstance(ctx, *instance)

	err = batch.Flush(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fail job %+v: %w", job, err)
	}
	engine.metrics.JobsFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))

	if errorCode != nil {
		// TODO: we should check wheter the BPMN element has error boundary event on it and if it has map varaibles and follow its flow
		return instance, []runtime.ExecutionToken{}, nil
	} else {
		return instance, []runtime.ExecutionToken{}, nil
	}
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
		variableHolder := processInstance.VariableHolder

		task := processInstance.Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
		if task == nil {
			return nil, errors.Join(newEngineErrorf("failed to find task element for job: %+v", job), err)
		}
		if err := variableHolder.EvaluateAndSetInputMappings(task.GetInputMapping(), engine.evaluateExpression); err != nil {
			job.State = runtime.ActivityStateFailed
			perr := engine.persistence.SaveJob(ctx, job)
			if perr != nil {
				return nil, errors.Join(fmt.Errorf("failed to save failed job"), err, perr)
			}
			return nil, fmt.Errorf("failed to evaluate variables: %w", err)
		}
		aj := &activatedJob{
			processInstanceInfo: &processInstance,
			key:                 job.Key,
			processInstanceKey:  job.ProcessInstanceKey,
			elementId:           job.ElementId,
			createdAt:           job.CreatedAt,
			variableHolder:      variableHolder,
		}
		activatedJobs = append(activatedJobs, aj)
	}
	return activatedJobs, nil
}
