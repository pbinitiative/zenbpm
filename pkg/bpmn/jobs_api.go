package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// JobFailByKey is used to mark external jobs as failed
func (engine *Engine) JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]interface{}) (retErr error) {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find job with key: %d", jobKey), err)
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
		return errors.Join(newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to create engine batch"), err)
	}

	failJobSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeJobKey, job.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, job.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, job.Token.Key),
	)

	job.State = runtime.ActivityStateFailed
	job.Token.State = runtime.TokenStateFailed
	instance.ProcessInstance().State = runtime.ActivityStateFailed

	batch.SaveJob(ctx, job)
	batch.SaveToken(ctx, job.Token)
	batch.SaveProcessInstance(ctx, instance)
	batch.SaveIncident(ctx, createNewIncidentFromToken(fmt.Errorf("%s :"+message, errorCode), job.Token, engine))

	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to fail job %+v: %w", job, err)
	}
	engine.metrics.JobsFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", job.Type), attribute.Bool("internal", false)))

	if errorCode != nil {
		// TODO: we should check wheter the BPMN element has error boundary event on it and if it has map varaibles and follow its flow
		return nil
	}
	return nil
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
		if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(task.GetInputMapping(), engine.evaluateExpression); err != nil {
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
	j, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find job with key: %d", jobKey), err)
	}
	job := &j

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
		return errors.Join(newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to create engine batch"), err)
	}

	if job.State == runtime.ActivityStateCompleted {
		engine.logger.Error("job %d is already completed", job.Key)
		return nil
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
	batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             job.Token.ElementInstanceKey,
		OutputVariables: outputVariables,
	})

	tokens, err := engine.handleElementTransition(ctx, &batch, instance, task, job.Token)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}

	err = engine.cancelBoundarySubscriptions(ctx, &batch, instance, &job.Token)
	if err != nil {
		return fmt.Errorf("failed to cancel boundary subscriptions for process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	job.State = runtime.ActivityStateCompleted
	batch.SaveJob(ctx, *job)
	for _, token := range tokens {
		batch.SaveToken(ctx, token)
	}
	batch.SaveProcessInstance(ctx, instance)

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

		err := engine.RunProcessInstance(ctx, instance, tokens)
		if err != nil {
			engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", instance.ProcessInstance().Key, err.Error()))
		}
		return nil
	}
}
