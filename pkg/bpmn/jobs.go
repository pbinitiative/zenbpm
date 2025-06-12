package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) createInternalTask(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element bpmn20.InternalTask, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	job := runtime.Job{
		ElementId:          currentToken.ElementId,
		ElementInstanceKey: currentToken.ElementInstanceKey,
		ProcessInstanceKey: currentToken.ProcessInstanceKey,
		Key:                engine.generateKey(),
		Type:               element.GetTaskType(),
		State:              runtime.ActivityStateActive,
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
		return job.State, nil
	}
	// if we have the handler handle the task directly
	// TODO: pull this out into function that will be called by API as well
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if job.State != runtime.ActivityStateCompleting {
		job.State = runtime.ActivityStateActive
		activatedJob := &activatedJob{
			processInstanceInfo:      instance,
			failHandler:              func(reason string) { job.State = runtime.ActivityStateFailed },
			completeHandler:          func() { job.State = runtime.ActivityStateCompleting },
			key:                      engine.generateKey(),
			processInstanceKey:       instance.Key,
			bpmnProcessId:            instance.Definition.BpmnProcessId,
			processDefinitionVersion: instance.Definition.Version,
			processDefinitionKey:     instance.Definition.Key,
			elementId:                job.ElementId,
			createdAt:                job.CreatedAt,
			variableHolder:           variableHolder,
		}
		if err := evaluateLocalVariables(&variableHolder, element.GetInputMapping()); err != nil {
			job.State = runtime.ActivityStateFailed
			instance.State = runtime.ActivityStateFailed
			err := batch.SaveJob(ctx, job)
			if err != nil {
				engine.logger.Error("failed to save job", "job", job.Key, "err", err)
				return runtime.ActivityStateFailed, fmt.Errorf("failed to save job: %w", err)
			}
			return runtime.ActivityStateFailed, nil
		}
		handler(activatedJob)
		if job.State == runtime.ActivityStateCompleting {
			err = propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping())
			if err != nil {
				instance.State = runtime.ActivityStateFailed
				job.State = runtime.ActivityStateFailed
			} else {
				job.State = runtime.ActivityStateCompleted
			}
		}
		err = batch.SaveJob(ctx, job)
		err = batch.Flush(ctx)
		if err != nil {
			return runtime.ActivityStateFailed, fmt.Errorf("failed to add save job into batch: %w", err)
		}
	}
	return job.State, nil
}

func (engine *Engine) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]interface{}) error {
	job, err := engine.persistence.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find job with key: %d", jobKey), err)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, job.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find process instance with key: %d", job.ProcessInstanceKey), err)
	}

	variableHolder := runtime.NewVariableHolderForPropagation(&instance.VariableHolder, variables)
	task := instance.Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
	if task == nil {
		return errors.Join(newEngineErrorf("failed to find task element for job: %+v", job), err)
	}

	if err := propagateProcessInstanceVariables(&variableHolder, task.GetOutputMapping()); err != nil {
		job.State = runtime.ActivityStateFailed
		instance.State = runtime.ActivityStateFailed
	}
	// TODO: variable mapping needs to be implemented
	job.State = runtime.ActivityStateCompleted
	batch := engine.persistence.NewBatch()
	batch.SaveJob(ctx, job)
	batch.SaveProcessInstance(ctx, instance)

	currentToken := job.Token

	tokens, err := engine.handleSimpleTransition(ctx, &instance, task, currentToken)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}
	batch.SaveToken(ctx, currentToken)
	err = batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete job %+v: %w", job, err)
	}

	// TODO: make sure that process instance is not running and if so modify currently running instance
	engine.runProcessInstance(ctx, &instance, tokens)
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
		variableHolder := processInstance.VariableHolder

		task := processInstance.Definition.Definitions.Process.GetInternalTaskById(job.Token.ElementId)
		if task == nil {
			return nil, errors.Join(newEngineErrorf("failed to find task element for job: %+v", job), err)
		}
		if err := evaluateLocalVariables(&variableHolder, task.GetInputMapping()); err != nil {
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
