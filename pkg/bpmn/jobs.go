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

func (engine *Engine) handleServiceTask(ctx context.Context, batch storage.Batch, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TaskElement) (*runtime.Job, error) {
	job, err := findOrCreateJob(ctx, engine, batch, element, instance, engine.generateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find or create job: %w", err)
	}

	//FIXME: logic of using the internal handler needs to be discussed whether it will be kept
	// If kept needs to work in parallel with external job completion
	handler := engine.findTaskHandler(element)
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if handler != nil {
		if job.JobState != runtime.ActivityStateCompleting {
			job.JobState = runtime.ActivityStateActive
			activatedJob := &activatedJob{
				processInstanceInfo:      instance,
				failHandler:              func(reason string) { job.JobState = runtime.ActivityStateFailed },
				completeHandler:          func() { job.JobState = runtime.ActivityStateCompleting },
				key:                      engine.generateKey(),
				processInstanceKey:       instance.Key,
				bpmnProcessId:            process.BpmnProcessId,
				processDefinitionVersion: process.Version,
				processDefinitionKey:     process.ProcessKey,
				elementId:                job.ElementId,
				createdAt:                job.CreatedAt,
				variableHolder:           variableHolder,
			}
			if err := evaluateLocalVariables(&variableHolder, element.GetInputMapping()); err != nil {
				job.JobState = runtime.ActivityStateFailed
				instance.State = runtime.ActivityStateFailed
				batch.SaveJob(ctx, *job)
				return job, nil
			}
			handler(activatedJob)
		}
	}

	if job.JobState == runtime.ActivityStateCompleting {
		err = propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping())
		if err != nil {
			job.JobState = runtime.ActivityStateFailed
			instance.State = runtime.ActivityStateFailed
		} else {
			job.JobState = runtime.ActivityStateCompleted
		}
	}
	err = batch.SaveJob(ctx, *job)
	if err != nil {
		return nil, fmt.Errorf("failed to add save job into batch: %w", err)
	}
	err = batch.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to close batch for handle service task: %w", err)
	}

	return job, nil
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
	element := job.BaseElement.(bpmn20.TaskElement)
	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		job.JobState = runtime.ActivityStateFailed
		instance.State = runtime.ActivityStateFailed
	}
	// TODO: variable mapping needs to be implemented
	job.JobState = runtime.ActivityStateCompleting
	engine.persistence.SaveJob(ctx, job)
	engine.persistence.SaveProcessInstance(ctx, instance)

	engine.RunOrContinueInstance(job.ProcessInstanceKey)

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
			return nil, fmt.Errorf("failed to find process instance for job key: %d: %w", job.JobKey, err)
		}
		variableHolder := processInstance.VariableHolder
		if err := evaluateLocalVariables(&variableHolder, job.BaseElement.(bpmn20.TaskElement).GetInputMapping()); err != nil {
			job.JobState = runtime.ActivityStateFailed
			engine.persistence.SaveJob(ctx, job)
			return nil, err
		}
		aj := &activatedJob{
			processInstanceInfo: &processInstance,
			key:                 job.JobKey,
			processInstanceKey:  job.ProcessInstanceKey,
			elementId:           job.ElementId,
			createdAt:           job.CreatedAt,
			variableHolder:      variableHolder,
		}
		activatedJobs = append(activatedJobs, aj)
	}
	return activatedJobs, nil
}

func findOrCreateJob(ctx context.Context, engine *Engine, jobWriter storage.JobStorageWriter, element bpmn20.TaskElement, instance *runtime.ProcessInstance, generateKey func() int64) (*runtime.Job, error) {
	be := element.(bpmn20.FlowNode)
	job, err := engine.persistence.FindJobByElementID(ctx, instance.Key, be.GetId())

	if err == nil {
		return &job, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("failed to find job by element id: %s for process instance: %d: %w", be.GetId(), instance.Key, err)
	}

	elementInstanceKey := generateKey()
	job = runtime.Job{
		ElementId:          be.GetId(),
		ElementInstanceKey: elementInstanceKey,
		ProcessInstanceKey: instance.GetInstanceKey(),
		JobKey:             elementInstanceKey + 1,
		JobState:           runtime.ActivityStateActive,
		CreatedAt:          time.Now(),
		BaseElement:        be,
	}
	jobWriter.SaveJob(ctx, job)

	return &job, nil
}
