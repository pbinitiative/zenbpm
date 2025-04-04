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

func (engine *Engine) handleServiceTask(ctx context.Context, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TaskElement) (*runtime.Job, error) {
	job, err := findOrCreateJob(ctx, engine, element, instance, engine.generateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find or create job: %w", err)
	}

	//FIXME: logic of using the internal handler needs to be discussed whether it will be kept
	// If kept needs to work in parallel with external job completion
	handler := engine.findTaskHandler(element)
	variableHolder := runtime.New(&instance.VariableHolder, nil)
	if handler != nil {
		if job.JobState != runtime.Completing {
			job.JobState = runtime.Active
			activatedJob := &activatedJob{
				processInstanceInfo:      instance,
				failHandler:              func(reason string) { job.JobState = runtime.Failed },
				completeHandler:          func() { job.JobState = runtime.Completing },
				key:                      engine.generateKey(),
				processInstanceKey:       instance.InstanceKey,
				bpmnProcessId:            process.BpmnProcessId,
				processDefinitionVersion: process.Version,
				processDefinitionKey:     process.ProcessKey,
				elementId:                job.ElementId,
				createdAt:                job.CreatedAt,
				variableHolder:           variableHolder,
			}
			if err := evaluateLocalVariables(&variableHolder, element.GetInputMapping()); err != nil {
				job.JobState = runtime.Failed
				instance.State = runtime.Failed
				engine.persistence.SaveJob(ctx, *job)
				return job, nil
			}
			handler(activatedJob)
		}
	}

	if job.JobState == runtime.Completing {
		if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
			job.JobState = runtime.Failed
			instance.State = runtime.Failed
		}
		job.JobState = runtime.Completed
	}
	engine.persistence.SaveJob(ctx, *job)
	// state.persistence.GetPersistence().FlushTransaction(ctx)

	return job, nil
}

func (engine *Engine) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]interface{}) error {
	jobs, err := engine.persistence.FindJobsByJobKey(ctx, jobKey)
	if err != nil {
		return newEngineErrorf("failed to find job with key: %d: %w", jobKey, err)
	}

	if len(jobs) == 0 {
		return nil
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, jobs[0].ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find process instance with key: %d: %w", jobs[0].ProcessInstanceKey, err)
	}

	variableHolder := runtime.NewForPropagation(&instance.VariableHolder, variables)
	element := jobs[0].BaseElement.(bpmn20.TaskElement)
	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		jobs[0].JobState = runtime.Failed
		instance.State = runtime.Failed
	}
	// TODO: variable mapping needs to be implemented
	jobs[0].JobState = runtime.Completing
	engine.persistence.SaveJob(ctx, jobs[0])
	engine.persistence.SaveProcessInstance(ctx, instance)

	engine.RunOrContinueInstance(jobs[0].ProcessInstanceKey)

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
			job.JobState = runtime.Failed
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

func findOrCreateJob(ctx context.Context, engine *Engine, element bpmn20.TaskElement, instance *runtime.ProcessInstance, generateKey func() int64) (*runtime.Job, error) {
	be := element.(bpmn20.FlowNode)
	job, err := engine.persistence.FindJobByElementID(ctx, instance.InstanceKey, be.GetId())

	if err == nil {
		return &job, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("failed to find job by element id: %s for process instance: %d: %w", be.GetId(), instance.InstanceKey, err)
	}

	elementInstanceKey := generateKey()
	job = runtime.Job{
		ElementId:          be.GetId(),
		ElementInstanceKey: elementInstanceKey,
		ProcessInstanceKey: instance.GetInstanceKey(),
		JobKey:             elementInstanceKey + 1,
		JobState:           runtime.Active,
		CreatedAt:          time.Now(),
		BaseElement:        be,
	}
	engine.persistence.SaveJob(ctx, job)

	return &job, nil
}
