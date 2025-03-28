package bpmn

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (state *Engine) handleServiceTask(ctx context.Context, process *runtime.ProcessDefinition, instance *processInstanceInfo, element bpmn20.TaskElement) (bool, *runtime.Job) {
	job := findOrCreateJob(ctx, state, element, instance, state.generateKey)

	//FIXME: logic of using the internal handler needs to be discussed whether it will be kept
	// If kept needs to work in parallel with external job completion
	handler := state.findTaskHandler(element)
	variableHolder := runtime.New(&instance.VariableHolder, nil)
	if handler != nil {
		if job.JobState != runtime.Completing {
			job.JobState = runtime.Active
			activatedJob := &activatedJob{
				processInstanceInfo:      instance,
				failHandler:              func(reason string) { job.JobState = runtime.Failed },
				completeHandler:          func() { job.JobState = runtime.Completing },
				key:                      state.generateKey(),
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
				state.persistence.PersistJob(ctx, job)
				return false, job
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
	state.persistence.PersistJob(ctx, job)
	state.persistence.GetPersistence().FlushTransaction(ctx)

	return job.JobState == runtime.Completed, job
}

func (state *Engine) JobCompleteById(ctx context.Context, jobId int64, variables map[string]interface{}) {
	jobs := state.persistence.FindJobs(nil, nil, nil, &jobId)

	if len(jobs) == 0 {
		return
	}

	instance := state.persistence.FindProcessInstanceByKey(jobs[0].ProcessInstanceKey)
	if instance == nil {
		return
	}

	variableHolder := runtime.NewForPropagation(&instance.VariableHolder, variables)
	element := jobs[0].BaseElement.(bpmn20.TaskElement)
	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		jobs[0].JobState = runtime.Failed
		instance.State = runtime.Failed
	}
	// TODO: variabl mapping needs to be implemented
	jobs[0].JobState = runtime.Completing
	state.persistence.PersistJob(ctx, jobs[0])
	state.persistence.PersistProcessInstance(ctx, instance)

	state.RunOrContinueInstance(jobs[0].ProcessInstanceKey)

}

func (state *Engine) ActivateJobs(ctx context.Context, jobType string) (activatedJobs []ActivatedJob, err error) {
	jobs := state.persistence.FindJobs(nil, &jobType, nil, nil, runtime.Active)

	activatedJobs = make([]ActivatedJob, 0)
	for _, job := range jobs {

		processInstance := state.FindProcessInstance(job.ProcessInstanceKey)
		if processInstance == nil {
			continue
		}
		variableHolder := processInstance.VariableHolder
		if err := evaluateLocalVariables(&variableHolder, job.BaseElement.(bpmn20.TaskElement).GetInputMapping()); err != nil {
			job.JobState = runtime.Failed
			state.persistence.PersistJob(ctx, job)
			return nil, err
		}
		aj := &activatedJob{
			processInstanceInfo: processInstance,
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

func findOrCreateJob(ctx context.Context, state *Engine, element bpmn20.TaskElement, instance *processInstanceInfo, generateKey func() int64) *runtime.Job {
	be := element.(bpmn20.FlowNode)
	jobs := state.persistence.FindJobs(ptr.To(be.GetId()), nil, instance, nil)
	if len(jobs) > 0 {
		jobs[0].BaseElement = be
		return jobs[0]
	}

	elementInstanceKey := generateKey()
	job := runtime.Job{
		ElementId:          be.GetId(),
		ElementInstanceKey: elementInstanceKey,
		ProcessInstanceKey: instance.GetInstanceKey(),
		JobKey:             elementInstanceKey + 1,
		JobState:           runtime.Active,
		CreatedAt:          time.Now(),
		BaseElement:        be,
	}

	state.persistence.PersistJob(ctx, &job)

	return &job
}
