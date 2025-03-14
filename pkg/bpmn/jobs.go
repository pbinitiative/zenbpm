package bpmn

import (
	"context"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/var_holder"
	"github.com/pbinitiative/zenbpm/pkg/ptr"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type job struct {
	ElementId          string        `json:"id"`
	ElementInstanceKey int64         `json:"ik"`
	ProcessInstanceKey int64         `json:"pik"`
	JobKey             int64         `json:"jk"`
	JobState           ActivityState `json:"s"`
	CreatedAt          time.Time     `json:"c"`
	baseElement        bpmn20.FlowNode
}

func (j job) Key() int64 {
	return j.JobKey
}

func (j job) State() ActivityState {
	return j.JobState
}

func (j job) Element() bpmn20.FlowNode {
	return j.baseElement
}

func (state *Engine) handleServiceTask(ctx context.Context, process *ProcessInfo, instance *processInstanceInfo, element bpmn20.TaskElement) (bool, *job) {
	job := findOrCreateJob(ctx, state, element, instance, state.generateKey)

	//FIXME: logic of using the internal handler needs to be discussed whether it will be kept
	// If kept needs to work in parallel with external job completion
	handler := state.findTaskHandler(element)
	variableHolder := var_holder.New(&instance.VariableHolder, nil)
	if handler != nil {
		if job.JobState != Completing {
			job.JobState = Active
			activatedJob := &activatedJob{
				processInstanceInfo:      instance,
				failHandler:              func(reason string) { job.JobState = Failed },
				completeHandler:          func() { job.JobState = Completing },
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
				job.JobState = Failed
				instance.State = Failed
				state.persistence.PersistJob(ctx, job)
				return false, job
			}
			handler(activatedJob)
		}
	}

	if job.JobState == Completing {
		if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
			job.JobState = Failed
			instance.State = Failed
		}
		job.JobState = Completed
	}
	state.persistence.PersistJob(ctx, job)

	return job.JobState == Completed, job
}

func (state *Engine) JobCompleteById(ctx context.Context, jobId int64) {
	jobs := state.persistence.FindJobs(nil, nil, &jobId)

	if len(jobs) == 0 {
		return
	}
	jobs[0].JobState = Completing
	state.persistence.PersistJob(ctx, jobs[0])

	state.RunOrContinueInstance(jobs[0].ProcessInstanceKey)

}

func findOrCreateJob(ctx context.Context, state *Engine, element bpmn20.TaskElement, instance *processInstanceInfo, generateKey func() int64) *job {
	be := element.(bpmn20.FlowNode)
	jobs := state.persistence.FindJobs(ptr.To(be.GetId()), instance, nil)
	if len(jobs) > 0 {
		jobs[0].baseElement = be
		return jobs[0]
	}

	elementInstanceKey := generateKey()
	job := job{
		ElementId:          be.GetId(),
		ElementInstanceKey: elementInstanceKey,
		ProcessInstanceKey: instance.GetInstanceKey(),
		JobKey:             elementInstanceKey + 1,
		JobState:           Active,
		CreatedAt:          time.Now(),
		baseElement:        be,
	}

	state.persistence.PersistJob(ctx, &job)

	return &job
}
