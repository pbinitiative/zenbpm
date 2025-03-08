package bpmn

import (
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/var_holder"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type job struct {
	ElementId          string               `json:"id"`
	ElementInstanceKey int64                `json:"ik"`
	ProcessInstanceKey int64                `json:"pik"`
	JobKey             int64                `json:"jk"`
	JobState           bpmn20.ActivityState `json:"s"`
	CreatedAt          time.Time            `json:"c"`
	baseElement        bpmn20.FlowNode
}

func (j job) Key() int64 {
	return j.JobKey
}

func (j job) State() bpmn20.ActivityState {
	return j.JobState
}

func (j job) Element() bpmn20.FlowNode {
	return j.baseElement
}

func (state *BpmnEngineState) handleServiceTask(process *ProcessInfo, instance *processInstanceInfo, element bpmn20.TaskElement) (bool, *job) {
	job := findOrCreateJob(state, element, instance, state.generateKey)

	//FIXME: logic of using the internal handler needs to be discussed whether it will be kept
	// If kept needs to work in parallel with external job completion
	handler := state.findTaskHandler(element)
	variableHolder := var_holder.New(&instance.VariableHolder, nil)
	if handler != nil {
		if job.JobState != bpmn20.Completing {
			job.JobState = bpmn20.Active
			activatedJob := &activatedJob{
				processInstanceInfo:      instance,
				failHandler:              func(reason string) { job.JobState = bpmn20.Failed },
				completeHandler:          func() { job.JobState = bpmn20.Completing },
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
				job.JobState = bpmn20.Failed
				instance.State = bpmn20.Failed
				state.persistence.PersistJob(job)
				return false, job
			}
			handler(activatedJob)
		}
	}

	if job.JobState == bpmn20.Completing {
		if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
			job.JobState = bpmn20.Failed
			instance.State = bpmn20.Failed
		}
		job.JobState = bpmn20.Completed
	}
	state.persistence.PersistJob(job)

	return job.JobState == bpmn20.Completed, job
}

func (state *BpmnEngineState) JobCompleteById(jobId int64) {
	jobs := state.persistence.FindJobs("", nil, jobId)

	if len(jobs) == 0 {
		return
	}
	jobs[0].JobState = bpmn20.Completing
	state.persistence.PersistJob(jobs[0])

	state.RunOrContinueInstance(jobs[0].ProcessInstanceKey)

}

func findOrCreateJob(state *BpmnEngineState, element bpmn20.TaskElement, instance *processInstanceInfo, generateKey func() int64) *job {
	be := element.(bpmn20.FlowNode)
	jobs := state.persistence.FindJobs(be.GetId(), instance, -1)
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
		JobState:           bpmn20.Active,
		CreatedAt:          time.Now(),
		baseElement:        be,
	}

	state.persistence.PersistJob(&job)

	return &job
}
