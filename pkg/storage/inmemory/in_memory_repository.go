package inmemory

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"slices"
	"sort"
)

// InMemoryStorage keeps process information in memory,
// please use NewInMemory to create a new object of this type.
type InMemoryStorage struct {
	processDefinitions   map[[16]byte]runtime.ProcessDefinition
	processInstances     map[int64]runtime.ProcessInstance
	messageSubscriptions map[int64]runtime.MessageSubscription
	timers               map[int64]runtime.Timer
	jobs                 map[int64]runtime.Job
}

func NewInMemory() storage.PersistentStorageNew {
	return &InMemoryStorage{
		processDefinitions:   make(map[[16]byte]runtime.ProcessDefinition),
		processInstances:     make(map[int64]runtime.ProcessInstance),
		messageSubscriptions: make(map[int64]runtime.MessageSubscription),
		timers:               make(map[int64]runtime.Timer),
		jobs:                 make(map[int64]runtime.Job),
	}
}

func (mem *InMemoryStorage) FindProcessDefinitionsById(ctx context.Context, processIds ...string) (definitions []runtime.ProcessDefinition, err error) {
	for _, d := range mem.processDefinitions {
		if slices.Contains(processIds, d.BpmnProcessId()) {
			definitions = append(definitions, d)
		}
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Version < definitions[j].Version
	})
	return definitions, nil
}

func (mem *InMemoryStorage) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	mem.processDefinitions[definition.BpmnChecksum] = definition
	return nil
}

func (mem *InMemoryStorage) FindProcessInstancesByKey(ctx context.Context, processInstanceKeys ...int64) (instances []runtime.ProcessInstance, err error) {
	for _, i := range mem.processInstances {
		if slices.Contains(processInstanceKeys, i.InstanceKey()) {
			instances = append(instances, i)
		}
	}
	return instances, nil
}

func (mem *InMemoryStorage) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	mem.processInstances[processInstance.GetInstanceKey()] = processInstance
	return nil
}

func (mem *InMemoryStorage) FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []runtime.ActivityState) ([]runtime.MessageSubscription, error) {

	//TODO implement me
	panic("implement me")
}

func (mem *InMemoryStorage) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	mem.messageSubscriptions[subscription.ElementInstanceKey] = subscription
	return nil
}

func (mem *InMemoryStorage) FindTimersByState(ctx context.Context, state runtime.TimerState) (timers []runtime.Timer, err error) {
	for _, t := range mem.timers {
		if t.TimerState == state {
			timers = append(timers, t)
		}
	}
	return timers, err
}

func (mem *InMemoryStorage) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	mem.timers[timer.ElementInstanceKey] = timer
	return nil
}

func (mem *InMemoryStorage) SaveJob(ctx context.Context, job runtime.Job) error {
	mem.jobs[job.JobKey] = job
	return nil
}

func (mem *InMemoryStorage) FindJobsByJobKey(ctx context.Context, jobKey int64) (jobs []runtime.Job, err error) {
	for _, j := range mem.jobs {
		if jobKey == j.JobKey {
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

func (mem *InMemoryStorage) FindJobsByState(ctx context.Context, state runtime.ActivityState) (jobs []runtime.Job, err error) {
	for _, j := range mem.jobs {
		if j.JobState == state {
			jobs = append(jobs, j)
		}
	}
	return jobs, err
}

func (mem *InMemoryStorage) FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]runtime.Activity, error) {
	//TODO implement me
	panic("implement me")
}

func (mem *InMemoryStorage) SaveActivity(ctx context.Context, activity runtime.Activity) error {
	//TODO implement me
	panic("implement me")
}

func (mem *InMemoryStorage) IsLeader(ctx context.Context) bool {
	return true
}
