package inmemory

import (
	"context"
	"errors"
	"slices"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// Storage keeps process information in memory,
// please use NewStorage to create a new object of this type.
type Storage struct {
	ProcessDefinitions   map[int64]runtime.ProcessDefinition
	ProcessInstances     map[int64]runtime.ProcessInstance
	MessageSubscriptions map[int64]runtime.MessageSubscription
	Timers               map[int64]runtime.Timer
	Jobs                 map[int64]runtime.Job
}

func NewStorage() *Storage {
	return &Storage{
		ProcessDefinitions:   make(map[int64]runtime.ProcessDefinition),
		ProcessInstances:     make(map[int64]runtime.ProcessInstance),
		MessageSubscriptions: make(map[int64]runtime.MessageSubscription),
		Timers:               make(map[int64]runtime.Timer),
		Jobs:                 make(map[int64]runtime.Job),
	}
}

var _ storage.Storage = &Storage{}

func (mem *Storage) NewBatch() storage.Batch {
	return &InMemoryStorageBatch{
		db:        mem,
		stmtToRun: make([]func() error, 0, 10),
	}
}

var _ storage.ProcessDefinitionStorageReader = &Storage{}

func (mem *Storage) FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (runtime.ProcessDefinition, error) {
	var res runtime.ProcessDefinition
	found := false
	for _, def := range mem.ProcessDefinitions {
		if def.BpmnProcessId != processDefinitionId {
			continue
		}
		if res.Version != 0 && def.Version < res.Version {
			continue
		}
		found = true
		res = def
	}
	if !found {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (runtime.ProcessDefinition, error) {
	res, ok := mem.ProcessDefinitions[processDefinitionKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindProcessDefinitionsById(ctx context.Context, processId string) ([]runtime.ProcessDefinition, error) {
	res := make([]runtime.ProcessDefinition, 0)
	for _, def := range mem.ProcessDefinitions {
		if def.BpmnProcessId != processId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b runtime.ProcessDefinition) int {
		return int(a.Version - b.Version)
	})

	return res, nil
}

var _ storage.ProcessDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	mem.ProcessDefinitions[definition.Key] = definition
	return nil
}

var _ storage.ProcessInstanceStorageReader = &Storage{}

func (mem *Storage) FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (runtime.ProcessInstance, error) {
	res, ok := mem.ProcessInstances[processInstanceKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &Storage{}

func (mem *Storage) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	mem.ProcessInstances[processInstance.Key] = processInstance
	return nil
}

var _ storage.TimerStorageReader = &Storage{}

func (mem *Storage) FindActivityTimers(ctx context.Context, activityKey int64, state runtime.TimerState) ([]runtime.Timer, error) {
	res := make([]runtime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.OriginActivity.GetKey() != activityKey {
			continue
		}
		if timer.TimerState != state {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

func (mem *Storage) FindTimersByState(ctx context.Context, processInstanceKey int64, state runtime.TimerState) ([]runtime.Timer, error) {
	res := make([]runtime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if timer.TimerState != state {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

var _ storage.TimerStorageWriter = &Storage{}

func (mem *Storage) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	mem.Timers[timer.GetKey()] = timer
	return nil
}

var _ storage.JobStorageReader = &Storage{}

func (mem *Storage) FindActiveJobsByType(ctx context.Context, jobType string) ([]runtime.Job, error) {
	res := make([]runtime.Job, 0)
	for _, job := range mem.Jobs {
		// TODO: uncomment once we have type
		// if job.Type != jobType{
		// 	continue
		// }
		res = append(res, job)
	}
	return res, nil
}

func (mem *Storage) FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (runtime.Job, error) {
	var res runtime.Job
	for _, job := range mem.Jobs {
		if job.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if job.ElementId != elementID {
			continue
		}
		return job, nil
	}
	return res, storage.ErrNotFound
}

func (mem *Storage) FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error) {
	var res runtime.Job
	res, ok := mem.Jobs[jobKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error) {
	res := make([]runtime.Job, 0)
	for _, job := range mem.Jobs {
		if job.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if job.GetState() != runtime.ActivityStateActive && job.GetState() != runtime.ActivityStateCompleting {
			continue
		}
		res = append(res, job)
	}
	return res, nil
}

var _ storage.JobStorageWriter = &Storage{}

func (mem *Storage) SaveJob(ctx context.Context, job runtime.Job) error {
	mem.Jobs[job.GetKey()] = job
	return nil
}

var _ storage.MessageStorageReader = &Storage{}

func (mem *Storage) FindActivityMessageSubscriptions(ctx context.Context, originActivityKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	res := make([]runtime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.OriginActivity.GetKey() != originActivityKey {
			continue
		}
		if sub.GetState() != state {
			continue
		}
		res = append(res, sub)
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	res := make([]runtime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if sub.GetState() != state {
			continue
		}
		res = append(res, sub)
	}
	return res, nil
}

var _ storage.MessageStorageWriter = &Storage{}

func (mem *Storage) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	mem.MessageSubscriptions[subscription.GetKey()] = subscription
	return nil
}

type InMemoryStorageBatch struct {
	db        *Storage
	stmtToRun []func() error
}

var _ storage.Batch = &InMemoryStorageBatch{}

// TODO: for now close just calls the functions
// in the future we want to actually execute this as one statement into memlite
func (b *InMemoryStorageBatch) Flush(ctx context.Context) error {
	var joinErr error
	for _, stmt := range b.stmtToRun {
		err := stmt()
		if err != nil {
			joinErr = errors.Join(joinErr, err)
		}
	}
	if joinErr != nil {
		return joinErr
	}
	b.stmtToRun = make([]func() error, 0)
	return nil
}

var _ storage.ProcessDefinitionStorageWriter = &InMemoryStorageBatch{}

func (b *InMemoryStorageBatch) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessDefinition(ctx, definition)
	})
	return nil
}

var _ storage.ProcessInstanceStorageWriter = &InMemoryStorageBatch{}

func (b *InMemoryStorageBatch) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessInstance(ctx, processInstance)
	})
	return nil
}

var _ storage.TimerStorageWriter = &InMemoryStorageBatch{}

func (b *InMemoryStorageBatch) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveTimer(ctx, timer)
	})
	return nil
}

var _ storage.JobStorageWriter = &InMemoryStorageBatch{}

func (b *InMemoryStorageBatch) SaveJob(ctx context.Context, job runtime.Job) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveJob(ctx, job)
	})
	return nil
}

var _ storage.MessageStorageWriter = &InMemoryStorageBatch{}

func (b *InMemoryStorageBatch) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveMessageSubscription(ctx, subscription)
	})
	return nil
}
