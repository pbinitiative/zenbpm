package inmemory

import (
	"context"
	"errors"
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"math/rand"
	"slices"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// Storage keeps process information in memory,
// please use NewStorage to create a new object of this type.
type Storage struct {
	DecisionDefinitions  map[int64]dmnruntime.DecisionDefinition
	ProcessDefinitions   map[int64]bpmnruntime.ProcessDefinition
	ProcessInstances     map[int64]bpmnruntime.ProcessInstance
	MessageSubscriptions map[int64]bpmnruntime.MessageSubscription
	Timers               map[int64]bpmnruntime.Timer
	Jobs                 map[int64]bpmnruntime.Job
	ExecutionTokens      map[int64]bpmnruntime.ExecutionToken
}

func (mem *Storage) GenerateId() int64 {
	return rand.Int63()
}

func NewStorage() *Storage {
	return &Storage{
		DecisionDefinitions:  make(map[int64]dmnruntime.DecisionDefinition),
		ProcessDefinitions:   make(map[int64]bpmnruntime.ProcessDefinition),
		ProcessInstances:     make(map[int64]bpmnruntime.ProcessInstance),
		MessageSubscriptions: make(map[int64]bpmnruntime.MessageSubscription),
		Timers:               make(map[int64]bpmnruntime.Timer),
		Jobs:                 make(map[int64]bpmnruntime.Job),
		ExecutionTokens:      make(map[int64]bpmnruntime.ExecutionToken),
	}
}

var _ storage.Storage = &Storage{}

func (mem *Storage) NewBatch() storage.Batch {
	return &StorageBatch{
		db:        mem,
		stmtToRun: make([]func() error, 0, 10),
	}
}

var _ storage.DecisionDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveDecisionDefinition(ctx context.Context, definition dmnruntime.DecisionDefinition) error {
	mem.DecisionDefinitions[definition.Key] = definition
	return nil
}

var _ storage.DecisionDefinitionStorageReader = &Storage{}

func (mem *Storage) FindDecisionDefinitionsById(ctx context.Context, decisionId string) ([]dmnruntime.DecisionDefinition, error) {
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, def := range mem.DecisionDefinitions {
		if def.Id != decisionId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b dmnruntime.DecisionDefinition) int {
		return int(a.Version - b.Version)
	})

	return res, nil
}

var _ storage.ProcessDefinitionStorageReader = &Storage{}

func (mem *Storage) FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (bpmnruntime.ProcessDefinition, error) {
	var res bpmnruntime.ProcessDefinition
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

func (mem *Storage) FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (bpmnruntime.ProcessDefinition, error) {
	res, ok := mem.ProcessDefinitions[processDefinitionKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindProcessDefinitionsById(ctx context.Context, processId string) ([]bpmnruntime.ProcessDefinition, error) {
	res := make([]bpmnruntime.ProcessDefinition, 0)
	for _, def := range mem.ProcessDefinitions {
		if def.BpmnProcessId != processId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b bpmnruntime.ProcessDefinition) int {
		return int(a.Version - b.Version)
	})

	return res, nil
}

var _ storage.ProcessDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	mem.ProcessDefinitions[definition.Key] = definition
	return nil
}

var _ storage.ProcessInstanceStorageReader = &Storage{}

func (mem *Storage) FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (bpmnruntime.ProcessInstance, error) {
	res, ok := mem.ProcessInstances[processInstanceKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &Storage{}

func (mem *Storage) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	mem.ProcessInstances[processInstance.Key] = processInstance
	return nil
}

var _ storage.TimerStorageReader = &Storage{}

func (mem *Storage) FindTimersByState(ctx context.Context, processInstanceKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error) {
	res := make([]bpmnruntime.Timer, 0)
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

func (mem *Storage) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	mem.Timers[timer.GetKey()] = timer
	return nil
}

var _ storage.JobStorageReader = &Storage{}

func (mem *Storage) FindActiveJobsByType(ctx context.Context, jobType string) ([]bpmnruntime.Job, error) {
	res := make([]bpmnruntime.Job, 0)
	for _, job := range mem.Jobs {
		// TODO: uncomment once we have type
		// if job.Type != jobType{
		// 	continue
		// }
		res = append(res, job)
	}
	return res, nil
}

func (mem *Storage) FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (bpmnruntime.Job, error) {
	var res bpmnruntime.Job
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

func (mem *Storage) FindJobByJobKey(ctx context.Context, jobKey int64) (bpmnruntime.Job, error) {
	var res bpmnruntime.Job
	res, ok := mem.Jobs[jobKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Job, error) {
	res := make([]bpmnruntime.Job, 0)
	for _, job := range mem.Jobs {
		if job.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if job.GetState() != bpmnruntime.ActivityStateActive && job.GetState() != bpmnruntime.ActivityStateCompleting {
			continue
		}
		res = append(res, job)
	}
	return res, nil
}

var _ storage.JobStorageWriter = &Storage{}

func (mem *Storage) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	mem.Jobs[job.GetKey()] = job
	return nil
}

var _ storage.MessageStorageReader = &Storage{}

// FindTokenMessageSubscriptions implements storage.Storage.
func (mem *Storage) FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {
	res := make([]bpmnruntime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.Token.Key == tokenKey {
			res = append(res, sub)
		}
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {
	res := make([]bpmnruntime.MessageSubscription, 0)
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

func (mem *Storage) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	mem.MessageSubscriptions[subscription.GetKey()] = subscription
	return nil
}

var _ storage.TokenStorageReader = &Storage{}

// GetTokensForProcessInstance implements storage.TokenStorageReader.
func (mem *Storage) GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	res := make([]bpmnruntime.ExecutionToken, 0)
	for _, tok := range mem.ExecutionTokens {
		if tok.ProcessInstanceKey == processInstanceKey {
			res = append(res, tok)
		}
	}
	return res, nil
}

// GetActiveTokensForPartition implements storage.Storage.
func (mem *Storage) GetRunningTokens(ctx context.Context) ([]bpmnruntime.ExecutionToken, error) {
	activeTokens := make([]bpmnruntime.ExecutionToken, 0)
	for _, token := range mem.ExecutionTokens {
		if token.State == bpmnruntime.TokenStateRunning {
			activeTokens = append(activeTokens, token)
		}
	}
	return activeTokens, nil
}

var _ storage.TokenStorageWriter = &Storage{}

// SaveToken implements storage.Storage.
func (mem *Storage) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	mem.ExecutionTokens[token.Key] = token
	return nil
}

type StorageBatch struct {
	db        *Storage
	stmtToRun []func() error
}

var _ storage.Batch = &StorageBatch{}

// TODO: for now close just calls the functions
// in the future we want to actually execute this as one statement into memlite
func (b *StorageBatch) Flush(ctx context.Context) error {
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

var _ storage.ProcessDefinitionStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessDefinition(ctx, definition)
	})
	return nil
}

var _ storage.ProcessInstanceStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessInstance(ctx, processInstance)
	})
	return nil
}

var _ storage.TimerStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveTimer(ctx, timer)
	})
	return nil
}

var _ storage.JobStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveJob(ctx, job)
	})
	return nil
}

var _ storage.MessageStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveMessageSubscription(ctx, subscription)
	})
	return nil
}

var _ storage.TokenStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveToken(ctx, token)
	})
	return nil
}
