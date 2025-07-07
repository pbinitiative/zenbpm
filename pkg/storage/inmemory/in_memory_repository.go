package inmemory

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"time"

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
	ExecutionTokens      map[int64]runtime.ExecutionToken
	FlowElementHistory   map[int64]runtime.FlowElementHistoryItem
	Incidents            map[int64]runtime.Incident
}

func (mem *Storage) GenerateId() int64 {
	return rand.Int63()
}

func NewStorage() *Storage {
	return &Storage{
		ProcessDefinitions:   make(map[int64]runtime.ProcessDefinition),
		ProcessInstances:     make(map[int64]runtime.ProcessInstance),
		MessageSubscriptions: make(map[int64]runtime.MessageSubscription),
		Timers:               make(map[int64]runtime.Timer),
		Jobs:                 make(map[int64]runtime.Job),
		ExecutionTokens:      make(map[int64]runtime.ExecutionToken),
		FlowElementHistory:   make(map[int64]runtime.FlowElementHistoryItem),
		Incidents:            make(map[int64]runtime.Incident),
	}
}

var _ storage.Storage = &Storage{}

func (mem *Storage) NewBatch() storage.Batch {
	return &StorageBatch{
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

func (mem *Storage) FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]runtime.Timer, error) {
	res := make([]runtime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.TimerState != runtime.TimerStateCreated {
			continue
		}
		if timer.Token.Key != tokenKey {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

func (mem *Storage) FindTimersTo(ctx context.Context, end time.Time) ([]runtime.Timer, error) {
	res := make([]runtime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.DueAt.After(end) {
			continue
		}
		if timer.TimerState != runtime.TimerStateCreated {
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

// FindTokenMessageSubscriptions implements storage.Storage.
func (mem *Storage) FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	res := make([]runtime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.Token.Key == tokenKey {
			res = append(res, sub)
		}
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

func (mem *Storage) FindIncidentByKey(ctx context.Context, key int64) (runtime.Incident, error) {
	var res runtime.Incident
	res, ok := mem.Incidents[key]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]runtime.Incident, error) {
	res := make([]runtime.Incident, 0)
	for _, inc := range mem.Incidents {
		if inc.ProcessInstanceKey != processInstanceKey {
			continue
		}
		res = append(res, inc)
	}
	return res, nil
}

var _ storage.MessageStorageWriter = &Storage{}

func (mem *Storage) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	mem.MessageSubscriptions[subscription.GetKey()] = subscription
	return nil
}

var _ storage.TokenStorageReader = &Storage{}

// GetTokensForProcessInstance implements storage.TokenStorageReader.
func (mem *Storage) GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]runtime.ExecutionToken, error) {
	res := make([]runtime.ExecutionToken, 0)
	for _, tok := range mem.ExecutionTokens {
		if tok.ProcessInstanceKey == processInstanceKey {
			res = append(res, tok)
		}
	}
	return res, nil
}

// GetActiveTokensForPartition implements storage.Storage.
func (mem *Storage) GetRunningTokens(ctx context.Context) ([]runtime.ExecutionToken, error) {
	activeTokens := make([]runtime.ExecutionToken, 0)
	for _, token := range mem.ExecutionTokens {
		if token.State == runtime.TokenStateRunning {
			activeTokens = append(activeTokens, token)
		}
	}
	return activeTokens, nil
}

var _ storage.TokenStorageWriter = &Storage{}

// SaveToken implements storage.Storage.
func (mem *Storage) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	mem.ExecutionTokens[token.Key] = token
	return nil
}

func (mem *Storage) SaveFlowElementHistory(ctx context.Context, historyItem runtime.FlowElementHistoryItem) error {
	mem.FlowElementHistory[historyItem.Key] = historyItem
	return nil
}

func (mem *Storage) SaveIncident(ctx context.Context, incident runtime.Incident) error {
	mem.Incidents[incident.Key] = incident
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

func (b *StorageBatch) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessDefinition(ctx, definition)
	})
	return nil
}

var _ storage.ProcessInstanceStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveProcessInstance(ctx, processInstance)
	})
	return nil
}

var _ storage.TimerStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveTimer(ctx, timer)
	})
	return nil
}

var _ storage.JobStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveJob(ctx context.Context, job runtime.Job) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveJob(ctx, job)
	})
	return nil
}

var _ storage.MessageStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveMessageSubscription(ctx, subscription)
	})
	return nil
}

var _ storage.TokenStorageWriter = &StorageBatch{}

func (b *StorageBatch) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveToken(ctx, token)
	})
	return nil
}

func (b *StorageBatch) SaveFlowElementHistory(ctx context.Context, historyItem runtime.FlowElementHistoryItem) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveFlowElementHistory(ctx, historyItem)
	})
	return nil
}

func (b *StorageBatch) SaveIncident(ctx context.Context, incident runtime.Incident) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveIncident(ctx, incident)
	})
	return nil
}
