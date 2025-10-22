package inmemory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"time"

	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// Storage keeps process information in memory,
// please use NewStorage to create a new object of this type.
type Storage struct {
	Decision             map[string]map[int64]dmnruntime.Decision
	DecisionDefinitions  map[int64]dmnruntime.DecisionDefinition
	ProcessDefinitions   map[int64]bpmnruntime.ProcessDefinition
	ProcessInstances     map[int64]bpmnruntime.ProcessInstance
	MessageSubscriptions map[int64]bpmnruntime.MessageSubscription
	Timers               map[int64]bpmnruntime.Timer
	Jobs                 map[int64]bpmnruntime.Job
	ExecutionTokens      map[int64]bpmnruntime.ExecutionToken
	FlowElementHistory   map[int64]bpmnruntime.FlowElementHistoryItem
	Incidents            map[int64]bpmnruntime.Incident
}

func (mem *Storage) GenerateId() int64 {
	return rand.Int63()
}

func NewStorage() *Storage {
	return &Storage{
		Decision:             make(map[string]map[int64]dmnruntime.Decision),
		DecisionDefinitions:  make(map[int64]dmnruntime.DecisionDefinition),
		ProcessDefinitions:   make(map[int64]bpmnruntime.ProcessDefinition),
		ProcessInstances:     make(map[int64]bpmnruntime.ProcessInstance),
		MessageSubscriptions: make(map[int64]bpmnruntime.MessageSubscription),
		Timers:               make(map[int64]bpmnruntime.Timer),
		Jobs:                 make(map[int64]bpmnruntime.Job),
		ExecutionTokens:      make(map[int64]bpmnruntime.ExecutionToken),
		FlowElementHistory:   make(map[int64]bpmnruntime.FlowElementHistoryItem),
		Incidents:            make(map[int64]bpmnruntime.Incident),
	}
}

func (mem *Storage) Copy() *Storage {
	c := NewStorage()
	for k, v := range mem.Decision {
		c.Decision[k] = v
	}
	for k, v := range mem.DecisionDefinitions {
		c.DecisionDefinitions[k] = v
	}
	for k, v := range mem.ProcessInstances {
		c.ProcessInstances[k] = v
	}
	for k, v := range mem.MessageSubscriptions {
		c.MessageSubscriptions[k] = v
	}
	for k, v := range mem.Timers {
		c.Timers[k] = v
	}
	for k, v := range mem.Jobs {
		c.Jobs[k] = v
	}
	for k, v := range mem.ExecutionTokens {
		c.ExecutionTokens[k] = v
	}
	for k, v := range mem.FlowElementHistory {
		c.FlowElementHistory[k] = v
	}
	for k, v := range mem.Incidents {
		c.Incidents[k] = v
	}
	return c
}

var _ storage.Storage = &Storage{}

func (mem *Storage) NewBatch() storage.Batch {
	return &StorageBatch{
		db:               mem,
		stmtToRun:        make([]func() error, 0, 10),
		postFlushActions: make([]func(), 0, 5),
	}
}

var _ storage.DecisionStorageReader = &Storage{}

func (mem *Storage) GetLatestDecisionById(ctx context.Context, decisionId string) (dmnruntime.Decision, error) {
	res := make([]dmnruntime.Decision, 0)
	for _, dec := range mem.Decision[decisionId] {
		res = append(res, dec)
	}
	slices.SortFunc(res, func(a, b dmnruntime.Decision) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.Decision{}, storage.ErrNotFound
}

func (mem *Storage) GetDecisionsById(ctx context.Context, decisionId string) ([]dmnruntime.Decision, error) {
	res := make([]dmnruntime.Decision, 0)
	for _, dec := range mem.Decision[decisionId] {
		res = append(res, dec)
	}
	return res, nil
}

func (mem *Storage) GetLatestDecisionByIdAndVersionTag(ctx context.Context, decisionId string, versionTag string) (dmnruntime.Decision, error) {
	res := make([]dmnruntime.Decision, 0)
	for _, dec := range mem.Decision[decisionId] {
		if dec.VersionTag != versionTag {
			continue
		}
		res = append(res, dec)
	}
	slices.SortFunc(res, func(a, b dmnruntime.Decision) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.Decision{}, storage.ErrNotFound
}

func (mem *Storage) GetLatestDecisionByIdAndDecisionDefinitionId(ctx context.Context, decisionId string, decisionDefinitionId string) (dmnruntime.Decision, error) {
	res := make([]dmnruntime.Decision, 0)
	for _, dec := range mem.Decision[decisionId] {
		if dec.DecisionDefinitionId != decisionDefinitionId {
			continue
		}
		res = append(res, dec)
	}
	slices.SortFunc(res, func(a, b dmnruntime.Decision) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.Decision{}, storage.ErrNotFound
}

func (mem *Storage) GetDecisionByIdAndDecisionDefinitionKey(ctx context.Context, decisionId string, decisionDefinitionKey int64) (dmnruntime.Decision, error) {
	return mem.Decision[decisionId][decisionDefinitionKey], nil
}

var _ storage.DecisionStorageWriter = &Storage{}

func (mem *Storage) SaveDecision(ctx context.Context, decision dmnruntime.Decision) error {
	mem.Decision[decision.Id] = make(map[int64]dmnruntime.Decision)
	mem.Decision[decision.Id][decision.DecisionDefinitionKey] = decision
	return nil
}

var _ storage.DecisionDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveDecisionDefinition(ctx context.Context, definition dmnruntime.DecisionDefinition) error {
	mem.DecisionDefinitions[definition.Key] = definition
	return nil
}

var _ storage.DecisionStorageWriter = &Storage{}

var _ storage.DecisionDefinitionStorageReader = &Storage{}

func (mem *Storage) FindLatestDecisionDefinitionById(ctx context.Context, decisionDefinitionId string) (dmnruntime.DecisionDefinition, error) {
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, def := range mem.DecisionDefinitions {
		if def.Id != decisionDefinitionId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b dmnruntime.DecisionDefinition) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.DecisionDefinition{}, storage.ErrNotFound
}

func (mem *Storage) FindDecisionDefinitionByKey(ctx context.Context, decisionDefinitionKey int64) (dmnruntime.DecisionDefinition, error) {
	res, ok := mem.DecisionDefinitions[decisionDefinitionKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindDecisionDefinitionsById(ctx context.Context, decisionDefinitionId string) ([]dmnruntime.DecisionDefinition, error) {
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, def := range mem.DecisionDefinitions {
		if def.Id != decisionDefinitionId {
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

func (mem *Storage) FindProcessInstanceByParentExecutionTokenKey(ctx context.Context, parentExecutionTokenKey int64) ([]bpmnruntime.ProcessInstance, error) {
	res := make([]bpmnruntime.ProcessInstance, 0)
	for _, processInstance := range mem.ProcessInstances {
		if processInstance.ParentProcessExecutionToken != nil && processInstance.ParentProcessExecutionToken.Key == parentExecutionTokenKey {
			res = append(res, processInstance)
		}
	}
	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &Storage{}

func (mem *Storage) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	mem.ProcessInstances[processInstance.Key] = processInstance
	return nil
}

var _ storage.TimerStorageReader = &Storage{}

func (mem *Storage) FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]bpmnruntime.Timer, error) {
	res := make([]bpmnruntime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.TimerState != bpmnruntime.TimerStateCreated {
			continue
		}
		if timer.Token.Key != tokenKey {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceTimers(ctx context.Context, processInstanceKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error) {
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

func (mem *Storage) FindTimersTo(ctx context.Context, end time.Time) ([]bpmnruntime.Timer, error) {
	res := make([]bpmnruntime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.DueAt.After(end) {
			continue
		}
		if timer.TimerState != bpmnruntime.TimerStateCreated {
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
		if job.Type != jobType || job.State != bpmnruntime.ActivityStateActive {
			continue
		}
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

func (mem *Storage) FindTokenJobsInState(ctx context.Context, tokenKey int64, states []bpmnruntime.ActivityState) ([]bpmnruntime.Job, error) {
	res := make([]bpmnruntime.Job, 0)
	for _, job := range mem.Jobs {
		if job.Token.Key != tokenKey {
			continue
		}
		for _, s := range states {
			if job.State != s {
				continue
			} else {
				res = append(res, job)
			}
		}
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

func (mem *Storage) FindMessageSubscriptionById(ctx context.Context, key int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error) {
	var res bpmnruntime.MessageSubscription
	res, ok := mem.MessageSubscriptions[key]
	if !ok {
		return res, storage.ErrNotFound
	}
	if res.State == state {
		return res, nil
	}
	return res, storage.ErrNotFound
}

// FindTokenMessageSubscriptions implements storage.Storage.
func (mem *Storage) FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {
	res := make([]bpmnruntime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.Token.Key == tokenKey && sub.State == state {
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

func (mem *Storage) FindActiveMessageSubscriptionKey(ctx context.Context, name string, correlationKey string) (int64, error) {
	res := make([]bpmnruntime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.GetState() != bpmnruntime.ActivityStateActive {
			continue
		}
		if sub.Name == name && sub.CorrelationKey == correlationKey {
			res = append(res, sub)
		}
	}
	if len(res) == 0 {
		return 0, storage.ErrNotFound
	}
	return res[0].Key, nil
}

func (mem *Storage) FindIncidentsByExecutionTokenKey(ctx context.Context, executionTokenKey int64) ([]bpmnruntime.Incident, error) {
	res := make([]bpmnruntime.Incident, 0)
	for _, inc := range mem.Incidents {
		if inc.Token.Key != executionTokenKey {
			continue
		}
		res = append(res, inc)
	}
	return res, nil
}

func (mem *Storage) FindIncidentByKey(ctx context.Context, key int64) (bpmnruntime.Incident, error) {
	var res bpmnruntime.Incident
	res, ok := mem.Incidents[key]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Incident, error) {
	res := make([]bpmnruntime.Incident, 0)
	for _, inc := range mem.Incidents {
		if inc.ProcessInstanceKey != processInstanceKey {
			continue
		}
		res = append(res, inc)
	}
	return res, nil
}

var _ storage.MessageStorageWriter = &Storage{}

func (mem *Storage) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	for _, message := range mem.MessageSubscriptions {
		if subscription.Key == message.Key {
			break
		}
		if message.State == bpmnruntime.ActivityStateActive && message.Name == subscription.Name && message.CorrelationKey == subscription.CorrelationKey {
			return fmt.Errorf("active message with the same correlationKey and name already exists")
		}
	}
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

func (mem *Storage) SaveFlowElementHistory(ctx context.Context, historyItem bpmnruntime.FlowElementHistoryItem) error {
	mem.FlowElementHistory[historyItem.Key] = historyItem
	return nil
}

func (mem *Storage) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	mem.Incidents[incident.Key] = incident
	return nil
}

type StorageBatch struct {
	db               *Storage
	stmtToRun        []func() error
	postFlushActions []func()
	preFlushActions  []func() error
}

var _ storage.Batch = &StorageBatch{}

func (b *StorageBatch) AddPostFlushAction(ctx context.Context, f func()) {
	b.postFlushActions = append(b.postFlushActions, f)
}

func (b *StorageBatch) AddPreFlushAction(ctx context.Context, f func() error) {
	b.preFlushActions = append(b.preFlushActions, f)
}

func (b *StorageBatch) Flush(ctx context.Context) error {
	dbCopy := b.db.Copy()
	var joinErr error
	for _, stmt := range b.stmtToRun {
		err := stmt()
		if err != nil {
			joinErr = errors.Join(joinErr, err)
		}
	}
	if joinErr != nil {
		b.db = dbCopy
		return joinErr
	}
	for _, action := range b.postFlushActions {
		action()
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

func (b *StorageBatch) SaveFlowElementHistory(ctx context.Context, historyItem bpmnruntime.FlowElementHistoryItem) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveFlowElementHistory(ctx, historyItem)
	})
	return nil
}

func (b *StorageBatch) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveIncident(ctx, incident)
	})
	return nil
}
