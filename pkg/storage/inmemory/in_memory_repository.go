package inmemory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// Storage keeps process information in memory,
// please use NewStorage to create a new object of this type.
type Storage struct {
	mu                     sync.RWMutex
	DmnResourceDefinitions map[int64]dmnruntime.DmnResourceDefinition
	DecisionDefinitions    map[int64]dmnruntime.DecisionDefinition
	DecisionInstances      map[int64]dmnruntime.DecisionInstance
	ProcessDefinitions     map[int64]bpmnruntime.ProcessDefinition
	ProcessInstances       map[int64]bpmnruntime.ProcessInstance
	MessageSubscriptions   map[int64]bpmnruntime.MessageSubscription
	Timers                 map[int64]bpmnruntime.Timer
	Jobs                   map[int64]bpmnruntime.Job
	ExecutionTokens        map[int64]bpmnruntime.ExecutionToken
	FlowElementInstance    map[int64]bpmnruntime.FlowElementInstance
	Incidents              map[int64]bpmnruntime.Incident
	ErrorSubscriptions     map[int64]bpmnruntime.ErrorSubscription
}

func (mem *Storage) GenerateId() int64 {
	return rand.Int63()
}

func NewStorage() *Storage {
	return &Storage{
		DmnResourceDefinitions: make(map[int64]dmnruntime.DmnResourceDefinition),
		DecisionDefinitions:    make(map[int64]dmnruntime.DecisionDefinition),
		DecisionInstances:      make(map[int64]dmnruntime.DecisionInstance),
		ProcessDefinitions:     make(map[int64]bpmnruntime.ProcessDefinition),
		ProcessInstances:       make(map[int64]bpmnruntime.ProcessInstance),
		MessageSubscriptions:   make(map[int64]bpmnruntime.MessageSubscription),
		Timers:                 make(map[int64]bpmnruntime.Timer),
		Jobs:                   make(map[int64]bpmnruntime.Job),
		ExecutionTokens:        make(map[int64]bpmnruntime.ExecutionToken),
		FlowElementInstance:    make(map[int64]bpmnruntime.FlowElementInstance),
		Incidents:              make(map[int64]bpmnruntime.Incident),
		ErrorSubscriptions:     make(map[int64]bpmnruntime.ErrorSubscription),
	}
}

func (mem *Storage) Copy() *Storage {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	c := NewStorage()
	for k, v := range mem.DmnResourceDefinitions {
		c.DmnResourceDefinitions[k] = v
	}
	for k, v := range mem.DecisionDefinitions {
		c.DecisionDefinitions[k] = v
	}
	for k, v := range mem.DecisionInstances {
		c.DecisionInstances[k] = v
	}
	for k, v := range mem.ProcessInstances {
		c.ProcessInstances[k] = v
	}
	for k, v := range mem.ProcessDefinitions {
		c.ProcessDefinitions[k] = v
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
	for k, v := range mem.FlowElementInstance {
		c.FlowElementInstance[k] = v
	}
	for k, v := range mem.Incidents {
		c.Incidents[k] = v
	}
	for k, v := range mem.ErrorSubscriptions {
		c.ErrorSubscriptions[k] = v
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

var _ storage.DecisionDefinitionStorageReader = &Storage{}

func (mem *Storage) GetLatestDecisionDefinitionById(ctx context.Context, decisionId string) (dmnruntime.DecisionDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, dec := range mem.DecisionDefinitions {
		if dec.Id == decisionId {
			res = append(res, dec)
		}
	}
	slices.SortFunc(res, func(a, b dmnruntime.DecisionDefinition) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.DecisionDefinition{}, storage.ErrNotFound
}

func (mem *Storage) GetDecisionDefinitionsById(ctx context.Context, decisionId string) ([]dmnruntime.DecisionDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, dec := range mem.DecisionDefinitions {
		if dec.Id == decisionId {
			res = append(res, dec)
		}
	}
	return res, nil
}

func (mem *Storage) GetLatestDecisionDefinitionByIdAndVersionTag(ctx context.Context, decisionId string, versionTag string) (dmnruntime.DecisionDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, dec := range mem.DecisionDefinitions {
		if dec.Id == decisionId && dec.VersionTag == versionTag {
			res = append(res, dec)
		}
	}
	slices.SortFunc(res, func(a, b dmnruntime.DecisionDefinition) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.DecisionDefinition{}, storage.ErrNotFound
}

func (mem *Storage) GetLatestDecisionDefinitionByIdAndDmnResourceDefinitionId(ctx context.Context, decisionId string, dmnResourceDefinitionId string) (dmnruntime.DecisionDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DecisionDefinition, 0)
	for _, dec := range mem.DecisionDefinitions {
		if dec.Id == decisionId && dec.DmnResourceDefinitionId == dmnResourceDefinitionId {
			res = append(res, dec)
		}
	}
	slices.SortFunc(res, func(a, b dmnruntime.DecisionDefinition) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.DecisionDefinition{}, storage.ErrNotFound
}

func (mem *Storage) GetDecisionDefinitionByIdAndDmnResourceDefinitionKey(ctx context.Context, decisionId string, decisionDefinitionKey int64) (dmnruntime.DecisionDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	for _, dd := range mem.DecisionDefinitions {
		if dd.Id == decisionId && dd.DmnResourceDefinitionKey == decisionDefinitionKey {
			return dd, nil
		}
	}
	return dmnruntime.DecisionDefinition{}, storage.ErrNotFound
}

var _ storage.DecisionDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveDecisionDefinition(ctx context.Context, decision dmnruntime.DecisionDefinition) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.DecisionDefinitions[decision.Key] = decision
	return nil
}

var _ storage.DmnResourceDefinitionStorageWriter = &Storage{}

func (mem *Storage) SaveDmnResourceDefinition(ctx context.Context, definition dmnruntime.DmnResourceDefinition) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.DmnResourceDefinitions[definition.Key] = definition
	return nil
}

var _ storage.DecisionInstanceStorageWriter = &Storage{}

func (mem *Storage) SaveDecisionInstance(ctx context.Context, result dmnruntime.DecisionInstance) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.DecisionInstances[result.Key] = result
	return nil
}

var _ storage.DecisionInstanceStorageReader = &Storage{}

func (mem *Storage) FindDecisionInstanceByKey(ctx context.Context, key int64) (dmnruntime.DecisionInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	for _, result := range mem.DecisionInstances {
		if key == result.Key {
			return result, nil
		}
	}
	return dmnruntime.DecisionInstance{}, storage.ErrNotFound
}

var _ storage.DecisionDefinitionStorageWriter = &Storage{}

var _ storage.DmnResourceDefinitionStorageReader = &Storage{}

func (mem *Storage) FindLatestDmnResourceDefinitionById(ctx context.Context, dmnResourceDefinitionId string) (dmnruntime.DmnResourceDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DmnResourceDefinition, 0)
	for _, def := range mem.DmnResourceDefinitions {
		if def.Id != dmnResourceDefinitionId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b dmnruntime.DmnResourceDefinition) int {
		return int(a.Version - b.Version)
	})

	if len(res) > 0 {
		return res[0], nil
	}
	return dmnruntime.DmnResourceDefinition{}, storage.ErrNotFound
}

func (mem *Storage) FindDmnResourceDefinitionByKey(ctx context.Context, dmnResourceDefinitionKey int64) (dmnruntime.DmnResourceDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res, ok := mem.DmnResourceDefinitions[dmnResourceDefinitionKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindDmnResourceDefinitionsById(ctx context.Context, dmnResourceDefinitionId string) ([]dmnruntime.DmnResourceDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]dmnruntime.DmnResourceDefinition, 0)
	for _, def := range mem.DmnResourceDefinitions {
		if def.Id != dmnResourceDefinitionId {
			continue
		}
		res = append(res, def)
	}
	slices.SortFunc(res, func(a, b dmnruntime.DmnResourceDefinition) int {
		return int(a.Version - b.Version)
	})

	return res, nil
}

var _ storage.ProcessDefinitionStorageReader = &Storage{}

func (mem *Storage) FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (bpmnruntime.ProcessDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res, ok := mem.ProcessDefinitions[processDefinitionKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindProcessDefinitionsById(ctx context.Context, processId string) ([]bpmnruntime.ProcessDefinition, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.ProcessDefinitions[definition.Key] = definition
	return nil
}

var _ storage.ProcessInstanceStorageReader = &Storage{}

func (mem *Storage) RefreshProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) (err error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	dbInstance, ok := mem.ProcessInstances[processInstance.ProcessInstance().Key]
	if !ok {
		return storage.ErrNotFound
	}

	switch dbInstance.Type() {
	case bpmnruntime.ProcessTypeDefault:
		processInstance.ProcessInstance().State = dbInstance.ProcessInstance().State
		processInstance.ProcessInstance().VariableHolder = dbInstance.ProcessInstance().VariableHolder
	case bpmnruntime.ProcessTypeMultiInstance:
		multiInstanceInstance, ok := processInstance.(*bpmnruntime.MultiInstanceInstance)
		if !ok {
			return fmt.Errorf("processInstance is not a MultiInstanceInstance")
		}
		parentToken := mem.ExecutionTokens[multiInstanceInstance.ParentProcessExecutionToken.Key]

		multiInstanceInstance.ProcessInstance().State = dbInstance.ProcessInstance().State
		multiInstanceInstance.ProcessInstance().VariableHolder = dbInstance.ProcessInstance().VariableHolder
		multiInstanceInstance.ParentProcessExecutionToken = parentToken
	case bpmnruntime.ProcessTypeSubProcess:
		multiInstanceInstance, ok := processInstance.(*bpmnruntime.SubProcessInstance)
		if !ok {
			return fmt.Errorf("processInstance is not a SubProcessInstance")
		}
		parentToken := mem.ExecutionTokens[multiInstanceInstance.ParentProcessExecutionToken.Key]

		multiInstanceInstance.ProcessInstance().State = dbInstance.ProcessInstance().State
		multiInstanceInstance.ProcessInstance().VariableHolder = dbInstance.ProcessInstance().VariableHolder
		multiInstanceInstance.ParentProcessExecutionToken = parentToken
	case bpmnruntime.ProcessTypeCallActivity:
		multiInstanceInstance, ok := processInstance.(*bpmnruntime.CallActivityInstance)
		if !ok {
			return fmt.Errorf("processInstance is not a CallActivityInstance")
		}
		parentToken := mem.ExecutionTokens[multiInstanceInstance.ParentProcessExecutionToken.Key]

		multiInstanceInstance.ProcessInstance().State = dbInstance.ProcessInstance().State
		multiInstanceInstance.ProcessInstance().VariableHolder = dbInstance.ProcessInstance().VariableHolder
		multiInstanceInstance.ParentProcessExecutionToken = parentToken
	}
	return nil
}

func (mem *Storage) FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (bpmnruntime.ProcessInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res, ok := mem.ProcessInstances[processInstanceKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindProcessInstancesByParentExecutionTokenKey(ctx context.Context, parentExecutionTokenKey int64) ([]bpmnruntime.ProcessInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.ProcessInstance, 0)
	for _, processInstance := range mem.ProcessInstances {
		switch instance := processInstance.(type) {
		case *bpmnruntime.MultiInstanceInstance:
			if instance.ParentProcessExecutionToken.Key == parentExecutionTokenKey {
				res = append(res, processInstance)
			}
		case *bpmnruntime.SubProcessInstance:
			if instance.ParentProcessExecutionToken.Key == parentExecutionTokenKey {
				res = append(res, processInstance)
			}
		case *bpmnruntime.CallActivityInstance:
			if instance.ParentProcessExecutionToken.Key == parentExecutionTokenKey {
				res = append(res, processInstance)
			}
		default:
			continue
		}
	}
	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &Storage{}

func (mem *Storage) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.ProcessInstances[processInstance.ProcessInstance().Key] = processInstance
	return nil
}

var _ storage.TimerStorageReader = &Storage{}

func (mem *Storage) GetTimer(ctx context.Context, timerKey int64) (bpmnruntime.Timer, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	if timer, ok := mem.Timers[timerKey]; ok {
		return timer, nil
	}
	return bpmnruntime.Timer{}, storage.ErrNotFound
}

func (mem *Storage) FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]bpmnruntime.Timer, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.TimerState != bpmnruntime.TimerStateCreated {
			continue
		}
		if timer.Token == nil || timer.Token.Key != tokenKey {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceTimers(ctx context.Context, processInstanceKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.ProcessInstanceKey == nil || *timer.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if timer.TimerState != state {
			continue
		}
		res = append(res, timer)
	}
	return res, nil
}

func (mem *Storage) FindProcessDefinitionTimers(ctx context.Context, processDefinitionKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.Timer, 0)
	for _, timer := range mem.Timers {
		if timer.ProcessDefinitionKey != processDefinitionKey {
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.Timers[timer.GetKey()] = timer
	return nil
}

func (mem *Storage) DeleteProcessDefinitionsTimers(ctx context.Context, processDefinitionKeys []int64) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	keySet := make(map[int64]struct{}, len(processDefinitionKeys))
	for _, k := range processDefinitionKeys {
		keySet[k] = struct{}{}
	}
	for key, timer := range mem.Timers {
		if _, ok := keySet[timer.ProcessDefinitionKey]; !ok {
			continue
		}
		if timer.ProcessInstanceKey != nil || timer.Token != nil {
			continue
		}
		delete(mem.Timers, key)
	}
	return nil
}

var _ storage.JobStorageReader = &Storage{}

func (mem *Storage) FindActiveJobsByType(ctx context.Context, jobType string) ([]bpmnruntime.Job, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.Job, 0)
	for _, job := range mem.Jobs {
		if job.Type != jobType || job.State != bpmnruntime.ActivityStateActive {
			continue
		}
		res = append(res, job)
	}
	return res, nil
}

func (mem *Storage) FindJobByJobKey(ctx context.Context, jobKey int64) (bpmnruntime.Job, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	var res bpmnruntime.Job
	res, ok := mem.Jobs[jobKey]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) GetJobsInStateByTokenKey(ctx context.Context, tokenKey int64, states []bpmnruntime.ActivityState) ([]bpmnruntime.Job, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.Job, 0)
	for _, job := range mem.Jobs {
		if job.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if job.GetState() != bpmnruntime.ActivityStateActive && job.GetState() != bpmnruntime.ActivityStateCompleting && job.GetState() != bpmnruntime.ActivityStateFailed {
			continue
		}
		res = append(res, job)
	}
	return res, nil
}

var _ storage.JobStorageWriter = &Storage{}

func (mem *Storage) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.Jobs[job.GetKey()] = job
	return nil
}

var _ storage.MessageStorageReader = &Storage{}

func (mem *Storage) FindMessageSubscriptionById(ctx context.Context, key int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.MessageSubscription, 0)
	for _, sub := range mem.MessageSubscriptions {
		if sub.Token.Key == tokenKey && sub.State == state {
			res = append(res, sub)
		}
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	var res bpmnruntime.Incident
	res, ok := mem.Incidents[key]
	if !ok {
		return res, storage.ErrNotFound
	}
	return res, nil
}

func (mem *Storage) FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Incident, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.Lock()
	defer mem.mu.Unlock()
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

func (mem *Storage) GetCompletedTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.ExecutionToken, 0)
	for _, tok := range mem.ExecutionTokens {
		if tok.ProcessInstanceKey == processInstanceKey && (tok.State == bpmnruntime.TokenStateCompleted) {
			res = append(res, tok)
		}
	}
	return res, nil
}

func (mem *Storage) GetTokenByKey(ctx context.Context, key int64) (bpmnruntime.ExecutionToken, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	if result, ok := mem.ExecutionTokens[key]; ok {
		return result, nil
	}
	return bpmnruntime.ExecutionToken{}, storage.ErrNotFound
}

// GetTokensForProcessInstance implements storage.TokenStorageReader.
func (mem *Storage) GetActiveTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.ExecutionToken, 0)
	for _, tok := range mem.ExecutionTokens {
		if tok.ProcessInstanceKey == processInstanceKey && (tok.State == bpmnruntime.TokenStateWaiting || tok.State == bpmnruntime.TokenStateFailed || tok.State == bpmnruntime.TokenStateRunning) {
			res = append(res, tok)
		}
	}
	return res, nil
}

func (mem *Storage) GetAllTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
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
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.ExecutionTokens[token.Key] = token
	return nil
}

var _ storage.FlowElementInstanceReader = &Storage{}

func (mem *Storage) GetFlowElementInstancesByTokenKey(ctx context.Context, token bpmnruntime.ExecutionToken) ([]bpmnruntime.FlowElementInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	flowElementInstances := make([]bpmnruntime.FlowElementInstance, 0)
	for _, flowElementInstance := range mem.FlowElementInstance {
		if flowElementInstance.ExecutionTokenKey == token.Key {
			flowElementInstances = append(flowElementInstances, flowElementInstance)
		}
	}
	return flowElementInstances, nil
}

func (mem *Storage) GetFlowElementInstanceCountByProcessInstanceKey(ctx context.Context, processInstanceKey int64) (int64, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	flowElementInstances := make([]bpmnruntime.FlowElementInstance, 0)
	for _, flowElementInstance := range mem.FlowElementInstance {
		if flowElementInstance.ProcessInstanceKey == processInstanceKey {
			flowElementInstances = append(flowElementInstances, flowElementInstance)
		}
	}
	return int64(len(flowElementInstances)), nil
}

func (mem *Storage) GetFlowElementInstancesByProcessInstanceKey(ctx context.Context, processInstanceKey int64, orderByTimeCreated bool) ([]bpmnruntime.FlowElementInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	flowElementInstances := make([]bpmnruntime.FlowElementInstance, 0)
	for _, flowElementInstance := range mem.FlowElementInstance {
		if flowElementInstance.ProcessInstanceKey == processInstanceKey {
			flowElementInstances = append(flowElementInstances, flowElementInstance)
		}
	}
	if orderByTimeCreated {
		sort.Slice(flowElementInstances, func(i, j int) bool {
			if flowElementInstances[i].CreatedAt.Compare(flowElementInstances[j].CreatedAt) > 0 {
				return true
			}
			return false
		})
	}
	return flowElementInstances, nil
}

func (mem *Storage) GetFlowElementInstanceByKey(ctx context.Context, key int64) (bpmnruntime.FlowElementInstance, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	if result, ok := mem.FlowElementInstance[key]; ok {
		return result, nil
	}
	return bpmnruntime.FlowElementInstance{}, storage.ErrNotFound
}

var _ storage.FlowElementInstanceWriter = &Storage{}

func (mem *Storage) SaveFlowElementInstance(ctx context.Context, flowElementInstance bpmnruntime.FlowElementInstance) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.FlowElementInstance[flowElementInstance.Key] = flowElementInstance
	return nil
}

func (mem *Storage) UpdateOutputFlowElementInstance(ctx context.Context, flowElementInstance bpmnruntime.FlowElementInstance) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	elementInstance := mem.FlowElementInstance[flowElementInstance.Key]
	elementInstance.OutputVariables = flowElementInstance.OutputVariables
	mem.FlowElementInstance[flowElementInstance.Key] = elementInstance
	return nil
}

func (mem *Storage) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.Incidents[incident.Key] = incident
	return nil
}

var _ storage.ErrorSubscriptionStorageWriter = &Storage{}

func (mem *Storage) SaveErrorSubscription(ctx context.Context, subscription bpmnruntime.ErrorSubscription) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.ErrorSubscriptions[subscription.GetKey()] = subscription
	return nil
}

var _ storage.ErrorSubscriptionStorageReader = &Storage{}

func (mem *Storage) FindTokenErrorSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ErrorState) ([]bpmnruntime.ErrorSubscription, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.ErrorSubscription, 0)
	for _, errorSubscription := range mem.ErrorSubscriptions {
		if errorSubscription.State != bpmnruntime.ErrorStateCreated {
			continue
		}
		if errorSubscription.Token.Key != tokenKey {
			continue
		}
		res = append(res, errorSubscription)
	}
	return res, nil
}

func (mem *Storage) FindProcessInstanceErrorSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ErrorState) ([]bpmnruntime.ErrorSubscription, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	res := make([]bpmnruntime.ErrorSubscription, 0)
	for _, errorSubscription := range mem.ErrorSubscriptions {
		if errorSubscription.ProcessInstanceKey != processInstanceKey {
			continue
		}
		if errorSubscription.State != state {
			continue
		}
		res = append(res, errorSubscription)
	}
	return res, nil
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

func (b *StorageBatch) DeleteProcessDefinitionsTimers(ctx context.Context, processDefinitionKeys []int64) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.DeleteProcessDefinitionsTimers(ctx, processDefinitionKeys)
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

func (b *StorageBatch) SaveFlowElementInstance(ctx context.Context, historyItem bpmnruntime.FlowElementInstance) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveFlowElementInstance(ctx, historyItem)
	})
	return nil
}

func (b *StorageBatch) UpdateOutputFlowElementInstance(ctx context.Context, flowElementInstance bpmnruntime.FlowElementInstance) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.UpdateOutputFlowElementInstance(ctx, flowElementInstance)
	})
	return nil
}

func (b *StorageBatch) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveIncident(ctx, incident)
	})
	return nil
}

func (b *StorageBatch) SaveErrorSubscription(ctx context.Context, subscription bpmnruntime.ErrorSubscription) error {
	b.stmtToRun = append(b.stmtToRun, func() error {
		return b.db.SaveErrorSubscription(ctx, subscription)
	})
	return nil
}
