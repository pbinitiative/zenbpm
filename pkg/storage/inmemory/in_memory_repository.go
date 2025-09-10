// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package inmemory

import (
	"context"
	"errors"
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
	Decision                    map[string]map[int64]dmnruntime.Decision
	DecisionDefinitions         map[int64]dmnruntime.DecisionDefinition
	ProcessDefinitions          map[int64]bpmnruntime.ProcessDefinition
	ProcessInstances            map[int64]bpmnruntime.ProcessInstance
	MessageSubscriptionPointers map[int64]bpmnruntime.MessageSubscriptionPointer
	MessageSubscriptions        map[int64]bpmnruntime.MessageSubscription
	Timers                      map[int64]bpmnruntime.Timer
	Jobs                        map[int64]bpmnruntime.Job
	ExecutionTokens             map[int64]bpmnruntime.ExecutionToken
	FlowElementHistory          map[int64]bpmnruntime.FlowElementHistoryItem
	Incidents                   map[int64]bpmnruntime.Incident
}

func (mem *Storage) GenerateId() int64 {
	return rand.Int63()
}

func NewStorage() *Storage {
	return &Storage{
		Decision:                    make(map[string]map[int64]dmnruntime.Decision),
		DecisionDefinitions:         make(map[int64]dmnruntime.DecisionDefinition),
		ProcessDefinitions:          make(map[int64]bpmnruntime.ProcessDefinition),
		ProcessInstances:            make(map[int64]bpmnruntime.ProcessInstance),
		MessageSubscriptionPointers: make(map[int64]bpmnruntime.MessageSubscriptionPointer),
		MessageSubscriptions:        make(map[int64]bpmnruntime.MessageSubscription),
		Timers:                      make(map[int64]bpmnruntime.Timer),
		Jobs:                        make(map[int64]bpmnruntime.Job),
		ExecutionTokens:             make(map[int64]bpmnruntime.ExecutionToken),
		FlowElementHistory:          make(map[int64]bpmnruntime.FlowElementHistoryItem),
		Incidents:                   make(map[int64]bpmnruntime.Incident),
	}
}

var _ storage.Storage = &Storage{}

func (mem *Storage) NewBatch() storage.Batch {
	return &StorageBatch{
		db:                  mem,
		stmtToRun:           make([]func() error, 0, 10),
		flushSuccessActions: make([]func(), 0, 5),
	}
}

var _ storage.MessageSubscriptionPointerStorageWriter = &Storage{}

func (mem *Storage) SaveMessageSubscriptionPointer(ctx context.Context, subscription bpmnruntime.MessageSubscriptionPointer) error {
	_, ok := mem.MessageSubscriptionPointers[subscription.Key]
	if ok {
		mem.MessageSubscriptionPointers[subscription.Key] = subscription
		return nil
	}

	for _, sub := range mem.MessageSubscriptionPointers {
		if subscription.CorrelationKey == sub.CorrelationKey &&
			subscription.Name == sub.Name &&
			sub.State == bpmnruntime.MessageSubscriptionActive {
			return storage.ErrUniqueConstraint
		}
	}
	if subscription.Key == 0 {
		subscription.Key = mem.GenerateId()
	}
	mem.MessageSubscriptionPointers[subscription.Key] = subscription

	return nil
}

var _ storage.MessageSubscriptionPointerStorageReader = &Storage{}

func (mem *Storage) TerminateMessageSubscriptionPointers(ctx context.Context, executionTokenKey int64) error {
	for key, subscription := range mem.MessageSubscriptionPointers {
		if subscription.ExecutionTokenKey == executionTokenKey && subscription.State == bpmnruntime.MessageSubscriptionActive {
			subscription = mem.MessageSubscriptionPointers[key]
			subscription.State = bpmnruntime.MessageSubscriptionComplete
		}
	}
	return nil
}

func (mem *Storage) TerminateMessageSubscriptionPointersForExecution(ctx context.Context, messageSubscriptions []bpmnruntime.MessageSubscription, executionTokenKey int64) {
	for _, subscription := range messageSubscriptions {
		sub := mem.MessageSubscriptionPointers[subscription.Key]
		sub.State = bpmnruntime.MessageSubscriptionComplete
	}
}

func (mem *Storage) FindActiveMessageSubscriptionPointer(ctx context.Context, name string, correlationKey string) (bpmnruntime.MessageSubscriptionPointer, error) {
	for _, subscription := range mem.MessageSubscriptionPointers {
		if subscription.Name == name && subscription.CorrelationKey == correlationKey && subscription.State == bpmnruntime.MessageSubscriptionActive {
			return subscription, nil
		}
	}
	return bpmnruntime.MessageSubscriptionPointer{}, storage.ErrNotFound
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

func (mem *Storage) FindMessageSubscriptionById(ctx context.Context, key int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error) {
	var res bpmnruntime.MessageSubscription
	res, ok := mem.MessageSubscriptions[key]
	if !ok {
		return res, storage.ErrNotFound
	}
	if res.MessageState == state {
		return res, nil
	}
	return res, storage.ErrNotFound
}

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
	db                  *Storage
	stmtToRun           []func() error
	flushSuccessActions []func()
}

var _ storage.Batch = &StorageBatch{}

func (b *StorageBatch) AddFlushSuccessAction(ctx context.Context, f func()) {
	b.flushSuccessActions = append(b.flushSuccessActions, f)
}

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
	for _, action := range b.flushSuccessActions {
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
