// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package storage

import (
	"context"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
)

// Storage interface for reading and writing process data into a (persistent) state.
// Interface is used by bpmn & dmn engines to interact with state.
//
// Methods that are expected to return exactly one match MUST return ErrNotFound when the result does not exist
type Storage interface {
	ProcessDefinitionStorageReader
	ProcessDefinitionStorageWriter
	ProcessInstanceStorageReader
	ProcessInstanceStorageWriter
	TimerStorageReader
	TimerStorageWriter
	JobStorageReader
	JobStorageWriter
	MessageStorageReader
	MessageStorageWriter
	TokenStorageReader
	TokenStorageWriter
	DecisionStorage
	FlowElementHistoryWriter
	IncidentStorageReader
	IncidentStorageWriter
	MessageSubscriptionPointerStorageWriter
	MessageSubscriptionPointerStorageReader

	GenerateId() int64
	NewBatch() Batch
}

type DecisionStorage interface {
	DecisionDefinitionStorageReader
	DecisionDefinitionStorageWriter
	DecisionStorageReader
	DecisionStorageWriter

	GenerateId() int64
}

// Batch holds an array of operations that need to be run when Flush is called.
type Batch interface {
	ProcessDefinitionStorageWriter
	ProcessInstanceStorageWriter
	TimerStorageWriter
	JobStorageWriter
	MessageStorageWriter
	TokenStorageWriter
	FlowElementHistoryWriter
	IncidentStorageWriter

	// Close will flush the batch into the storage and prepares the batch for new statements
	Flush(ctx context.Context) error
	// AddFlushSuccessAction registers a function f that will be called after a successful flush has been performed
	AddFlushSuccessAction(ctx context.Context, f func())
}

type DecisionStorageReader interface {
	GetLatestDecisionById(ctx context.Context, decisionId string) (dmnruntime.Decision, error)

	GetDecisionsById(ctx context.Context, decisionId string) ([]dmnruntime.Decision, error)

	GetLatestDecisionByIdAndVersionTag(ctx context.Context, decisionId string, versionTag string) (dmnruntime.Decision, error)

	GetLatestDecisionByIdAndDecisionDefinitionId(ctx context.Context, decisionId string, decisionDefinitionId string) (dmnruntime.Decision, error)

	GetDecisionByIdAndDecisionDefinitionKey(ctx context.Context, decisionId string, decisionDefinitionKey int64) (dmnruntime.Decision, error)
}

type DecisionStorageWriter interface {
	SaveDecision(ctx context.Context, decision dmnruntime.Decision) error
}

type DecisionDefinitionStorageReader interface {
	FindLatestDecisionDefinitionById(ctx context.Context, decisionDefinitionId string) (dmnruntime.DecisionDefinition, error)

	FindDecisionDefinitionByKey(ctx context.Context, decisionDefinitionKey int64) (dmnruntime.DecisionDefinition, error)

	// FindDecisionDefinitionsById return zero or many registered DecisionDefinitions with given ID
	// result array is ordered by version number desc
	FindDecisionDefinitionsById(ctx context.Context, decisionDefinitionId string) ([]dmnruntime.DecisionDefinition, error)
}

type DecisionDefinitionStorageWriter interface {
	// SaveDecisionDefinition persists a DecisionDefinition
	// and potentially overwrites prior data stored with the given DecisionKey
	SaveDecisionDefinition(ctx context.Context, definition dmnruntime.DecisionDefinition) error
}

type ProcessDefinitionStorageReader interface {
	FindLatestProcessDefinitionById(ctx context.Context, ProcessDefinitionId string) (bpmnruntime.ProcessDefinition, error)

	FindProcessDefinitionByKey(ctx context.Context, ProcessDefinitionKey int64) (bpmnruntime.ProcessDefinition, error)

	// FindProcessDefinitionsById return zero or many registered processes with given ID
	// result array is ordered by version number, from 1 (first) and largest version (last)
	FindProcessDefinitionsById(ctx context.Context, processId string) ([]bpmnruntime.ProcessDefinition, error)
}

type ProcessDefinitionStorageWriter interface {
	// SaveProcessDefinition persists a ProcessDefinition
	// and potentially overwrites prior data stored with the given ProcessKey
	SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error
}

type ProcessInstanceStorageReader interface {
	FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (bpmnruntime.ProcessInstance, error)
}

type ProcessInstanceStorageWriter interface {
	// SaveProcessInstance persists the instance
	// and potentially overwrites prior data stored with given process instance key
	SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error
}

type TimerStorageReader interface {
	// FindTimersTo returns a list of timers that have dueDate before end and are in CREATED state
	FindTimersTo(ctx context.Context, end time.Time) ([]bpmnruntime.Timer, error)

	FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]bpmnruntime.Timer, error)
}

type TimerStorageWriter interface {
	// SaveTimer persists the Timer
	// and potentially overwrites prior data stored with given key
	SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error
}

type JobStorageReader interface {
	// FindPendingProcessInstanceJobs returns jobs for process instance that are in Active or Completing state
	FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Job, error)

	FindJobByJobKey(ctx context.Context, jobKey int64) (bpmnruntime.Job, error)

	FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (bpmnruntime.Job, error)

	FindActiveJobsByType(ctx context.Context, jobType string) ([]bpmnruntime.Job, error)
}

type JobStorageWriter interface {
	// SaveJob persists the Job
	// and potentially overwrites prior data stored with given JobKey
	SaveJob(ctx context.Context, job bpmnruntime.Job) error
}

type MessageSubscriptionPointerStorageWriter interface {
	SaveMessageSubscriptionPointer(ctx context.Context, subscription bpmnruntime.MessageSubscriptionPointer) error

	TerminateMessageSubscriptionPointers(ctx context.Context, executionTokenKey int64) error

	TerminateMessageSubscriptionPointersForExecution(ctx context.Context, messageSubscriptions []bpmnruntime.MessageSubscription, executionTokenKey int64)
}

type MessageSubscriptionPointerStorageReader interface {
	FindActiveMessageSubscriptionPointer(ctx context.Context, name string, correlationKey string) (bpmnruntime.MessageSubscriptionPointer, error)
}

type MessageStorageReader interface {
	FindMessageSubscriptionById(ctx context.Context, key int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error)

	// FindProcessInstanceMessageSubscriptions return message subscriptions for process instance that are in Active or Ready state
	FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error)

	FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error)
}

type MessageStorageWriter interface {
	// SaveMessageSubscription persists the MessageSubscription
	// and potentially overwrites prior data stored with given key
	SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error
}

type TokenStorageReader interface {
	GetRunningTokens(ctx context.Context) ([]bpmnruntime.ExecutionToken, error)
	// TODO: update this so it doesn't have to return all the tokens
	GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error)
}

type TokenStorageWriter interface {
	SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error
}

type FlowElementHistoryWriter interface {
	SaveFlowElementHistory(ctx context.Context, item bpmnruntime.FlowElementHistoryItem) error
}

type IncidentStorageReader interface {
	FindIncidentByKey(ctx context.Context, key int64) (bpmnruntime.Incident, error)
	FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Incident, error)
}

type IncidentStorageWriter interface {
	SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error
}
