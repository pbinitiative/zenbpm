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
	FlowElementInstanceReader
	FlowElementInstanceWriter
	IncidentStorageReader
	IncidentStorageWriter

	GenerateId() int64
	NewBatch() Batch
}

type DecisionStorage interface {
	DmnResourceDefinitionStorageReader
	DmnResourceDefinitionStorageWriter
	DecisionDefinitionStorageReader
	DecisionDefinitionStorageWriter
	DecisionInstanceStorageReader
	DecisionInstanceStorageWriter

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
	FlowElementInstanceWriter
	IncidentStorageWriter

	// Close will flush the batch into the storage and prepares the batch for new statements
	Flush(ctx context.Context) error

	// AddPreFlushAction registers a function f that will be called after a before flush. If error is returned flush is not performed
	AddPreFlushAction(ctx context.Context, f func() error)

	// AddPostFlushAction registers a function f that will be called after a successful flush has been performed
	AddPostFlushAction(ctx context.Context, f func())
}

type DecisionDefinitionStorageReader interface {
	GetLatestDecisionDefinitionById(ctx context.Context, decisionId string) (dmnruntime.DecisionDefinition, error)

	GetDecisionDefinitionsById(ctx context.Context, decisionId string) ([]dmnruntime.DecisionDefinition, error)

	GetLatestDecisionDefinitionByIdAndVersionTag(ctx context.Context, decisionId string, versionTag string) (dmnruntime.DecisionDefinition, error)

	GetLatestDecisionDefinitionByIdAndDmnResourceDefinitionId(ctx context.Context, decisionId string, dmnResourceDefinitionId string) (dmnruntime.DecisionDefinition, error)

	GetDecisionDefinitionByIdAndDmnResourceDefinitionKey(ctx context.Context, decisionId string, decisionDefinitionKey int64) (dmnruntime.DecisionDefinition, error)
}

type DecisionDefinitionStorageWriter interface {
	SaveDecisionDefinition(ctx context.Context, decision dmnruntime.DecisionDefinition) error
}

type DmnResourceDefinitionStorageReader interface {
	FindLatestDmnResourceDefinitionById(ctx context.Context, dmnResourceDefinitionId string) (dmnruntime.DmnResourceDefinition, error)

	FindDmnResourceDefinitionByKey(ctx context.Context, decisionDefinitionKey int64) (dmnruntime.DmnResourceDefinition, error)

	// FindDmnResourceDefinitionsById return zero or many registered DmnResourceDefinitions with given ID
	// result array is ordered by version number desc
	FindDmnResourceDefinitionsById(ctx context.Context, dmnResourceDefinitionId string) ([]dmnruntime.DmnResourceDefinition, error)
}

type DmnResourceDefinitionStorageWriter interface {
	// SaveDmnResourceDefinition persists a DmnResourceDefinition
	// and potentially overwrites prior data stored with the given DecisionKey
	SaveDmnResourceDefinition(ctx context.Context, definition dmnruntime.DmnResourceDefinition) error
}

type DecisionInstanceStorageWriter interface {
	SaveDecisionInstance(ctx context.Context, result dmnruntime.DecisionInstance) error
}

type DecisionInstanceStorageReader interface {
	FindDecisionInstanceByKey(ctx context.Context, key int64) (dmnruntime.DecisionInstance, error)
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
	FindProcessInstanceByParentExecutionTokenKey(ctx context.Context, parentExecutionTokenKey int64) ([]bpmnruntime.ProcessInstance, error)
	RefreshProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) (err error)
}

type ProcessInstanceStorageWriter interface {
	// SaveProcessInstance persists the instance
	// and potentially overwrites prior data stored with given process instance key
	SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error
}

type TimerStorageReader interface {
	GetTimer(ctx context.Context, timerKey int64) (bpmnruntime.Timer, error)

	// FindTimersTo returns a list of timers that have dueDate before end and are in CREATED state
	FindTimersTo(ctx context.Context, end time.Time) ([]bpmnruntime.Timer, error)

	FindProcessInstanceTimers(ctx context.Context, processInstanceKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error)

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

	FindTokenJobsInState(ctx context.Context, tokenKey int64, states []bpmnruntime.ActivityState) ([]bpmnruntime.Job, error)

	FindJobByJobKey(ctx context.Context, jobKey int64) (bpmnruntime.Job, error)

	FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (bpmnruntime.Job, error)

	FindActiveJobsByType(ctx context.Context, jobType string) ([]bpmnruntime.Job, error)
}

type JobStorageWriter interface {
	// SaveJob persists the Job
	// and potentially overwrites prior data stored with given JobKey
	SaveJob(ctx context.Context, job bpmnruntime.Job) error
}

type MessageStorageReader interface {
	FindMessageSubscriptionById(ctx context.Context, key int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error)

	FindActiveMessageSubscriptionKey(ctx context.Context, name string, correlationKey string) (int64, error)

	// FindProcessInstanceMessageSubscriptions return message subscriptions for process instance that are in Active or Ready state
	FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error)

	FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error)
}

type MessageStorageWriter interface {
	// SaveMessageSubscription persists the MessageSubscription
	// returns an error if there is already an active conflicting message present.
	SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error
}

type TokenStorageReader interface {
	GetRunningTokens(ctx context.Context) ([]bpmnruntime.ExecutionToken, error)
	GetActiveTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error)
	GetCompletedTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error)
	GetAllTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error)
	GetTokenByKey(ctx context.Context, key int64) (bpmnruntime.ExecutionToken, error)
}

type TokenStorageWriter interface {
	SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error
}

type FlowElementInstanceReader interface {
	GetFlowElementInstanceCountByProcessInstanceKey(ctx context.Context, processInstanceKey int64) (int64, error)
	GetFlowElementInstancesByProcessInstanceKey(ctx context.Context, processInstanceKey int64, orderByTimeCreated bool) ([]bpmnruntime.FlowElementInstance, error)
	GetFlowElementInstanceByKey(ctx context.Context, key int64) (bpmnruntime.FlowElementInstance, error)
}

type FlowElementInstanceWriter interface {
	SaveFlowElementInstance(ctx context.Context, flowElementInstance bpmnruntime.FlowElementInstance) error
	UpdateOutputFlowElementInstance(ctx context.Context, flowElementInstance bpmnruntime.FlowElementInstance) error
}

type IncidentStorageReader interface {
	FindIncidentByKey(ctx context.Context, key int64) (bpmnruntime.Incident, error)
	FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Incident, error)
	FindIncidentsByExecutionTokenKey(ctx context.Context, executionTokenKey int64) ([]bpmnruntime.Incident, error)
}

type IncidentStorageWriter interface {
	SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error
}
