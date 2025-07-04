package storage

import (
	"context"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// PersistentStorageNew interface for reading and writing process data into a (persistent) state.
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

	GenerateId() int64
	NewBatch() Batch
}

type Batch interface {
	ProcessDefinitionStorageWriter
	ProcessInstanceStorageWriter
	TimerStorageWriter
	JobStorageWriter
	MessageStorageWriter
	TokenStorageWriter

	// Close will flush the batch into the storage and prepares the batch for new statements
	Flush(ctx context.Context) error
}

type ProcessDefinitionStorageReader interface {
	FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (runtime.ProcessDefinition, error)

	FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (runtime.ProcessDefinition, error)

	// FindProcessDefinitionsById return zero or many registered processes with given ID
	// result array is ordered by version number, from 1 (first) and largest version (last)
	FindProcessDefinitionsById(ctx context.Context, processId string) ([]runtime.ProcessDefinition, error)
}

type ProcessDefinitionStorageWriter interface {
	// SaveProcessDefinition persists a ProcessDefinition
	// and potentially overwrites prior data stored with the given ProcessKey
	SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error
}

type ProcessInstanceStorageReader interface {
	FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (runtime.ProcessInstance, error)
}

type ProcessInstanceStorageWriter interface {
	// SaveProcessInstance persists the instance
	// and potentially overwrites prior data stored with given process instance key
	SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error
}

type TimerStorageReader interface {
	// FindTimersTo returns a list of timers that have dueDate before end and are in CREATED state
	FindTimersTo(ctx context.Context, end time.Time) ([]runtime.Timer, error)

	FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]runtime.Timer, error)
}

type TimerStorageWriter interface {
	// SaveTimer persists the Timer
	// and potentially overwrites prior data stored with given key
	SaveTimer(ctx context.Context, timer runtime.Timer) error
}

type JobStorageReader interface {
	// FindPendingProcessInstanceJobs returns jobs for process instance that are in Active or Completing state
	FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error)

	FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error)

	FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (runtime.Job, error)

	FindActiveJobsByType(ctx context.Context, jobType string) ([]runtime.Job, error)
}

type JobStorageWriter interface {
	// SaveJob persists the Job
	// and potentially overwrites prior data stored with given JobKey
	SaveJob(ctx context.Context, job runtime.Job) error
}

type MessageStorageReader interface {
	// FindProcessInstanceMessageSubscription return message subscriptions for process instance that are in Active or Ready state
	FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error)

	FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error)
}

type MessageStorageWriter interface {
	// SaveMessageSubscription persists the MessageSubscription
	// and potentially overwrites prior data stored with given key
	SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error
}

type TokenStorageReader interface {
	GetRunningTokens(ctx context.Context) ([]runtime.ExecutionToken, error)
	// TODO: update this so it doesn't have to return all the tokens
	GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]runtime.ExecutionToken, error)
}

type TokenStorageWriter interface {
	SaveToken(ctx context.Context, token runtime.ExecutionToken) error
}
