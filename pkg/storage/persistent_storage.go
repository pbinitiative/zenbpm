package storage

import (
	"context"

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

	NewBatch() Batch
}

type Batch interface {
	ProcessDefinitionStorageWriter
	ProcessInstanceStorageWriter
	TimerStorageWriter
	JobStorageWriter
	MessageStorageWriter

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
	FindTimersByState(ctx context.Context, processInstanceKey int64, state runtime.TimerState) ([]runtime.Timer, error)

	// TODO: in runtime.Timer there is a comment about not exposing OriginActivity outside of package. How do we want to satisfy this method if we do that?
	FindActivityTimers(ctx context.Context, activityKey int64, state runtime.TimerState) ([]runtime.Timer, error)
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

	FindActivityMessageSubscriptions(ctx context.Context, originActivityKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error)
}

type MessageStorageWriter interface {
	// SaveMessageSubscription persists the MessageSubscription
	// and potentially overwrites prior data stored with given key
	SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error
}

// type ProcessDefinition struct {
// 	BpmnProcessId    string
// 	Version          int32
// 	Key              int64
// 	BpmnData         string
// 	BpmnResourceName string
// 	BpmnChecksum     [16]byte
// }
//
// type ProcessInstance struct {
// 	DefinitionKey int64
// 	Key           int64
// 	Variables     map[string]any
// 	CreatedAt     time.Time
// 	State         runtime.ActivityState
// }
//
// type MessageSubscription struct {
// 	ElementId            string
// 	ElementInstanceKey   int64
// 	ProcessDefinitionKey int64
// 	ProcessInstanceKey   int64
// 	Name                 string
// 	State                runtime.ActivityState
// 	CreatedAt            time.Time
// 	OriginActivityKey    int64
// }
//
// type Timer struct {
// 	ElementId            string
// 	ElementInstanceKey   int64
// 	ProcessDefinitionKey int64
// 	ProcessInstanceKey   int64
// 	TimerState           runtime.TimerState
// 	CreatedAt            time.Time
// 	DueAt                time.Time
// 	OriginActivityKey    int64
// }
//
// type Job struct {
// 	ElementId          string
// 	ElementInstanceKey int64
// 	ProcessInstanceKey int64
// 	Key                int64
// 	State              runtime.ActivityState
// 	CreatedAt          time.Time
// }
