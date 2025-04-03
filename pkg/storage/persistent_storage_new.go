package storage

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// PersistentStorageNew interface for reading and writing process data into a (persistent) state
// interface is used by bpmn & dmn engines to interact with state
type PersistentStorageNew interface {
	// FindProcessDefinitionById return registered processes with given ID
	FindProcessDefinitionById(ctx context.Context, processDefinitionId string) (runtime.ProcessDefinition, error)

	// FindProcessDefinitionByKey return registered processes with given key
	FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (runtime.ProcessDefinition, error)

	// TODO: remove?
	// FindProcessDefinitionsById return zero or many registered processes with given ID
	// result array is ordered by version number, from 1 (first) and largest version (last)
	FindProcessDefinitionsById(ctx context.Context, processIds ...string) ([]runtime.ProcessDefinition, error)

	// FindProcessInstanceByKey return process instance by given key
	FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (runtime.ProcessInstance, error)

	// FindPendingProcessInstanceJobs returns jobs for process instance that are in Active or Completing state
	FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error)

	// FindProcessInstanceMessageSubscription return message subscriptions for process instance that are in Active or Ready state
	FindProcessInstanceMessageSubscription(ctx context.Context, processInstanceKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error)

	// TODO: remove?
	// FindProcessInstancesByKey return zero or many process instance by given keys
	FindProcessInstancesByKey(ctx context.Context, processInstanceKeys ...int64) ([]runtime.ProcessInstance, error)

	// SaveProcessDefinition persists a ProcessDefinition
	// and potentially overwrites prior data stored with the given ProcessKey
	SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error

	// SaveProcessInstance persists the instance
	// and potentially overwrites prior data stored with given process instance key
	SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error

	// FindMessageSubscription return zero or many message subscriptions by given params
	FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []runtime.ActivityState) ([]runtime.MessageSubscription, error)

	// SaveMessageSubscription persists the MessageSubscription
	// and potentially overwrites prior data stored with given key
	SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error

	// FindTimersByState return zero or many Timer by given TimerState
	FindTimersByState(ctx context.Context, state runtime.TimerState) ([]runtime.Timer, error)

	// SaveTimer persists the Timer
	// and potentially overwrites prior data stored with given key
	SaveTimer(ctx context.Context, timer runtime.Timer) error

	// FindJobsByJobKey return zero or many Job by given JobKey
	FindJobsByJobKey(ctx context.Context, jobKey int64) ([]runtime.Job, error)

	// FindJobsByState return zero or many Job by given JobState
	FindJobsByState(ctx context.Context, state runtime.ActivityState) ([]runtime.Job, error)

	// SaveJob persists the Job
	// and potentially overwrites prior data stored with given JobKey
	SaveJob(ctx context.Context, job runtime.Job) error

	FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]runtime.Activity, error)

	SaveActivity(ctx context.Context, activity runtime.Activity) error
}
