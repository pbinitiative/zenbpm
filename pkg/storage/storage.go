package storage

import (
	"context"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// PersistentStorage
// Deprecated: to be replaced by PersistentStorageNew
type PersistentStorage interface {
	// TODO: once the storage interface is considered done, remove Query, Execute, and IsLeader
	Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error)
	Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error)
	IsLeader(ctx context.Context) bool
}

// PersistentStorageNew interface for reading and writing process data into a (persistent) state
// TODO: discuss if we will use structs or interfaces
type PersistentStorageNew interface {
	IsLeader(ctx context.Context) bool

	// FindProcessDefinitionsById return zero or many registered processes with given ID
	// result array is ordered by version number, from 1 (first) and largest version (last)
	FindProcessDefinitionsById(ctx context.Context, processIds ...string) ([]ProcessDefinition, error)

	// FindProcessInstancesByKey return zero or many process instance by given keys
	FindProcessInstancesByKey(ctx context.Context, processInstanceKeys ...int64) ([]ProcessInstance, error)

	// SaveProcessDefinition persists a ProcessDefinition
	// and potentially overwrites prior data stored with the given ProcessKey
	SaveProcessDefinition(ctx context.Context, definition ProcessDefinition) error

	// SaveProcessInstance persists the instance
	// and potentially overwrites prior data stored with given process instance key
	SaveProcessInstance(ctx context.Context, processInstance ProcessInstance) error

	// FindMessageSubscription return zero or many message subscriptions by given params
	FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []string) ([]MessageSubscription, error)

	// SaveMessageSubscription persists the MessageSubscription
	// and potentially overwrites prior data stored with given key
	SaveMessageSubscription(ctx context.Context, subscription MessageSubscription) error

	// FindTimersByState return zero or many Timer by given TimerState
	FindTimersByState(ctx context.Context, state TimerState) ([]Timer, error)

	// SaveTimer persists the Timer
	// and potentially overwrites prior data stored with given key
	SaveTimer(ctx context.Context, timer Timer) error

	// FindJobsByJobKey return zero or many Job by given JobKey
	FindJobsByJobKey(ctx context.Context, jobKey int64) ([]Job, error)

	// FindJobsByState return zero or many Job by given JobState
	FindJobsByState(ctx context.Context, state JobState) ([]Job, error)

	// SaveJob persists the Job
	// and potentially overwrites prior data stored with given JobKey
	SaveJob(ctx context.Context, job Job) error

	FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]Activity, error)

	SaveActivity(ctx context.Context, activity Activity) error
}
