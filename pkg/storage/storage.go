package storage

import (
	"context"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// PersistentStorage
// Deprecated: to be replaced by PersistentStorageNew
type PersistentStorage interface {
	// PersistentStorageNew
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

	FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []string) ([]MessageSubscription, error)
	SaveMessageSubscription(ctx context.Context, subscription MessageSubscription) error

	FindTimersByState(ctx context.Context, state TimeState) ([]Timer, error)
	SaveTimer(ctx context.Context, timer Timer) error

	FindJobsByJobKey(ctx context.Context, jobKey int64) ([]Job, error)
	FindJobsByState(ctx context.Context, state JobState) ([]Job, error)
	SaveJob(ctx context.Context, job Job) error

	FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]Activity, error)
	SaveActivity(ctx context.Context, activity Activity) error
}
