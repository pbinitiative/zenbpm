package storage

import (
	"context"
	"github.com/rqlite/rqlite/v8/command/proto"
)

type PersistentStorage interface {
	// PersistentStorageApi
	// TODO: once the storage interface is considered done, remove Query, Execute, and IsLeader
	Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error)
	Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error)
	IsLeader(ctx context.Context) bool
}

// PersistentStorageApi interface for reading and writing process data into a (persistent) state
type PersistentStorageApi interface {

	// FindProcessDefinitionsById returns all registered processes with given ID
	// result array is ordered by version number, from 1 (first) and largest version (last)
	FindProcessDefinitionsById(ctx context.Context, processIds ...string) ([]ProcessDefinition, error)

	// SaveProcessDefinition persists a ProcessDefinition and potentially overwrites prior data stored with the given ProcessKey
	SaveProcessDefinition(ctx context.Context, definition ProcessDefinition) error

	// Examples of how these interfaces should look like based on BpmnEnginePersistence.

	// ReadProcessInstances(ctx context.Context, processInstanceKeys ...int64) ([]ProcessInstance, error)
	// ReadMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []string) ([]MessageSubscription, error)
	// ReadTimers(ctx context.Context, state TimeState) ([]Timer, error)
	// ReadJobs(ctx context.Context, state JobState) ([]Job, error)
	// ReadActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]Activity, error)

	// WriteProcessInstance(ctx context.Context, processInstance ProcessInstance) error
	// WriteMessageSubscription(ctx context.Context, subscription MessageSubscription) error
	// WriteTimer(ctx context.Context, timer Timer) error
	// WriteJob(ctx context.Context, job Job) error
	// WriteActivity(ctx context.Context, activity Activity) error
}
