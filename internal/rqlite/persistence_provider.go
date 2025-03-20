package rqlite

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
)

type BpmnEnginePersistence interface {
	FindProcesses(ctx context.Context, processId *string, processKey *int64) ([]sql.ProcessDefinition, error)
	FindProcessInstances(ctx context.Context, processInstanceKey *int64, processDefinitionKey *int64) ([]sql.ProcessInstance, error)
	FindMessageSubscriptions(ctx context.Context, originActivityKey *int64, processInstanceKey *int64, elementId *string, state []string) ([]sql.MessageSubscription, error)
	FindTimers(ctx context.Context, originActivityKey *int64, processInstanceKey *int64, state []string) ([]sql.Timer, error)
	FindJobs(ctx context.Context, elementId *string, jobType *string, processInstanceKey *int64, jobKey *int64, state []string) ([]sql.Job, error)
	FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey *int64) ([]sql.ActivityInstance, error)

	IsLeader() bool

	SaveNewProcess(ctx context.Context, process sql.ProcessDefinition) error
	SaveProcessInstance(ctx context.Context, processInstance sql.ProcessInstance) error
	SaveMessageSubscription(ctx context.Context, subscription sql.MessageSubscription) error
	SaveTimer(ctx context.Context, timer sql.Timer) error
	SaveJob(ctx context.Context, job sql.Job) error
	SaveActivity(ctx context.Context, activity sql.ActivityInstance) error

	FlushTransaction(ctx context.Context) error
}
