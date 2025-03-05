package rqlite

import (
	"context"
)

type BpmnEnginePersistence interface {
	FindProcesses(ctx context.Context, processId string, processKey int64) ([]ProcessDefinition, error)
	FindProcessInstances(ctx context.Context, processInstanceKey int64, processDefinitionKey int64) ([]ProcessInstance, error)
	FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []string) ([]MessageSubscription, error)
	FindTimers(ctx context.Context, originActivityKey int64, processInstanceKey int64, state []string) ([]Timer, error)
	FindJobs(ctx context.Context, elementId string, processInstanceKey int64, jobKey int64, state []string) ([]Job, error)
	FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]ActivityInstance, error)

	IsLeader() bool

	SaveNewProcess(ctx context.Context, process *ProcessDefinition) error
	SaveProcessInstance(ctx context.Context, processInstance *ProcessInstance) error
	SaveMessageSubscription(ctx context.Context, subscription *MessageSubscription) error
	SaveTimer(ctx context.Context, timer *Timer) error
	SaveJob(ctx context.Context, job *Job) error
	SaveActivity(ctx context.Context, activity ActivityInstance) error
}
