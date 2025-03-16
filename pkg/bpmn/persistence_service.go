package bpmn

import (
	"context"

	rqlite "github.com/pbinitiative/zenbpm/internal/rqlite"
)

type BpmnEnginePersistenceService interface {
	FindProcessById(processId string) *ProcessInfo
	FindProcessesById(processId string) []*ProcessInfo
	FindProcessByKey(processKey int64) *ProcessInfo
	FindProcessInstanceByKey(processInstanceKey int64) *processInstanceInfo
	FindProcessInstances(processInstanceKey int64) []*processInstanceInfo

	FindMessageSubscription(originActivityKey *int64, processInstance *processInstanceInfo, elementId *string, state ...ActivityState) []*MessageSubscription
	FindTimers(originActivityKey *int64, processInstanceKey *int64, state ...TimerState) []*Timer
	FindJobs(elementId *string, jobType *string, processInstance *processInstanceInfo, jobKey *int64, state ...ActivityState) []*job
	FindJobByKey(jobKey int64) *job
	PersistNewProcess(ctx context.Context, process *ProcessInfo) error
	PersistProcessInstance(ctx context.Context, processInstance *processInstanceInfo) error
	PersistNewMessageSubscription(ctx context.Context, subscription *MessageSubscription) error
	PersistNewTimer(ctx context.Context, timer *Timer) error
	PersistJob(ctx context.Context, job *job) error

	GetPersistence() *rqlite.PersistenceRqlite
}
