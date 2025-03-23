package bpmn

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	rqlite "github.com/pbinitiative/zenbpm/internal/rqlite"
)

type BpmnEnginePersistenceService interface {
	FindProcessById(processId string) *runtime.ProcessDefinition
	FindProcessesById(processId string) []*runtime.ProcessDefinition
	FindProcessByKey(processKey int64) *runtime.ProcessDefinition
	FindProcessInstanceByKey(processInstanceKey int64) *processInstanceInfo
	FindProcessInstances(processInstanceKey int64) []*processInstanceInfo

	FindMessageSubscription(originActivityKey *int64, processInstance *processInstanceInfo, elementId *string, state ...runtime.ActivityState) []*runtime.MessageSubscription
	FindTimers(originActivityKey *int64, processInstanceKey *int64, state ...runtime.TimerState) []*runtime.Timer
	FindJobs(elementId *string, jobType *string, processInstance *processInstanceInfo, jobKey *int64, state ...runtime.ActivityState) []*runtime.Job
	FindJobByKey(jobKey int64) *runtime.Job
	PersistNewProcess(ctx context.Context, process *runtime.ProcessDefinition) error
	PersistProcessInstance(ctx context.Context, processInstance *processInstanceInfo) error
	PersistNewMessageSubscription(ctx context.Context, subscription *runtime.MessageSubscription) error
	PersistNewTimer(ctx context.Context, timer *runtime.Timer) error
	PersistJob(ctx context.Context, job *runtime.Job) error

	GetPersistence() *rqlite.PersistenceRqlite
}
