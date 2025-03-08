package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	rqlite "github.com/pbinitiative/zenbpm/pkg/bpmn/persistence/rqlite"
)

type BpmnEnginePersistenceService interface {
	FindProcessById(processId string) *ProcessInfo
	FindProcessesById(processId string) []*ProcessInfo
	FindProcessByKey(processKey int64) *ProcessInfo
	FindProcessInstanceByKey(processInstanceKey int64) *processInstanceInfo
	FindProcessInstances(processInstanceKey int64) []*processInstanceInfo

	FindMessageSubscription(originActivityKey int64, processInstance *processInstanceInfo, elementId string, state ...bpmn20.ActivityState) []*MessageSubscription
	FindTimers(originActivityKey int64, processInstanceKey int64, state ...TimerState) []*Timer
	FindJobs(elementId string, processInstance *processInstanceInfo, jobKey int64, state ...bpmn20.ActivityState) []*job
	FindJobByKey(jobKey int64) *job
	PersistNewProcess(process *ProcessInfo) error
	PersistProcessInstance(processInstance *processInstanceInfo) error
	PersistNewMessageSubscription(subscription *MessageSubscription) error
	PersistNewTimer(timer *Timer) error
	PersistJob(job *job) error

	GetPersistence() *rqlite.BpmnEnginePersistenceRqlite
}
