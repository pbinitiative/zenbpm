package bpmn

import (
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// ActivatedJob is a struct to provide information for registered task handler
type activatedJob struct {
	processInstanceInfo      runtime.ProcessInstance
	completeHandler          func()
	failHandler              func(reason string)
	key                      int64
	processInstanceKey       int64
	bpmnProcessId            string
	processDefinitionVersion int32
	processDefinitionKey     int64
	elementId                string
	createdAt                time.Time
	localVariables           map[string]interface{}
	outputVariables          map[string]interface{}
}

// ActivatedJob represents an abstraction for the activated job
// don't forget to call Fail or Complete when your task worker job is complete or not.
type ActivatedJob interface {
	// Key the key, a unique identifier for the job
	Key() int64

	// ProcessInstanceKey the job's process instance key
	ProcessInstanceKey() int64

	// BpmnProcessId Retrieve id of the job process definition
	BpmnProcessId() string

	// ProcessDefinitionVersion Retrieve version of the job process definition
	ProcessDefinitionVersion() int32

	// ProcessDefinitionKey Retrieve key of the job process definition
	ProcessDefinitionKey() int64

	// ElementId Get element id of the job
	ElementId() string

	// Variable from the process instance's variable context
	Variable(key string) interface{}

	// SetVariable in the variables context of the given process instance
	SetOutputVariable(key string, value interface{})

	GetLocalVariables() map[string]interface{}

	GetOutputVariables() map[string]interface{}

	// InstanceKey get instance key from ProcessInfo
	InstanceKey() int64

	// CreatedAt when the job was created
	CreatedAt() time.Time

	// Fail does set the State the worker missed completing the job
	// Fail and Complete mutual exclude each other
	Fail(reason string)

	// Complete does set the State the worker successfully completing the job
	// Fail and Complete mutual exclude each other
	Complete()
}

// CreatedAt implements ActivatedJob
func (aj *activatedJob) CreatedAt() time.Time {
	return aj.createdAt
}

// InstanceKey implements ActivatedJob
func (aj *activatedJob) InstanceKey() int64 {
	return aj.processInstanceInfo.ProcessInstance().GetInstanceKey()
}

// ElementId implements ActivatedJob
func (aj *activatedJob) ElementId() string {
	return aj.elementId
}

// Key implements ActivatedJob
func (aj *activatedJob) Key() int64 {
	return aj.key
}

// BpmnProcessId implements ActivatedJob
func (aj *activatedJob) BpmnProcessId() string {
	return aj.bpmnProcessId
}

// ProcessDefinitionKey implements ActivatedJob
func (aj *activatedJob) ProcessDefinitionKey() int64 {
	return aj.processDefinitionKey
}

// ProcessDefinitionVersion implements ActivatedJob
func (aj *activatedJob) ProcessDefinitionVersion() int32 {
	return aj.processDefinitionVersion
}

// ProcessInstanceKey implements ActivatedJob
func (aj *activatedJob) ProcessInstanceKey() int64 {
	return aj.processInstanceKey
}

// Variable implements ActivatedJob
func (aj *activatedJob) Variable(key string) interface{} {
	return aj.localVariables[key]
}

// SetOutputVariable implements ActivatedJob
func (aj *activatedJob) SetOutputVariable(key string, value interface{}) {
	aj.outputVariables[key] = value
}

func (aj *activatedJob) GetLocalVariables() map[string]interface{} {
	return aj.localVariables
}

func (aj *activatedJob) GetOutputVariables() map[string]interface{} {
	return aj.outputVariables
}

// Fail implements ActivatedJob
func (aj *activatedJob) Fail(reason string) {
	aj.failHandler(reason)
}

// Complete implements ActivatedJob
func (aj *activatedJob) Complete() {
	aj.completeHandler()
}
