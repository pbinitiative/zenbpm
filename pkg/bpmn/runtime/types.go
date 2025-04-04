package runtime

import (
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type ProcessDefinition struct {
	BpmnProcessId    string              // The ID as defined in the BPMN file
	Version          int32               // A version of the process, default=1, incremented, when another process with the same ID is loaded
	ProcessKey       int64               // The engines key for this given process with version
	Definitions      bpmn20.TDefinitions // parsed file content
	BpmnData         string              // the raw source data, compressed and encoded via ascii85
	BpmnResourceName string              // some name for the resource
	BpmnChecksum     [16]byte            // internal checksum to identify different versions
}

type CatchEvent struct {
	Name       string
	CaughtAt   time.Time
	IsConsumed bool
	Variables  map[string]interface{}
}

// type ProcessInstance interface {
// 	GetProcessInfo() *ProcessDefinition
// 	GetInstanceKey() int64
//
// 	// GetVariable from the process instance's variable context
// 	GetVariable(key string) interface{}
//
// 	// SetVariable to the process instance's variable context
// 	SetVariable(key string, value interface{})
//
// 	GetCreatedAt() time.Time
// 	GetState() ActivityState
// }

type ProcessInstance struct {
	Definition     *ProcessDefinition `json:"-"`
	InstanceKey    int64              `json:"ik"`
	VariableHolder VariableHolder     `json:"vh,omitempty"`
	CreatedAt      time.Time          `json:"c"`
	State          ActivityState      `json:"s"`
	CaughtEvents   []CatchEvent       `json:"ce,omitempty"`
	Activities     []Activity
}

func (pi *ProcessInstance) GetProcessInfo() *ProcessDefinition {
	return pi.Definition
}

func (pi *ProcessInstance) GetInstanceKey() int64 {
	return pi.InstanceKey
}

func (pi *ProcessInstance) GetVariable(key string) interface{} {
	return pi.VariableHolder.GetVariable(key)
}

func (pi *ProcessInstance) SetVariable(key string, value interface{}) {
	pi.VariableHolder.SetVariable(key, value)
}

func (pi *ProcessInstance) GetCreatedAt() time.Time {
	return pi.CreatedAt
}

// GetState returns one of [ Ready, Active, Completed, Failed ]
func (pi *ProcessInstance) GetState() ActivityState {
	return pi.State
}

func (pi *ProcessInstance) AppendActivity(activity Activity) {
	pi.Activities = append(pi.Activities, activity)
}

func (pi *ProcessInstance) FindActiveActivityByElementId(id string) Activity {
	for _, a := range pi.Activities {
		if a.Element().GetId() == id && a.State() == Active {
			return a
		}
	}
	return nil
}

func (pi *ProcessInstance) FindActivity(key int64) Activity {
	for _, a := range pi.Activities {
		if a.Key() == key {
			return a
		}
	}
	return nil
}

// ActivityState as per BPMN 2.0 spec, section 13.2.2 Activity, page 428, State diagram:
//
//	              (Inactive)
//	                  O
//	                  |
//	A Token           v
//	Arrives        ┌─────┐
//	               │Ready│
//	               └─────┘
//	                  v         Activity Interrupted             An Alternative Path For
//	                  O -------------------------------------->O----------------------------+
//	Data InputSet     v                                        | Event Gateway Selected     |
//	Available     ┌──────┐                         Interrupting|                            |
//	              │Active│                         Event       |                            |
//	              └──────┘                                     |                            v
//	                  v         Activity Interrupted           v An Alternative Path For┌─────────┐
//	                  O -------------------------------------->O ---------------------->│Withdrawn│
//	Activity's work   v                                        | Event Gateway Selected └─────────┘
//	completed     ┌──────────┐                     Interrupting|                            |
//	              │Completing│                     Event       |                 The Process|
//	              └──────────┘                                 |                 Ends       |
//	                  v         Activity Interrupted           v  Non-Error                 |
//	Completing        O -------------------------------------->O--------------+             |
//	Requirements Done v                                  Error v              v             |
//	Assignments   ┌─────────┐                              ┌───────┐       ┌───────────┐    |
//	Completed     │Completed│                              │Failing│       │Terminating│    |
//	              └─────────┘                              └───────┘       └───────────┘    |
//	                  v  Compensation ┌────────────┐          v               v             |
//	                  O ------------->│Compensating│          O <-------------O Terminating |
//	                  |  Occurs       └────────────┘          v               v Requirements Done
//	      The Process |         Compensation v   Compensation  |           ┌──────────┐     |
//	      Ends        |       +--------------O----------------/|\--------->│Terminated│     |
//	                  |       | Completes    |   Interrupted   |           └──────────┘     |
//	                  |       v              |                 v              |             |
//	                  | ┌───────────┐        |Compensation┌──────┐            |             |
//	                  | │Compensated│        +----------->│Failed│            |             |
//	                  | └─────┬─────┘         Failed      └──────┘            |             |
//	                  |       |                               |               |             |
//	                  v      / The Process Ends               / Process Ends /              |
//	                  O<--------------------------------------------------------------------+
//	             (Closed)
type ActivityState string

const (
	Active       ActivityState = "ACTIVE"
	Compensated  ActivityState = "COMPENSATED"
	Compensating ActivityState = "COMPENSATING"
	Completed    ActivityState = "COMPLETED"
	Completing   ActivityState = "COMPLETING"
	Failed       ActivityState = "FAILED"
	Failing      ActivityState = "FAILING"
	Ready        ActivityState = "READY"
	Terminated   ActivityState = "TERMINATED"
	Terminating  ActivityState = "TERMINATING"
	Withdrawn    ActivityState = "WITHDRAWN"
)

type MessageSubscription struct {
	ElementId          string
	ElementInstanceKey int64
	ProcessKey         int64
	ProcessInstanceKey int64
	Name               string
	MessageState       ActivityState
	CreatedAt          time.Time
	OriginActivity     Activity        // Deprecated: FIXME, should not be public, nor serialized
	BaseElement        bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (m MessageSubscription) Key() int64 {
	return m.ElementInstanceKey
}

func (m MessageSubscription) State() ActivityState {
	return m.MessageState
}

func (m MessageSubscription) Element() bpmn20.FlowNode {
	return m.BaseElement
}

type TimerState string

const TimerCreated TimerState = "CREATED"
const TimerTriggered TimerState = "TRIGGERED"
const TimerCancelled TimerState = "CANCELLED"

// Timer is created, when a process instance reaches a Timer Intermediate Message Event.
// The logic is simple: CreatedAt + Duration = DueAt
// The TimerState is one of [ TimerCreated, TimerTriggered, TimerCancelled ]
type Timer struct {
	ElementId          string
	ElementInstanceKey int64
	ProcessKey         int64
	ProcessInstanceKey int64
	TimerState         TimerState
	CreatedAt          time.Time
	DueAt              time.Time
	Duration           time.Duration
	OriginActivity     Activity        // Deprecated: FIXME, should not be public, nor serialized
	BaseElement        bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (t Timer) Key() int64 {
	return t.ElementInstanceKey
}

func (t Timer) State() ActivityState {
	switch t.TimerState {
	case TimerCreated:
		return Active
	case TimerTriggered:
		return Completed
	case TimerCancelled:
		return Withdrawn
	}
	panic(fmt.Sprintf("[invariant check] missing mapping for timer state=%s", t.TimerState))
}

func (t Timer) Element() bpmn20.FlowNode {
	return t.BaseElement
}

type Activity interface {
	Key() int64
	State() ActivityState
	Element() bpmn20.FlowNode
}

type Job struct {
	ElementId          string
	ElementInstanceKey int64
	ProcessInstanceKey int64
	JobKey             int64
	JobState           ActivityState
	CreatedAt          time.Time
	BaseElement        bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (j Job) Key() int64 {
	return j.JobKey
}

func (j Job) State() ActivityState {
	return j.JobState
}

func (j Job) Element() bpmn20.FlowNode {
	return j.BaseElement
}
