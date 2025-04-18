package runtime

import (
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type ProcessDefinition struct {
	BpmnProcessId    string              // The ID as defined in the BPMN file
	Version          int32               // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Key              int64               // The engines key for this given process with version
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

type ProcessInstance struct {
	Definition     *ProcessDefinition
	Key            int64
	VariableHolder VariableHolder
	CreatedAt      time.Time
	State          ActivityState
	CaughtEvents   []CatchEvent
	Activities     []Activity
}

func (pi *ProcessInstance) GetProcessInfo() *ProcessDefinition {
	return pi.Definition
}

func (pi *ProcessInstance) GetInstanceKey() int64 {
	return pi.Key
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
		if a.Element().GetId() == id && a.GetState() == ActivityStateActive {
			return a
		}
	}
	return nil
}

func (pi *ProcessInstance) FindActivity(key int64) Activity {
	for _, a := range pi.Activities {
		if a.GetKey() == key {
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
type ActivityState int

//go:generate go tool stringer -type=ActivityState

const (
	_ ActivityState = iota
	ActivityStateActive
	ActivityStateCompensated
	ActivityStateCompensating
	ActivityStateCompleted
	ActivityStateCompleting
	ActivityStateFailed
	ActivityStateFailing
	ActivityStateReady
	ActivityStateTerminated
	ActivityStateTerminating
	ActivityStateWithdrawn
)

type MessageSubscription struct {
	ElementId            string
	ElementInstanceKey   int64
	ProcessDefinitionKey int64
	ProcessInstanceKey   int64
	Name                 string
	MessageState         ActivityState
	CreatedAt            time.Time
	OriginActivity       Activity        // Deprecated: FIXME, should not be public, nor serialized
	BaseElement          bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (m MessageSubscription) EqualTo(m2 MessageSubscription) bool {
	if m.ElementId == m2.ElementId &&
		m.ElementInstanceKey == m2.ElementInstanceKey &&
		m.ProcessDefinitionKey == m2.ProcessDefinitionKey &&
		m.ProcessInstanceKey == m2.ProcessInstanceKey &&
		m.Name == m2.Name &&
		m.MessageState == m2.MessageState &&
		m.CreatedAt.Truncate(time.Millisecond).Equal(m2.CreatedAt.Truncate(time.Millisecond)) {
		return true
	}
	return false
}

func (m MessageSubscription) GetKey() int64 {
	return m.ElementInstanceKey
}

func (m MessageSubscription) GetState() ActivityState {
	return m.MessageState
}

func (m MessageSubscription) Element() bpmn20.FlowNode {
	return m.BaseElement
}

//go:generate go tool stringer -type=TimerState
type TimerState int

const (
	_ TimerState = iota
	TimerStateCreated
	TimerStateTriggered
	TimerStateCancelled
)

// Timer is created, when a process instance reaches a Timer Intermediate Message Event.
// The logic is simple: CreatedAt + Duration = DueAt
// The TimerState is one of [ TimerCreated, TimerTriggered, TimerCancelled ]
type Timer struct {
	ElementId            string
	Key                  int64
	ProcessDefinitionKey int64
	ProcessInstanceKey   int64
	TimerState           TimerState
	CreatedAt            time.Time
	DueAt                time.Time
	Duration             time.Duration
	OriginActivity       Activity        // Deprecated: FIXME, should not be public, nor serialized
	BaseElement          bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (t Timer) GetKey() int64 {
	return t.Key
}

func (t Timer) EqualTo(t2 Timer) bool {
	if t.Key == t2.Key &&
		t.ElementId == t2.ElementId &&
		t.ProcessDefinitionKey == t2.ProcessDefinitionKey &&
		t.TimerState == t2.TimerState &&
		t.CreatedAt.Truncate(time.Millisecond).Equal(t2.CreatedAt.Truncate(time.Millisecond)) &&
		t.DueAt.Truncate(time.Millisecond).Equal(t2.DueAt.Truncate(time.Millisecond)) &&
		t.Duration == t2.Duration {
		return true
	}
	return false
}

func (t Timer) GetState() ActivityState {
	switch t.TimerState {
	case TimerStateCreated:
		return ActivityStateActive
	case TimerStateTriggered:
		return ActivityStateCompleted
	case TimerStateCancelled:
		return ActivityStateWithdrawn
	}
	panic(fmt.Sprintf("[invariant check] missing mapping for timer state=%s", t.TimerState))
}

func (t Timer) Element() bpmn20.FlowNode {
	return t.BaseElement
}

type Activity interface {
	GetKey() int64
	GetState() ActivityState
	Element() bpmn20.FlowNode
}

type Job struct {
	ElementId          string
	ElementInstanceKey int64
	ProcessInstanceKey int64
	Key                int64
	State              ActivityState
	CreatedAt          time.Time
	BaseElement        bpmn20.FlowNode // Deprecated: FIXME, should not be public, nor serialized
}

func (j Job) GetKey() int64 {
	return j.Key
}

func (j Job) GetState() ActivityState {
	return j.State
}

func (j Job) Element() bpmn20.FlowNode {
	return j.BaseElement
}
