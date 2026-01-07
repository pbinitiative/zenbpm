package runtime

import (
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type ProcessDefinition struct {
	BpmnProcessId   string              // The ID as defined in the BPMN file
	Version         int32               // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Key             int64               // The engines key for this given process with version
	Definitions     bpmn20.TDefinitions // parsed file content
	BpmnData        string              // the raw source data, compressed and encoded via ascii85
	BpmnProcessName string              // the name of the process
	BpmnChecksum    [16]byte            // internal checksum to identify different versions
}

type CatchEvent struct {
	Name       string
	CaughtAt   time.Time
	IsConsumed bool
	Variables  map[string]interface{}
}

type ProcessInstance struct {
	Definition                  *ProcessDefinition
	Key                         int64
	BusinessKey                 *string // TODO: introduce cluster data layer and remove this from the engine
	VariableHolder              VariableHolder
	CreatedAt                   time.Time
	State                       ActivityState
	ParentProcessExecutionToken *ExecutionToken
}

func (pi *ProcessInstance) GetProcessInfo() *ProcessDefinition {
	return pi.Definition
}

func (pi *ProcessInstance) GetInstanceKey() int64 {
	return pi.Key
}

func (pi *ProcessInstance) GetVariable(key string) interface{} {
	return pi.VariableHolder.GetLocalVariable(key)
}

func (pi *ProcessInstance) SetVariable(key string, value interface{}) {
	pi.VariableHolder.SetLocalVariable(key, value)
}

func (pi *ProcessInstance) GetCreatedAt() time.Time {
	return pi.CreatedAt
}

// GetState returns one of [ Ready, Active, Completed, Failed ]
func (pi *ProcessInstance) GetState() ActivityState {
	return pi.State
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
	Key                  int64
	ElementId            string
	ProcessDefinitionKey int64
	ProcessInstanceKey   int64
	Name                 string
	CorrelationKey       string
	State                ActivityState
	CreatedAt            time.Time
	Token                ExecutionToken
}

func (m MessageSubscription) EqualTo(m2 MessageSubscription) bool {
	if m.ElementId == m2.ElementId &&
		m.Key == m2.Key &&
		m.ProcessDefinitionKey == m2.ProcessDefinitionKey &&
		m.ProcessInstanceKey == m2.ProcessInstanceKey &&
		m.Name == m2.Name &&
		m.State == m2.State &&
		m.CreatedAt.Truncate(time.Millisecond).Equal(m2.CreatedAt.Truncate(time.Millisecond)) {
		return true
	}
	return false
}

func (m MessageSubscription) GetId() string {
	return m.ElementId
}
func (m MessageSubscription) GatewayEvent() {}
func (m MessageSubscription) GetKey() int64 {
	return m.Key
}

func (m MessageSubscription) GetState() ActivityState {
	return m.State
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
	ElementId            string // id of the intermediateCatchEvent
	Key                  int64
	ElementInstanceKey   int64
	ProcessDefinitionKey int64
	ProcessInstanceKey   int64
	TimerState           TimerState
	CreatedAt            time.Time
	DueAt                time.Time
	Duration             time.Duration
	Token                ExecutionToken
}

func (t Timer) GetId() string {
	return t.ElementId
}
func (t Timer) GatewayEvent() {}

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
		t.Duration == t2.Duration &&
		t.Token == t2.Token {
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
	Type               string
	Variables          map[string]any
	CreatedAt          time.Time
	Token              ExecutionToken
}

func (j Job) GetKey() int64 {
	return j.Key
}

func (j Job) GetState() ActivityState {
	return j.State
}

//go:generate go tool stringer -type=TokenState
type TokenState int

const (
	_ TokenState = iota
	TokenStateRunning
	TokenStateWaiting
	TokenStateCompleted
	TokenStateCanceled
	TokenStateFailed
)

// ExecutionToken represents one processing step in the engine.
// Engine assumes that:
//   - when an instance of the token hits parallel gateway it is completed and new tokens are created for each fork
//
// https://github.com/pbinitiative/zenbpm/issues/110
type ExecutionToken struct {
	Key                int64
	ElementInstanceKey int64
	ElementId          string
	ProcessInstanceKey int64
	State              TokenState
	CreatedAt          time.Time
}

type FlowElementHistoryItem struct {
	Key                int64
	ProcessInstanceKey int64
	ElementId          string
	CreatedAt          time.Time
}

// Incident represent an incident that happened in process execution
type Incident struct {
	Key                int64
	ElementInstanceKey int64
	ElementId          string
	ProcessInstanceKey int64
	Message            string
	CreatedAt          time.Time
	ResolvedAt         *time.Time
	Token              ExecutionToken
}
