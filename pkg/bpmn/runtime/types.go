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

type ProcessType int

//go:generate go tool stringer -type=ProcessType

const (
	_ ProcessType = iota
	ProcessTypeDefault
	ProcessTypeSubProcess
	ProcessTypeCallActivity
	ProcessTypeMultiInstance
)

type ProcessInstance interface {
	Type() ProcessType
	ProcessInstance() *ProcessInstanceData
	GetParentProcessInstanceKey() *int64
}

type SubProcessInstance struct {
	ParentProcessExecutionToken           ExecutionToken
	ParentProcessTargetElementInstanceKey int64
	ParentProcessTargetElementId          string
	ProcessInstanceData
}

func (s *SubProcessInstance) ProcessInstance() *ProcessInstanceData {
	return &s.ProcessInstanceData
}

func (s *SubProcessInstance) Type() ProcessType {
	return ProcessTypeSubProcess
}

func (s *SubProcessInstance) GetParentProcessInstanceKey() *int64 {
	return &s.ParentProcessExecutionToken.ProcessInstanceKey
}

type MultiInstanceInstance struct {
	ParentProcessExecutionToken           ExecutionToken
	ParentProcessTargetElementInstanceKey int64
	ParentProcessTargetElementId          string
	ProcessInstanceData
}

func (m *MultiInstanceInstance) ProcessInstance() *ProcessInstanceData {
	return &m.ProcessInstanceData
}

func (m *MultiInstanceInstance) Type() ProcessType {
	return ProcessTypeMultiInstance
}

func (m *MultiInstanceInstance) GetParentProcessInstanceKey() *int64 {
	return &m.ParentProcessExecutionToken.ProcessInstanceKey
}

type CallActivityInstance struct {
	ParentProcessExecutionToken           ExecutionToken
	ParentProcessTargetElementInstanceKey int64
	ProcessInstanceData
}

func (c *CallActivityInstance) ProcessInstance() *ProcessInstanceData {
	return &c.ProcessInstanceData
}

func (c *CallActivityInstance) Type() ProcessType {
	return ProcessTypeCallActivity
}

func (c *CallActivityInstance) GetParentProcessInstanceKey() *int64 {
	return &c.ParentProcessExecutionToken.ProcessInstanceKey
}

type DefaultProcessInstance struct {
	ProcessInstanceData
}

func (d *DefaultProcessInstance) ProcessInstance() *ProcessInstanceData {
	return &d.ProcessInstanceData
}

func (d *DefaultProcessInstance) Type() ProcessType {
	return ProcessTypeDefault
}

func (d *DefaultProcessInstance) GetParentProcessInstanceKey() *int64 {
	return nil
}

type ProcessInstanceData struct {
	Definition     *ProcessDefinition
	Key            int64
	BusinessKey    *string // TODO: introduce cluster data layer and remove this from the engine
	VariableHolder VariableHolder
	CreatedAt      time.Time
	State          ActivityState
	StartElementId *string
	// HistoryTTLSec is the history retention TTL in seconds carried in memory so
	// that child instances can inherit it from their parent without a synchronous
	// DB read at save time. nil means "no TTL". For root instances it is derived
	// from the request context at creation; for child instances it is copied from
	// the parent runtime object; for instances loaded from the DB it is read back
	// from the persisted value.
	HistoryTTLSec *int64
}

func (pi *ProcessInstanceData) GetProcessInfo() *ProcessDefinition {
	return pi.Definition
}

func (pi *ProcessInstanceData) GetInstanceKey() int64 {
	return pi.Key
}

func (pi *ProcessInstanceData) GetVariable(key string) interface{} {
	return pi.VariableHolder.GetLocalVariable(key)
}

func (pi *ProcessInstanceData) SetVariable(key string, value interface{}) {
	pi.VariableHolder.SetLocalVariable(key, value)
}

func (pi *ProcessInstanceData) GetCreatedAt() time.Time {
	return pi.CreatedAt
}

// GetState returns one of [ Ready, Active, Completed, Failed ]
func (pi *ProcessInstanceData) GetState() ActivityState {
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

type MessageSubscriptionType int

//go:generate go tool stringer -type=MessageSubscriptionType

const (
	MessageSubscriptionTypeUnknown MessageSubscriptionType = iota
	MessageSubscriptionTypeToken
	MessageSubscriptionTypeInstance
	MessageSubscriptionTypeDefinition
)

type MessageSubscription interface {
	Type() MessageSubscriptionType
	MessageSubscription() *MessageSubscriptionData
}

type MessageSubscriptionData struct {
	Key                  int64
	ElementId            string
	Name                 string
	State                ActivityState
	ProcessDefinitionKey int64
	CreatedAt            time.Time
}

type TokenMessageSubscription struct {
	Token              ExecutionToken
	ProcessInstanceKey int64
	CorrelationKey     string
	MessageSubscriptionData
}

func (t *TokenMessageSubscription) Type() MessageSubscriptionType {
	return MessageSubscriptionTypeToken
}
func (t *TokenMessageSubscription) MessageSubscription() *MessageSubscriptionData {
	return &t.MessageSubscriptionData
}

func (t *TokenMessageSubscription) GetId() string {
	return t.ElementId
}
func (t *TokenMessageSubscription) GetKey() int64 {
	return t.Key
}
func (t *TokenMessageSubscription) GatewayEvent() {}

type InstanceMessageSubscription struct {
	ProcessInstanceKey int64
	CorrelationKey     string
	MessageSubscriptionData
}

func (t *InstanceMessageSubscription) Type() MessageSubscriptionType {
	return MessageSubscriptionTypeInstance
}

func (t *InstanceMessageSubscription) MessageSubscription() *MessageSubscriptionData {
	return &t.MessageSubscriptionData
}

type DefinitionMessageSubscription struct {
	MessageSubscriptionData
}

func (t *DefinitionMessageSubscription) Type() MessageSubscriptionType {
	return MessageSubscriptionTypeDefinition
}

func (t *DefinitionMessageSubscription) MessageSubscription() *MessageSubscriptionData {
	return &t.MessageSubscriptionData
}

func EqualTo(m MessageSubscription, m2 MessageSubscription) bool {
	if m == nil || m2 == nil {
		return m == nil && m2 == nil
	}
	if m.Type() != m2.Type() {
		return false
	}
	md := m.MessageSubscription()
	md2 := m2.MessageSubscription()
	if md.ElementId != md2.ElementId ||
		md.Key != md2.Key ||
		md.ProcessDefinitionKey != md2.ProcessDefinitionKey ||
		md.Name != md2.Name ||
		md.State != md2.State ||
		!md.CreatedAt.Truncate(time.Millisecond).Equal(md2.CreatedAt.Truncate(time.Millisecond)) {
		return false
	}
	switch a := m.(type) {
	case *TokenMessageSubscription:
		b := m2.(*TokenMessageSubscription)
		return a.ProcessInstanceKey == b.ProcessInstanceKey &&
			a.CorrelationKey == b.CorrelationKey &&
			a.Token.Key == b.Token.Key
	case *InstanceMessageSubscription:
		b := m2.(*InstanceMessageSubscription)
		return a.ProcessInstanceKey == b.ProcessInstanceKey &&
			a.CorrelationKey == b.CorrelationKey
	case *DefinitionMessageSubscription:
		return true
	}
	return false
}

//go:generate go tool stringer -type=ErrorState

type ErrorState int

const (
	_ ErrorState = iota
	ErrorStateCreated
	ErrorStateCancelled
)

type ErrorSubscription struct {
	ElementId            string
	Key                  int64
	ElementInstanceKey   int64
	ProcessDefinitionKey int64
	ProcessInstanceKey   int64
	ErrorCode            *string
	State                ErrorState
	CreatedAt            time.Time
	Token                ExecutionToken
}

func (errorSubscription ErrorSubscription) EqualTo(e2 ErrorSubscription) bool {
	if errorSubscription.ElementId == e2.ElementId &&
		errorSubscription.Key == e2.Key &&
		errorSubscription.ProcessDefinitionKey == e2.ProcessDefinitionKey &&
		errorSubscription.ProcessInstanceKey == e2.ProcessInstanceKey &&
		errorSubscription.ErrorCode == e2.ErrorCode &&
		errorSubscription.State == e2.State &&
		errorSubscription.CreatedAt.Truncate(time.Millisecond).Equal(e2.CreatedAt.Truncate(time.Millisecond)) {
		return true
	}
	return false
}

func (errorSubscription ErrorSubscription) GetId() string {
	return errorSubscription.ElementId
}
func (errorSubscription ErrorSubscription) GetKey() int64 {
	return errorSubscription.Key
}
func (errorSubscription ErrorSubscription) GetState() ErrorState {
	return errorSubscription.State
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
	ElementId            string // id of the intermediateCatchEvent or event sub process TimerStartEvent
	Key                  int64
	ElementInstanceKey   *int64
	ProcessDefinitionKey int64
	ProcessInstanceKey   *int64
	TimerState           TimerState
	CreatedAt            time.Time
	DueAt                time.Time
	Duration             time.Duration
	Token                *ExecutionToken
}

func (t Timer) GetId() string {
	return t.ElementId
}
func (t Timer) GatewayEvent() {}

func (t Timer) GetKey() int64 {
	return t.Key
}

func (t Timer) EqualTo(t2 Timer) bool {
	// Compare ElementInstanceKey pointers
	elementInstanceKeyMatch := false
	if t.ElementInstanceKey == nil && t2.ElementInstanceKey == nil {
		elementInstanceKeyMatch = true
	} else if t.ElementInstanceKey != nil && t2.ElementInstanceKey != nil {
		elementInstanceKeyMatch = *t.ElementInstanceKey == *t2.ElementInstanceKey
	}

	// Compare ProcessInstanceKey pointers
	processInstanceKeyMatch := false
	if t.ProcessInstanceKey == nil && t2.ProcessInstanceKey == nil {
		processInstanceKeyMatch = true
	} else if t.ProcessInstanceKey != nil && t2.ProcessInstanceKey != nil {
		processInstanceKeyMatch = *t.ProcessInstanceKey == *t2.ProcessInstanceKey
	}

	// Compare Token pointers
	tokenMatch := false
	if t.Token == nil && t2.Token == nil {
		tokenMatch = true
	} else if t.Token != nil && t2.Token != nil {
		tokenMatch = *t.Token == *t2.Token
	}

	if t.Key == t2.Key &&
		t.ElementId == t2.ElementId &&
		t.ProcessDefinitionKey == t2.ProcessDefinitionKey &&
		t.TimerState == t2.TimerState &&
		t.CreatedAt.Truncate(time.Millisecond).Equal(t2.CreatedAt.Truncate(time.Millisecond)) &&
		t.DueAt.Truncate(time.Millisecond).Equal(t2.DueAt.Truncate(time.Millisecond)) &&
		t.Duration == t2.Duration &&
		elementInstanceKeyMatch &&
		processInstanceKeyMatch &&
		tokenMatch {
		return true
	}
	return false
}

func (t Timer) GetState() (ActivityState, error) {
	switch t.TimerState {
	case TimerStateCreated:
		return ActivityStateActive, nil
	case TimerStateTriggered:
		return ActivityStateCompleted, nil
	case TimerStateCancelled:
		return ActivityStateWithdrawn, nil
	}
	return 0, fmt.Errorf("missing ActivityState mapping for timer state=%s", t.TimerState)
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
	InputVariables     map[string]any
	OutputVariables    map[string]any
	CreatedAt          time.Time
	Token              ExecutionToken
	Assignee           *string
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

type FlowElementInstance struct {
	Key                int64
	ProcessInstanceKey int64
	ElementId          string
	CreatedAt          time.Time
	CompletedAt        *time.Time
	ExecutionTokenKey  int64
	InputVariables     map[string]any
	OutputVariables    map[string]any
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
