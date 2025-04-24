package bpmn20

import "github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"

const (
	ElementTypeServiceTask  ElementType = "SERVICE_TASK"
	ElementTypeUserTask     ElementType = "USER_TASK"
	ElementTypeSequenceFlow ElementType = "SEQUENCE_FLOW"
)

type Activity interface {
	FlowNode
	GetCompletionQuantity() int
	GetIsForCompensation() bool
	GetStartQuantity() int
}

type TaskElement interface {
	FlowNode
	GetTaskType() string
	GetInputMapping() []extensions.TIoMapping
	GetOutputMapping() []extensions.TIoMapping
}

type UserTaskElement interface {
	TaskElement
	GetAssignmentAssignee() string
	GetAssignmentCandidateGroups() []string
}

type TActivity struct {
	TFlowNode
	CompletionQuantity int  `xml:"completionQuantity,attr"`
	IsForCompensation  bool `xml:"isForCompensation,attr"`
	StartQuantity      int  `xml:"startQuantity,attr" default:"1"`
}
type TTask struct {
	TActivity
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

// TExternallyProcessedTask is to be processed by external Job workers. Is not part of original BPMN Implementation
// BPMN 2.0 Unorthodox.
type TExternallyProcessedTask struct {
	TTask
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
}
type TServiceTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

type TBusinessRuleTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

type TSendTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

type TUserTask struct {
	TTask
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	AssignmentDefinition extensions.TAssignmentDefinition `xml:"extensionElements>assignmentDefinition"`
}

func (task TTask) GetInputMapping() []extensions.TIoMapping {
	return task.Input
}
func (task TTask) GetOutputMapping() []extensions.TIoMapping {
	return task.Output
}

func (serviceTask TServiceTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (businessRuleTask TBusinessRuleTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (sendTask TSendTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (sendTask TExternallyProcessedTask) GetTaskType() string {
	return sendTask.TaskDefinition.TypeName
}

func (userTask TUserTask) GetType() ElementType {
	return ElementTypeUserTask
}
func (userTask TUserTask) GetTaskType() string { return "user-task-type" }

func (userTask TUserTask) GetAssignmentAssignee() string {
	return userTask.AssignmentDefinition.Assignee
}

func (userTask TUserTask) GetAssignmentCandidateGroups() []string {
	return userTask.AssignmentDefinition.GetCandidateGroups()
}
