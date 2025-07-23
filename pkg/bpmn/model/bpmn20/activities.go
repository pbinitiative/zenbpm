package bpmn20

import (
	"encoding/xml"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

const (
	ElementTypeServiceTask  ElementType = "SERVICE_TASK"
	ElementTypeUserTask     ElementType = "USER_TASK"
	ElementTypeSequenceFlow ElementType = "SEQUENCE_FLOW"
)

type Activity interface {
	FlowNode
	// TODO: Do we need this?
	// GetCompletionQuantity() int
	// GetIsForCompensation() bool
	// GetStartQuantity() int
}

type InternalTask interface {
	Activity
	GetId() string
	GetType() ElementType
	GetTaskType() string
	GetInputMapping() []extensions.TIoMapping
	GetOutputMapping() []extensions.TIoMapping
}

type UserTaskElement interface {
	InternalTask
	GetAssignmentAssignee() string
	GetAssignmentCandidateGroups() []string
}

type TActivity struct {
	TFlowNode
	CompletionQuantity int  `xml:"completionQuantity,attr"`
	IsForCompensation  bool `xml:"isForCompensation,attr"`
	StartQuantity      int  `xml:"startQuantity,attr" default:"1"`

	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (task TActivity) GetInputMapping() []extensions.TIoMapping  { return task.Input }
func (task TActivity) GetOutputMapping() []extensions.TIoMapping { return task.Output }

type TTask struct {
	TActivity
}

// TExternallyProcessedTask is to be processed by external Job workers. Is not part of original BPMN Implementation
// BPMN 2.0 Unorthodox.
type TExternallyProcessedTask struct {
	TTask
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
}

func (sendTask TExternallyProcessedTask) GetTaskType() string {
	return sendTask.TaskDefinition.TypeName
}

type TServiceTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

func (serviceTask TServiceTask) GetType() ElementType { return ElementTypeServiceTask }

type TBusinessRuleTask struct {
	TTask
	Implementation TBusinessRuleTaskImplementation
}

func (businessRuleTask *TBusinessRuleTask) GetTaskType() string {
	return "business-rule-task-type"
}

type TBusinessRuleTaskImplementation interface {
	businessRuleTaskImplementation()
}

// TBusinessRuleTaskDefault Implementation according to BPMN 2.0 spec
type TBusinessRuleTaskDefault struct {
	Implementation string `xml:"implementation,attr"`
}

func (d TBusinessRuleTaskDefault) businessRuleTaskImplementation() {}

// TBusinessRuleTaskLocal Camunda modeler DMN decision
type TBusinessRuleTaskLocal struct {
	CalledDecision extensions.TCalledDecision `xml:"extensionElements>calledDecision"`
}

func (d TBusinessRuleTaskLocal) businessRuleTaskImplementation() {}

// TBusinessRuleTaskExternal Camunda modeler Job Worker decision
type TBusinessRuleTaskExternal struct {
	TExternallyProcessedTask
	Headers extensions.THeader `xml:"extensionElements>taskHeaders"`
}

func (d TBusinessRuleTaskExternal) businessRuleTaskImplementation() {}

func (businessRuleTask *TBusinessRuleTask) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TBusinessRuleTaskDefault
		TBusinessRuleTaskLocal
		TBusinessRuleTaskExternal
		TTask
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	businessRuleTask.TTask = tempStruct.TTask
	switch {
	case tempStruct.TBusinessRuleTaskDefault.Implementation != "":
		businessRuleTask.Implementation = &tempStruct.TBusinessRuleTaskDefault
	case tempStruct.TBusinessRuleTaskLocal.CalledDecision.DecisionId != "":
		businessRuleTask.Implementation = &tempStruct.TBusinessRuleTaskLocal
	case tempStruct.TBusinessRuleTaskExternal.TaskDefinition.TypeName != "":
		businessRuleTask.Implementation = &tempStruct.TBusinessRuleTaskExternal
	}
	return nil
}

func (businessRuleTask TBusinessRuleTask) GetType() ElementType { return ElementTypeServiceTask }

type TSendTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

func (sendTask TSendTask) GetType() ElementType { return ElementTypeServiceTask }

type TUserTask struct {
	TTask
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	AssignmentDefinition extensions.TAssignmentDefinition `xml:"extensionElements>assignmentDefinition"`
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

type TCallActivity struct {
	TActivity
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	CalledElement extensions.TCalledElement `xml:"extensionElements>calledElement"`
}
