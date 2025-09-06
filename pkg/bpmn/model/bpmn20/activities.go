// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn20

import (
	"encoding/xml"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

const (
	ElementTypeServiceTask  ElementType = "SERVICE_TASK"
	ElementTypeUserTask     ElementType = "USER_TASK"
	ElementTypeSequenceFlow ElementType = "SEQUENCE_FLOW"
	ElementBusinessRuleTask ElementType = "BUSINESS_RULE_TASK"
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

	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
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

func (businessRuleTask *TBusinessRuleTask) GetType() ElementType { return ElementBusinessRuleTask }

func (businessRuleTask *TBusinessRuleTask) GetTaskType() string {
	return businessRuleTask.Implementation.(*TBusinessRuleTaskExternal).TaskDefinition.TypeName
}

// TODO: implement data quality checks
func (businessRuleTask *TBusinessRuleTask) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
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
	case tempStruct.TBusinessRuleTaskLocal.CalledDecision.DecisionId != "":
		businessRuleTask.Implementation = &tempStruct.TBusinessRuleTaskLocal
	case tempStruct.TBusinessRuleTaskExternal.TaskDefinition.TypeName != "":
		businessRuleTask.Implementation = &tempStruct.TBusinessRuleTaskExternal
	}
	return nil
}

type TBusinessRuleTaskImplementation interface {
	businessRuleTaskImplementation()
}

type TBusinessRuleTaskLocal struct {
	CalledDecision extensions.TCalledDecision `xml:"extensionElements>calledDecision"`
}

func (d TBusinessRuleTaskLocal) businessRuleTaskImplementation() {}

type TBusinessRuleTaskExternal struct {
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
	Headers        extensions.THeader         `xml:"extensionElements>taskHeaders"`
}

func (d TBusinessRuleTaskExternal) businessRuleTaskImplementation() {}

type TSendTask struct {
	TExternallyProcessedTask
	OperationRef   string `xml:"operationRef,attr"`
	Implementation string `xml:"implementation,attr"`
}

func (sendTask TSendTask) GetType() ElementType { return ElementTypeServiceTask }

type TUserTask struct {
	TTask
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
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
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
	CalledElement extensions.TCalledElement `xml:"extensionElements>calledElement"`
}
