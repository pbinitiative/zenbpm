package bpmn20

import "github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"

type ElementType string
type GatewayDirection string

const (
	ElementTypeStartEvent             ElementType = "START_EVENT"
	ElementTypeEndEvent               ElementType = "END_EVENT"
	ElementTypeServiceTask            ElementType = "SERVICE_TASK"
	ElementTypeUserTask               ElementType = "USER_TASK"
	ElementTypeParallelGateway        ElementType = "PARALLEL_GATEWAY"
	ElementTypeExclusiveGateway       ElementType = "EXCLUSIVE_GATEWAY"
	ElementTypeIntermediateCatchEvent ElementType = "INTERMEDIATE_CATCH_EVENT"
	ElementTypeIntermediateThrowEvent ElementType = "INTERMEDIATE_THROW_EVENT"
	ElementTypeEventBasedGateway      ElementType = "EVENT_BASED_GATEWAY"
	ElementTypeInclusiveGateway       ElementType = "INCLUSIVE_GATEWAY"
	ElementTypeSequenceFlow           ElementType = "SEQUENCE_FLOW"

	Unspecified GatewayDirection = "Unspecified"
	Converging  GatewayDirection = "Converging"
	Diverging   GatewayDirection = "Diverging"
	Mixed       GatewayDirection = "Mixed"
)

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

type GatewayElement interface {
	FlowNode
	IsParallel() bool
	IsExclusive() bool
	IsInclusive() bool
}

func (task TTask) GetInputMapping() []extensions.TIoMapping {
	return task.Input
}
func (task TTask) GetOutputMapping() []extensions.TIoMapping {
	return task.Output
}

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

func (endEvent TEndEvent) GetType() ElementType { return ElementTypeEndEvent }

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

func (parallelGateway TParallelGateway) GetType() ElementType {
	return ElementTypeParallelGateway
}

func (parallelGateway TParallelGateway) IsParallel() bool {
	return true
}
func (parallelGateway TParallelGateway) IsExclusive() bool {
	return false
}

func (parallelGateway TParallelGateway) IsInclusive() bool {
	return false
}

func (exclusiveGateway TExclusiveGateway) GetType() ElementType {
	return ElementTypeExclusiveGateway
}

func (exclusiveGateway TExclusiveGateway) IsParallel() bool {
	return false
}
func (exclusiveGateway TExclusiveGateway) IsExclusive() bool {
	return true
}

func (exclusiveGateway TExclusiveGateway) IsInclusive() bool {
	return false
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetType() ElementType {
	return ElementTypeIntermediateCatchEvent
}

// -------------------------------------------------------------------------

func (eventBasedGateway TEventBasedGateway) GetType() ElementType {
	return ElementTypeEventBasedGateway
}

func (eventBasedGateway TEventBasedGateway) IsParallel() bool {
	return false
}

func (eventBasedGateway TEventBasedGateway) IsExclusive() bool {
	return true
}

func (eventBasedGateway TEventBasedGateway) IsInclusive() bool {
	return false
}

// -------------------------------------------------------------------------

func (intermediateThrowEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}

func (inclusiveGateway TInclusiveGateway) GetType() ElementType {
	return ElementTypeInclusiveGateway
}

func (inclusiveGateway TInclusiveGateway) IsParallel() bool {
	return false
}

func (inclusiveGateway TInclusiveGateway) IsExclusive() bool {
	return false
}

func (inclusiveGateway TInclusiveGateway) IsInclusive() bool {
	return true
}
