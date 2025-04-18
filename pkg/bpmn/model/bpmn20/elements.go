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
	GetInputMapping() []extensions.TIoMapping
	GetOutputMapping() []extensions.TIoMapping
	GetTaskDefinitionType() string
	GetAssignmentAssignee() string
	GetAssignmentCandidateGroups() []string
}

type GatewayElement interface {
	FlowNode
	IsParallel() bool
	IsExclusive() bool
	IsInclusive() bool
}

func (startEvent TStartEvent) GetId() string {
	return startEvent.Id
}

func (startEvent TStartEvent) GetName() string {
	return startEvent.Name
}

func (startEvent TStartEvent) GetIncomingAssociation() []string {
	return startEvent.IncomingAssociation
}

func (startEvent TStartEvent) GetOutgoingAssociation() []string {
	return startEvent.OutgoingAssociation
}

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

func (endEvent TEndEvent) GetId() string {
	return endEvent.Id
}

func (endEvent TEndEvent) GetName() string {
	return endEvent.Name
}

func (endEvent TEndEvent) GetIncomingAssociation() []string {
	return endEvent.IncomingAssociation
}

func (endEvent TEndEvent) GetOutgoingAssociation() []string {
	return endEvent.OutgoingAssociation
}

func (endEvent TEndEvent) GetType() ElementType {
	return ElementTypeEndEvent
}

func (serviceTask TServiceTask) GetId() string {
	return serviceTask.Id
}

func (serviceTask TServiceTask) GetName() string {
	return serviceTask.Name
}

func (serviceTask TServiceTask) GetIncomingAssociation() []string {
	return serviceTask.IncomingAssociation
}

func (serviceTask TServiceTask) GetOutgoingAssociation() []string {
	return serviceTask.OutgoingAssociation
}

func (serviceTask TServiceTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (serviceTask TServiceTask) GetInputMapping() []extensions.TIoMapping {
	return serviceTask.Input
}

func (serviceTask TServiceTask) GetOutputMapping() []extensions.TIoMapping {
	return serviceTask.Output
}

func (serviceTask TServiceTask) GetTaskDefinitionType() string {
	return serviceTask.TaskDefinition.TypeName
}

func (serviceTask TServiceTask) GetAssignmentAssignee() string {
	return ""
}

func (serviceTask TServiceTask) GetAssignmentCandidateGroups() []string {
	return []string{}
}

func (businessRuleTask TBusinessRuleTask) GetId() string {
	return businessRuleTask.Id
}

func (businessRuleTask TBusinessRuleTask) GetName() string {
	return businessRuleTask.Name
}

func (businessRuleTask TBusinessRuleTask) GetIncomingAssociation() []string {
	return businessRuleTask.IncomingAssociation
}

func (businessRuleTask TBusinessRuleTask) GetOutgoingAssociation() []string {
	return businessRuleTask.OutgoingAssociation
}

func (businessRuleTask TBusinessRuleTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (businessRuleTask TBusinessRuleTask) GetInputMapping() []extensions.TIoMapping {
	return businessRuleTask.Input
}

func (businessRuleTask TBusinessRuleTask) GetOutputMapping() []extensions.TIoMapping {
	return businessRuleTask.Output
}

func (businessRuleTask TBusinessRuleTask) GetTaskDefinitionType() string {
	return businessRuleTask.TaskDefinition.TypeName
}

func (businessRuleTask TBusinessRuleTask) GetAssignmentAssignee() string {
	return ""
}

func (businessRuleTask TBusinessRuleTask) GetAssignmentCandidateGroups() []string {
	return []string{}
}

func (sendTask TSendTask) GetId() string {
	return sendTask.Id
}

func (sendTask TSendTask) GetName() string {
	return sendTask.Name
}

func (sendTask TSendTask) GetIncomingAssociation() []string {
	return sendTask.IncomingAssociation
}

func (sendTask TSendTask) GetOutgoingAssociation() []string {
	return sendTask.OutgoingAssociation
}

func (sendTask TSendTask) GetType() ElementType {
	return ElementTypeServiceTask
}

func (sendTask TSendTask) GetInputMapping() []extensions.TIoMapping {
	return sendTask.Input
}

func (sendTask TSendTask) GetOutputMapping() []extensions.TIoMapping {
	return sendTask.Output
}

func (sendTask TSendTask) GetTaskDefinitionType() string {
	return sendTask.TaskDefinition.TypeName
}

func (sendTask TSendTask) GetAssignmentAssignee() string {
	return ""
}

func (sendTask TSendTask) GetAssignmentCandidateGroups() []string {
	return []string{}
}

func (userTask TUserTask) GetId() string {
	return userTask.Id
}

func (userTask TUserTask) GetName() string {
	return userTask.Name
}

func (userTask TUserTask) GetIncomingAssociation() []string {
	return userTask.IncomingAssociation
}

func (userTask TUserTask) GetOutgoingAssociation() []string {
	return userTask.OutgoingAssociation
}

func (userTask TUserTask) GetType() ElementType {
	return ElementTypeUserTask
}

func (userTask TUserTask) GetInputMapping() []extensions.TIoMapping {
	return userTask.Input
}

func (userTask TUserTask) GetOutputMapping() []extensions.TIoMapping {
	return userTask.Output
}

func (userTask TUserTask) GetTaskDefinitionType() string {
	return "user-task-type"
}

func (userTask TUserTask) GetAssignmentAssignee() string {
	return userTask.AssignmentDefinition.Assignee
}

func (userTask TUserTask) GetAssignmentCandidateGroups() []string {
	return userTask.AssignmentDefinition.GetCandidateGroups()
}

func (parallelGateway TParallelGateway) GetId() string {
	return parallelGateway.Id
}

func (parallelGateway TParallelGateway) GetName() string {
	return parallelGateway.Name
}

func (parallelGateway TParallelGateway) GetIncomingAssociation() []string {
	return parallelGateway.IncomingAssociation
}

func (parallelGateway TParallelGateway) GetOutgoingAssociation() []string {
	return parallelGateway.OutgoingAssociation
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

func (exclusiveGateway TExclusiveGateway) GetId() string {
	return exclusiveGateway.Id
}

func (exclusiveGateway TExclusiveGateway) GetName() string {
	return exclusiveGateway.Name
}

func (exclusiveGateway TExclusiveGateway) GetIncomingAssociation() []string {
	return exclusiveGateway.IncomingAssociation
}

func (exclusiveGateway TExclusiveGateway) GetOutgoingAssociation() []string {
	return exclusiveGateway.OutgoingAssociation
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

func (intermediateCatchEvent TIntermediateCatchEvent) GetId() string {
	return intermediateCatchEvent.Id
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetName() string {
	return intermediateCatchEvent.Name
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetIncomingAssociation() []string {
	return intermediateCatchEvent.IncomingAssociation
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetOutgoingAssociation() []string {
	return intermediateCatchEvent.OutgoingAssociation
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetType() ElementType {
	return ElementTypeIntermediateCatchEvent
}

// -------------------------------------------------------------------------

func (eventBasedGateway TEventBasedGateway) GetId() string {
	return eventBasedGateway.Id
}

func (eventBasedGateway TEventBasedGateway) GetName() string {
	return eventBasedGateway.Name
}

func (eventBasedGateway TEventBasedGateway) GetIncomingAssociation() []string {
	return eventBasedGateway.IncomingAssociation
}

func (eventBasedGateway TEventBasedGateway) GetOutgoingAssociation() []string {
	return eventBasedGateway.OutgoingAssociation
}

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

func (intermediateThrowEvent TIntermediateThrowEvent) GetId() string {
	return intermediateThrowEvent.Id
}

func (intermediateThrowEvent TIntermediateThrowEvent) GetName() string {
	return intermediateThrowEvent.Name
}

func (intermediateThrowEvent TIntermediateThrowEvent) GetIncomingAssociation() []string {
	return intermediateThrowEvent.IncomingAssociation
}

func (intermediateThrowEvent TIntermediateThrowEvent) GetOutgoingAssociation() []string {
	// by specification, not supported
	return nil
}

func (intermediateThrowEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}

func (inclusiveGateway TInclusiveGateway) GetId() string {
	return inclusiveGateway.Id
}

func (inclusiveGateway TInclusiveGateway) GetName() string {
	return inclusiveGateway.Name
}

func (inclusiveGateway TInclusiveGateway) GetIncomingAssociation() []string {
	return inclusiveGateway.IncomingAssociation
}

func (inclusiveGateway TInclusiveGateway) GetOutgoingAssociation() []string {
	return inclusiveGateway.OutgoingAssociation
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
