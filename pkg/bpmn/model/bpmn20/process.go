package bpmn20

import "github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"

type TFlowElementsContainer struct {
	StartEvents            []TStartEvent             `xml:"startEvent"`
	EndEvents              []TEndEvent               `xml:"endEvent"`
	SequenceFlows          []TSequenceFlow           `xml:"sequenceFlow"`
	ServiceTasks           []TServiceTask            `xml:"serviceTask"`
	UserTasks              []TUserTask               `xml:"userTask"`
	ParallelGateway        []TParallelGateway        `xml:"parallelGateway"`
	ExclusiveGateway       []TExclusiveGateway       `xml:"exclusiveGateway"`
	IntermediateCatchEvent []TIntermediateCatchEvent `xml:"intermediateCatchEvent"`
	IntermediateThrowEvent []TIntermediateThrowEvent `xml:"intermediateThrowEvent"`
	EventBasedGateway      []TEventBasedGateway      `xml:"eventBasedGateway"`
	InclusiveGateway       []TInclusiveGateway       `xml:"inclusiveGateway"`
}

type TProcess struct {
	TCallableElement
	TFlowElementsContainer
	ProcessType                  string `xml:"processType,attr"`
	IsClosed                     bool   `xml:"isClosed,attr"`
	IsExecutable                 bool   `xml:"isExecutable,attr"`
	DefinitionalCollaborationRef string `xml:"definitionalCollaborationRef,attr"`
}

type Activity interface {
	FlowNode
	GetCompletionQuantity() int
	GetIsForCompensation() bool
	GetStartQuantity() int
}

type TActivity struct {
	TFlowNode
	CompletionQuantity int  `xml:"completionQuantity,attr"`
	IsForCompensation  bool `xml:"isForCompensation,attr"`
	StartQuantity      int  `xml:"startQuantity,attr"`
}
type TTask struct {
	TActivity
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

type TGateway struct {
	TFlowNode
	GatewayDirection GatewayDirection `xml:"gatewayDirection,attr"`
}

type TEvent struct {
	TActivity
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

type TUserTask struct {
	TTask
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	AssignmentDefinition extensions.TAssignmentDefinition `xml:"extensionElements>assignmentDefinition"`
}

type TInclusiveGateway struct {
	TGateway
}

type TParallelGateway struct {
	TGateway
}

type TExclusiveGateway struct {
	TGateway
}

type TEventBasedGateway struct {
	TGateway
	Instantiate      bool                  `xml:"instantiate,attr"`
	EventGatewayType EventBasedGatewayType `xml:"eventGatewayType,attr"`
}
type EventBasedGatewayType = string

const (
	EventBasedGatewayTypeExclusive EventBasedGatewayType = "Exclusive"
	EventBasedGatewayTypeParallel  EventBasedGatewayType = "Parallel"
)

type TStartEvent struct {
	TEvent
	IsInterrupting   bool `xml:"isInterrupting,attr"`
	ParallelMultiple bool `xml:"parallelMultiple,attr"`
}

type TEndEvent struct {
	TEvent
}

type TIntermediateCatchEvent struct {
	TEvent
	MessageEventDefinition TMessageEventDefinition `xml:"messageEventDefinition"`
	TimerEventDefinition   TTimerEventDefinition   `xml:"timerEventDefinition"`
	LinkEventDefinition    TLinkEventDefinition    `xml:"linkEventDefinition"`
	ParallelMultiple       bool                    `xml:"parallelMultiple"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

type TIntermediateThrowEvent struct {
	TEvent
	LinkEventDefinition TLinkEventDefinition `xml:"linkEventDefinition"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

type TMessageEventDefinition struct {
	Id         string `xml:"id,attr"`
	MessageRef string `xml:"messageRef,attr"`
}

type TTimerEventDefinition struct {
	Id           string        `xml:"id,attr"`
	TimeDuration TTimeDuration `xml:"timeDuration"`
}

type TLinkEventDefinition struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

type TTimeDuration struct {
	XMLText string `xml:",innerxml"`
}

func (activity TActivity) GetCompletionQuantity() int {
	return activity.CompletionQuantity
}
func (activity TActivity) GetIsForCompensation() bool {
	return activity.IsForCompensation
}
func (activity TActivity) GetStartQuantity() int {
	return activity.StartQuantity
}
