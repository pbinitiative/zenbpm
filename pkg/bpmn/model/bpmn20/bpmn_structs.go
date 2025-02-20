package bpmn20

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20/extensions"
)

//type TProcess struct {
//	TCallableElement
//	//Id                           string                    `xml:"id,attr"`
//	//Name                         string                    `xml:"name,attr"`
//	ProcessType                  string         `xml:"processType,attr"`
//	IsClosed                     bool           `xml:"isClosed,attr"`
//	IsExecutable                 bool           `xml:"isExecutable,attr"`
//	DefinitionalCollaborationRef string         `xml:"definitionalCollaborationRef,attr"`
//	FlowElements                 []TFlowElement `xml:"-"`
//	//StartEvents                  []TStartEvent             `xml:"startEvent"`
//	//EndEvents                    []TEndEvent               `xml:"endEvent"`
//	//SequenceFlows                []TSequenceFlow           `xml:"sequenceFlow"`
//	//ServiceTasks                 []TServiceTask            `xml:"serviceTask"`
//	//UserTasks                    []TUserTask               `xml:"userTask"`
//	//ParallelGateway              []TParallelGateway        `xml:"parallelGateway"`
//	//ExclusiveGateway             []TExclusiveGateway       `xml:"exclusiveGateway"`
//	//IntermediateCatchEvent       []TIntermediateCatchEvent `xml:"intermediateCatchEvent"`
//	//IntermediateThrowEvent        []TIntermediateThrowEvent `xml:"intermediateThrowEvent"`
//	//EventBasedGateway            []TEventBasedGateway      `xml:"eventBasedGateway"`
//	//InclusiveGateway             []TInclusiveGateway       `xml:"inclusiveGateway"`
//}

func init() {
	registeredRootElements[RootElementMessage] = func() RootElement { return &TMessage{} }
}

func (d *RootElementsType) Messages() *[]TMessage {
	var messages []TMessage
	for _, re := range []RootElement(*d) {
		if re.getRootElementType() == RootElementMessage {
			messages = append(messages, re.(TMessage))
		}
	}
	return &messages
}

//type TSequenceFlow struct {
//	Id                  string        `xml:"id,attr"`
//	Name                string        `xml:"name,attr"`
//	SourceRef           string        `xml:"sourceRef,attr"`
//	TargetRef           string        `xml:"targetRef,attr"`
//	ConditionExpression []TExpression `xml:"conditionExpression"`
//}

type TExpression struct {
	Text string `xml:",innerxml"`
}

type TStartEvent struct {
	TEvent
	IsInterrupting   bool `xml:"isInterrupting,attr"`
	ParallelMultiple bool `xml:"parallelMultiple,attr"`
}

type TEndEvent = TEvent

type TServiceTask struct {
	TTask
	Default        string                     `xml:"default,attr"`
	OperationRef   string                     `xml:"operationRef,attr"`
	Implementation string                     `xml:"implementation,attr"`
	Input          []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
	Output         []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
}

type TUserTask struct {
	TTask
	Input                []extensions.TIoMapping          `xml:"extensionElements>ioMapping>input"`
	Output               []extensions.TIoMapping          `xml:"extensionElements>ioMapping>output"`
	AssignmentDefinition extensions.TAssignmentDefinition `xml:"extensionElements>assignmentDefinition"`
}

type TParallelGateway struct {
	TGateway
}

type TExclusiveGateway struct {
	TGateway
}

type TIntermediateCatchEvent struct {
	TEvent
	MessageEventDefinition TMessageEventDefinition `xml:"messageEventDefinition"`
	TimerEventDefinition   TTimerEventDefinition   `xml:"timerEventDefinition"`
	LinkEventDefinition    TLinkEventDefinition    `xml:"linkEventDefinition"`
	ParallelMultiple       bool                    `xml:"parallelMultiple"`
	Output                 []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

type TIntermediateThrowEvent struct {
	TEvent
	LinkEventDefinition TLinkEventDefinition    `xml:"linkEventDefinition"`
	Output              []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
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

type TMessage struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

func (t TMessage) getRootElementType() RootElementType { return RootElementMessage }

type TTimeDuration struct {
	XMLText string `xml:",innerxml"`
}

type TInclusiveGateway struct {
	Id                  string   `xml:"id,attr"`
	Name                string   `xml:"name,attr"`
	IncomingAssociation []string `xml:"incoming"`
	OutgoingAssociation []string `xml:"outgoing"`
}

const (
	//RootElementProcess RootElementType = "process"
	RootElementMessage RootElementType = "message"
)
