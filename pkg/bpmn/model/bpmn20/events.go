package bpmn20

import "github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"

const (
	ElementTypeStartEvent             ElementType = "START_EVENT"
	ElementTypeEndEvent               ElementType = "END_EVENT"
	ElementTypeIntermediateCatchEvent ElementType = "INTERMEDIATE_CATCH_EVENT"
	ElementTypeIntermediateThrowEvent ElementType = "INTERMEDIATE_THROW_EVENT"
)

type TEvent struct {
	TActivity
}

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

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

func (endEvent TEndEvent) GetType() ElementType { return ElementTypeEndEvent }

func (intermediateCatchEvent TIntermediateCatchEvent) GetType() ElementType {
	return ElementTypeIntermediateCatchEvent
}

func (intermediateCatchEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}
