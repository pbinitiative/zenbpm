package bpmn20

import (
	"encoding/xml"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

const (
	ElementTypeStartEvent             ElementType = "START_EVENT"
	ElementTypeEndEvent               ElementType = "END_EVENT"
	ElementTypeIntermediateCatchEvent ElementType = "INTERMEDIATE_CATCH_EVENT"
	ElementTypeIntermediateThrowEvent ElementType = "INTERMEDIATE_THROW_EVENT"

	ElementTypeIntermediateMessageThrowEvent ElementType = "INTERMEDIATE_MESSAGE_THROW_EVENT"
)

type TEvent struct {
	TActivity
}

type TStartEvent struct {
	TEvent
	IsInterrupting   bool `xml:"isInterrupting,attr"`
	ParallelMultiple bool `xml:"parallelMultiple,attr"`
}

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

type TEndEvent struct {
	TEvent
}

func (endEvent TEndEvent) GetType() ElementType { return ElementTypeEndEvent }

type EventDefinition interface {
	eventDefinition()
}

type TIntermediateCatchEvent struct {
	TEvent
	EventDefinition  EventDefinition
	ParallelMultiple bool `xml:"parallelMultiple"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (definitions *TIntermediateCatchEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition TMessageEventDefinition `xml:"messageEventDefinition"`
		TimerEventDefinition   TTimerEventDefinition   `xml:"timerEventDefinition"`
		LinkEventDefinition    TLinkEventDefinition    `xml:"linkEventDefinition"`
		ParallelMultiple       bool                    `xml:"parallelMultiple"`
		Input                  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
		Output                 []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	definitions.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition.Id != "":
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		definitions.EventDefinition = tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition.Id != "":
		definitions.EventDefinition = tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition.Id != "":
		definitions.EventDefinition = tempStruct.LinkEventDefinition
	}
	definitions.ParallelMultiple = tempStruct.ParallelMultiple
	definitions.Output = tempStruct.Output
	return nil
}

func (intermediateCatchEvent TIntermediateCatchEvent) GetType() ElementType {
	return ElementTypeIntermediateCatchEvent
}

type TIntermediateThrowEvent struct {
	TEvent
	EventDefinition EventDefinition
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (definitions *TIntermediateThrowEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition TMessageEventDefinition `xml:"messageEventDefinition"`
		TimerEventDefinition   TTimerEventDefinition   `xml:"timerEventDefinition"`
		LinkEventDefinition    TLinkEventDefinition    `xml:"linkEventDefinition"`
		Input                  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
		Output                 []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	definitions.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition.Id != "":
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		definitions.EventDefinition = tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition.Id != "":
		definitions.EventDefinition = tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition.Id != "":
		definitions.EventDefinition = tempStruct.LinkEventDefinition
	}
	definitions.Output = tempStruct.Output
	return nil
}

func (intermediateCatchEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}

type TMessageEventDefinition struct {
	TFlowNode
	Id             string                     `xml:"id,attr"`
	MessageRef     string                     `xml:"messageRef,attr"`
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
	input          []extensions.TIoMapping
	output         []extensions.TIoMapping
}

func (TMessageEventDefinition) eventDefinition() {}
func (d TMessageEventDefinition) GetId() string  { return d.Id }
func (d TMessageEventDefinition) GetType() ElementType {
	return ElementTypeIntermediateMessageThrowEvent
}
func (d TMessageEventDefinition) GetTaskType() string                       { return d.TaskDefinition.TypeName }
func (d TMessageEventDefinition) GetInputMapping() []extensions.TIoMapping  { return d.input }
func (d TMessageEventDefinition) GetOutputMapping() []extensions.TIoMapping { return d.output }

type TTimerEventDefinition struct {
	Id           string        `xml:"id,attr"`
	TimeDuration TTimeDuration `xml:"timeDuration"`
}

func (TTimerEventDefinition) eventDefinition() {}

type TLinkEventDefinition struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

func (TLinkEventDefinition) eventDefinition() {}

type TTimeDuration struct {
	XMLText string `xml:",innerxml"`
}
