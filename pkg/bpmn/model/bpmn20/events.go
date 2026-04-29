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
	ElementTypeBoundaryEvent          ElementType = "BOUNDARY_EVENT"

	ElementTypeIntermediateMessageThrowEvent ElementType = "INTERMEDIATE_MESSAGE_THROW_EVENT"
)

type TEvent struct {
	TFlowNode
}

type TStartEvent struct {
	TEvent
	IsInterrupting   bool `xml:"isInterrupting,attr"`
	ParallelMultiple bool `xml:"parallelMultiple,attr"`
	EventDefinitions []EventDefinition
}

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

func (startEvent *TStartEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition *TMessageEventDefinition `xml:"messageEventDefinition"`
		TimerEventDefinition   *TTimerEventDefinition   `xml:"timerEventDefinition"`
		IsInterrupting         *bool                    `xml:"isInterrupting,attr"`
		ParallelMultiple       bool                     `xml:"parallelMultiple,attr"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	startEvent.TEvent = tempStruct.TEvent
	if tempStruct.IsInterrupting == nil || *tempStruct.IsInterrupting {
		startEvent.IsInterrupting = true
	}
	startEvent.ParallelMultiple = tempStruct.ParallelMultiple
	startEvent.EventDefinitions = make([]EventDefinition, 0)
	if tempStruct.TimerEventDefinition != nil {
		startEvent.EventDefinitions = append(startEvent.EventDefinitions, *tempStruct.TimerEventDefinition)
	}
	if tempStruct.MessageEventDefinition != nil {
		startEvent.EventDefinitions = append(startEvent.EventDefinitions, *tempStruct.MessageEventDefinition)
	}
	return nil
}

type TEndEvent struct {
	TEvent
	EventDefinitions []EventDefinition
	TaskDefinition   extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
	Input            []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
	Output           []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
}

type TTerminateEventDefinition struct {
	Id *string `xml:"id,attr"`
}

func (TTerminateEventDefinition) eventDefinition() {}

func (endEvent *TEndEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		TerminateEventDefinition *TTerminateEventDefinition `xml:"terminateEventDefinition"`
		MessageEventDefinition   *TMessageEventDefinition   `xml:"messageEventDefinition"`
		TaskDefinition           extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
		Input                    []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
		Output                   []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	endEvent.TEvent = tempStruct.TEvent
	endEvent.EventDefinitions = make([]EventDefinition, 0)
	if tempStruct.TerminateEventDefinition != nil {
		endEvent.EventDefinitions = append(endEvent.EventDefinitions, *tempStruct.TerminateEventDefinition)
	}
	if tempStruct.MessageEventDefinition != nil {
		endEvent.EventDefinitions = append(endEvent.EventDefinitions, *tempStruct.MessageEventDefinition)
		endEvent.TaskDefinition = tempStruct.TaskDefinition
		endEvent.Input = tempStruct.Input
		endEvent.Output = tempStruct.Output
	}
	return nil
}

func (endEvent TEndEvent) GetType() ElementType { return ElementTypeEndEvent }

func (endEvent TEndEvent) GetTaskType() string {
	return endEvent.TaskDefinition.TypeName
}

func (endEvent TEndEvent) GetInputMapping() []extensions.TIoMapping {
	return endEvent.Input
}

func (endEvent TEndEvent) GetOutputMapping() []extensions.TIoMapping {
	return endEvent.Output
}

type EventDefinition interface {
	eventDefinition()
}

type TUnknownEventDefinition struct {
	XMLName xml.Name
	Id      string `xml:"id,attr"`
}

type TUnsupportedEventDefinition struct {
	Name string
	Id   string
}

func (TUnsupportedEventDefinition) eventDefinition() {}

func isEventDefinitionElement(localName string) bool {
	const suffix = "EventDefinition"
	return len(localName) > len(suffix) && localName[len(localName)-len(suffix):] == suffix
}

type TIntermediateCatchEvent struct {
	TEvent
	EventDefinition  EventDefinition
	ParallelMultiple bool `xml:"parallelMultiple"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (definitions *TIntermediateCatchEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition  *TMessageEventDefinition  `xml:"messageEventDefinition"`
		TimerEventDefinition    *TTimerEventDefinition    `xml:"timerEventDefinition"`
		LinkEventDefinition     *TLinkEventDefinition     `xml:"linkEventDefinition"`
		ParallelMultiple        bool                      `xml:"parallelMultiple"`
		Input                   []extensions.TIoMapping   `xml:"extensionElements>ioMapping>input"`
		Output                  []extensions.TIoMapping   `xml:"extensionElements>ioMapping>output"`
		UnknownEventDefinitions []TUnknownEventDefinition `xml:",any"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	definitions.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition != nil:
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		definitions.EventDefinition = *tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.LinkEventDefinition
	default:
		for _, u := range tempStruct.UnknownEventDefinitions {
			if isEventDefinitionElement(u.XMLName.Local) {
				definitions.EventDefinition = TUnsupportedEventDefinition{Name: u.XMLName.Local, Id: u.Id}
				break
			}
		}
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
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
	Input          []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
	Output         []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
}

func (d TIntermediateThrowEvent) GetInputMapping() []extensions.TIoMapping  { return d.Input }
func (d TIntermediateThrowEvent) GetOutputMapping() []extensions.TIoMapping { return d.Output }

func (d TIntermediateThrowEvent) GetTaskType() string { return d.TaskDefinition.TypeName }

func (definitions *TIntermediateThrowEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition  *TMessageEventDefinition   `xml:"messageEventDefinition"`
		TimerEventDefinition    *TTimerEventDefinition     `xml:"timerEventDefinition"`
		LinkEventDefinition     *TLinkEventDefinition      `xml:"linkEventDefinition"`
		TaskDefinition          extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
		Input                   []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
		Output                  []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
		UnknownEventDefinitions []TUnknownEventDefinition  `xml:",any"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	definitions.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition != nil:
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		definitions.EventDefinition = *tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.LinkEventDefinition
	default:
		for _, u := range tempStruct.UnknownEventDefinitions {
			if isEventDefinitionElement(u.XMLName.Local) {
				definitions.EventDefinition = TUnsupportedEventDefinition{Name: u.XMLName.Local, Id: u.Id}
				break
			}
		}
	}
	definitions.Output = tempStruct.Output
	definitions.TaskDefinition = tempStruct.TaskDefinition
	return nil
}

func (intermediateCatchEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}

type TBoundaryEvent struct {
	TEvent
	EventDefinition EventDefinition
	AttachedToRef   string `xml:"attachedToRef,attr"`
	CancellActivity bool   `xml:"cancelActivity,attr"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/camunda/zeebe-bpmn-moddle
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (definitions *TBoundaryEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		AttachedToRef           string                    `xml:"attachedToRef,attr"`
		CancellActivity         bool                      `xml:"cancelActivity,attr"`
		MessageEventDefinition  *TMessageEventDefinition  `xml:"messageEventDefinition"`
		TimerEventDefinition    *TTimerEventDefinition    `xml:"timerEventDefinition"`
		ErrorEventDefinition    *TErrorEventDefinition    `xml:"errorEventDefinition"`
		Output                  []extensions.TIoMapping   `xml:"extensionElements>ioMapping>output"`
		UnknownEventDefinitions []TUnknownEventDefinition `xml:",any"`
	}{CancellActivity: true}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	definitions.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.TimerEventDefinition
	case tempStruct.ErrorEventDefinition != nil:
		definitions.EventDefinition = *tempStruct.ErrorEventDefinition
	default:
		for _, u := range tempStruct.UnknownEventDefinitions {
			if isEventDefinitionElement(u.XMLName.Local) {
				definitions.EventDefinition = TUnsupportedEventDefinition{Name: u.XMLName.Local, Id: u.Id}
				break
			}
		}
	}
	definitions.Output = tempStruct.Output
	definitions.AttachedToRef = tempStruct.AttachedToRef
	definitions.CancellActivity = tempStruct.CancellActivity
	return nil
}

func (b TBoundaryEvent) GetId() string { return b.Id }
func (b TBoundaryEvent) GetType() ElementType {
	return ElementTypeBoundaryEvent
}
func (d TBoundaryEvent) GetOutputMapping() []extensions.TIoMapping { return d.Output }

type TMessageEventDefinition struct {
	TFlowNode
	Id         *string `xml:"id,attr"`
	MessageRef string  `xml:"messageRef,attr"`
	input      []extensions.TIoMapping
	output     []extensions.TIoMapping
}

func (TMessageEventDefinition) eventDefinition() {}
func (d TMessageEventDefinition) GetId() string  { return *d.Id }
func (d TMessageEventDefinition) GetType() ElementType {
	return ElementTypeIntermediateMessageThrowEvent
}
func (d TMessageEventDefinition) GetInputMapping() []extensions.TIoMapping  { return d.input }
func (d TMessageEventDefinition) GetOutputMapping() []extensions.TIoMapping { return d.output }

type TTimerEventDefinition struct {
	Id           *string    `xml:"id,attr"`
	TimeDuration *TTimeInfo `xml:"timeDuration"`
	TimeDate     *TTimeInfo `xml:"timeDate"`
	// TimeCycle    TTimeInfo `xml:"timeCycle"` // TODO: implement support for TimeCycles
}

func (TTimerEventDefinition) eventDefinition() {}
func (t TTimerEventDefinition) GetId() string {
	if t.Id == nil {
		return ""
	}
	return *t.Id
}

type TLinkEventDefinition struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

func (TLinkEventDefinition) eventDefinition() {}

type TTimeInfo struct {
	XMLText string `xml:",innerxml"`
}

type TErrorEventDefinition struct {
	Id       string  `xml:"id,attr"`
	Name     string  `xml:"name,attr"`
	ErrorRef *string `xml:"errorRef,attr"`
}

func (TErrorEventDefinition) eventDefinition() {}
