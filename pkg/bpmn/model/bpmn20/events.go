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
	Output           []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

func (startEvent TStartEvent) GetType() ElementType {
	return ElementTypeStartEvent
}

func (startEvent TStartEvent) GetOutputMapping() []extensions.TIoMapping {
	return startEvent.Output
}

func (startEvent *TStartEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		MessageEventDefinition *TMessageEventDefinition `xml:"messageEventDefinition"`
		TimerEventDefinition   *TTimerEventDefinition   `xml:"timerEventDefinition"`
		ErrorEventDefinition   *TErrorEventDefinition   `xml:"errorEventDefinition"`
		IsInterrupting         *bool                    `xml:"isInterrupting,attr"`
		ParallelMultiple       bool                     `xml:"parallelMultiple,attr"`
		Output                 []extensions.TIoMapping  `xml:"extensionElements>ioMapping>output"`
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
	startEvent.Output = tempStruct.Output
	startEvent.EventDefinitions = make([]EventDefinition, 0)
	if tempStruct.TimerEventDefinition != nil {
		startEvent.EventDefinitions = append(startEvent.EventDefinitions, *tempStruct.TimerEventDefinition)
	}
	if tempStruct.MessageEventDefinition != nil {
		startEvent.EventDefinitions = append(startEvent.EventDefinitions, *tempStruct.MessageEventDefinition)
	}
	if tempStruct.ErrorEventDefinition != nil {
		startEvent.EventDefinitions = append(startEvent.EventDefinitions, *tempStruct.ErrorEventDefinition)
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

func (TTerminateEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}

func (endEvent *TEndEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		TEvent
		TerminateEventDefinition *TTerminateEventDefinition `xml:"terminateEventDefinition"`
		MessageEventDefinition   *TMessageEventDefinition   `xml:"messageEventDefinition"`
		TErrorEventDefinition    TErrorEventDefinition      `xml:"errorEventDefinition"`
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
	if tempStruct.TErrorEventDefinition.Id != nil {
		endEvent.EventDefinitions = append(endEvent.EventDefinitions, tempStruct.TErrorEventDefinition)
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

func (TUnsupportedEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}

func isEventDefinitionElement(localName string) bool {
	const suffix = "EventDefinition"
	return len(localName) > len(suffix) && localName[len(localName)-len(suffix):] == suffix
}

type TIntermediateCatchEvent struct {
	TEvent
	EventDefinition  EventDefinition
	ParallelMultiple bool `xml:"parallelMultiple"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/pbinitiative/zenbpm-bpmn-moddle
	Input  []extensions.TIoMapping `xml:"extensionElements>ioMapping>input"`
	Output []extensions.TIoMapping `xml:"extensionElements>ioMapping>output"`
}

// UnmarshalXML decodes an intermediate catch event from its BPMN XML representation.
func (intermediateCatchEvent *TIntermediateCatchEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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
	intermediateCatchEvent.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition != nil:
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		intermediateCatchEvent.EventDefinition = *tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition != nil:
		intermediateCatchEvent.EventDefinition = *tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition != nil:
		intermediateCatchEvent.EventDefinition = *tempStruct.LinkEventDefinition
	default:
		for _, u := range tempStruct.UnknownEventDefinitions {
			if isEventDefinitionElement(u.XMLName.Local) {
				intermediateCatchEvent.EventDefinition = TUnsupportedEventDefinition{Name: u.XMLName.Local, Id: u.Id}
				break
			}
		}
	}
	intermediateCatchEvent.ParallelMultiple = tempStruct.ParallelMultiple
	intermediateCatchEvent.Input = tempStruct.Input
	intermediateCatchEvent.Output = tempStruct.Output
	return nil
}

// GetType returns the element type for an intermediate catch event.
func (intermediateCatchEvent TIntermediateCatchEvent) GetType() ElementType {
	return ElementTypeIntermediateCatchEvent
}

// GetInputMapping returns the input mappings configured for the intermediate catch event.
func (intermediateCatchEvent TIntermediateCatchEvent) GetInputMapping() []extensions.TIoMapping {
	return intermediateCatchEvent.Input
}

// GetOutputMapping returns the output mappings configured for the intermediate catch event.
func (intermediateCatchEvent TIntermediateCatchEvent) GetOutputMapping() []extensions.TIoMapping {
	return intermediateCatchEvent.Output
}

type TIntermediateThrowEvent struct {
	TEvent
	EventDefinition EventDefinition
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/pbinitiative/zenbpm-bpmn-moddle
	TaskDefinition extensions.TTaskDefinition `xml:"extensionElements>taskDefinition"`
	Input          []extensions.TIoMapping    `xml:"extensionElements>ioMapping>input"`
	Output         []extensions.TIoMapping    `xml:"extensionElements>ioMapping>output"`
}

// GetInputMapping returns the input mappings configured for the intermediate throw event.
func (intermediateThrowEvent TIntermediateThrowEvent) GetInputMapping() []extensions.TIoMapping {
	return intermediateThrowEvent.Input
}

// GetOutputMapping returns the output mappings configured for the intermediate throw event.
func (intermediateThrowEvent TIntermediateThrowEvent) GetOutputMapping() []extensions.TIoMapping {
	return intermediateThrowEvent.Output
}

// GetTaskType returns the task type configured for the intermediate throw event.
func (intermediateThrowEvent TIntermediateThrowEvent) GetTaskType() string {
	return intermediateThrowEvent.TaskDefinition.TypeName
}

// UnmarshalXML decodes an intermediate throw event from its BPMN XML representation.
func (intermediateThrowEvent *TIntermediateThrowEvent) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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
	intermediateThrowEvent.TEvent = tempStruct.TEvent
	switch {
	case tempStruct.MessageEventDefinition != nil:
		tempStruct.MessageEventDefinition.input = tempStruct.Input
		tempStruct.MessageEventDefinition.output = tempStruct.Output
		intermediateThrowEvent.EventDefinition = *tempStruct.MessageEventDefinition
	case tempStruct.TimerEventDefinition != nil:
		intermediateThrowEvent.EventDefinition = *tempStruct.TimerEventDefinition
	case tempStruct.LinkEventDefinition != nil:
		intermediateThrowEvent.EventDefinition = *tempStruct.LinkEventDefinition
	default:
		for _, u := range tempStruct.UnknownEventDefinitions {
			if isEventDefinitionElement(u.XMLName.Local) {
				intermediateThrowEvent.EventDefinition = TUnsupportedEventDefinition{Name: u.XMLName.Local, Id: u.Id}
				break
			}
		}
	}
	intermediateThrowEvent.Input = tempStruct.Input
	intermediateThrowEvent.Output = tempStruct.Output
	intermediateThrowEvent.TaskDefinition = tempStruct.TaskDefinition
	return nil
}

// GetType returns the element type for an intermediate throw event.
func (intermediateThrowEvent TIntermediateThrowEvent) GetType() ElementType {
	return ElementTypeIntermediateThrowEvent
}

type TBoundaryEvent struct {
	TEvent
	EventDefinition EventDefinition
	AttachedToRef   string `xml:"attachedToRef,attr"`
	CancellActivity bool   `xml:"cancelActivity,attr"`
	// BPMN 2.0 Unorthodox elements. Part of the extensions elements see https://github.com/pbinitiative/zenbpm-bpmn-moddle
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

func (TMessageEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}
func (d TMessageEventDefinition) GetId() string { return *d.Id }
func (d TMessageEventDefinition) GetType() ElementType {
	return ElementTypeIntermediateMessageThrowEvent
}
func (d TMessageEventDefinition) GetInputMapping() []extensions.TIoMapping  { return d.input }
func (d TMessageEventDefinition) GetOutputMapping() []extensions.TIoMapping { return d.output }

type TTimerEventDefinition struct {
	Id           *string    `xml:"id,attr"`
	TimeDuration *TTimeInfo `xml:"timeDuration"`
	TimeDate     *TTimeInfo `xml:"timeDate"`
	TimeCycle    *TTimeInfo `xml:"timeCycle"`
}

func (TTimerEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}
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

func (TLinkEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}

type TTimeInfo struct {
	XMLText string `xml:",innerxml"`
}

type TErrorEventDefinition struct {
	Id       *string `xml:"id,attr"`
	Name     string  `xml:"name,attr"`
	ErrorRef *string `xml:"errorRef,attr"`
}

func (TErrorEventDefinition) eventDefinition() {
	// Marker method to satisfy EventDefinition.
}
