package bpmn20

import (
	"bytes"
	"encoding/xml"
	"io"
)

// All BPMN elements that inherit from the BaseElement will have the capability,
// through the Documentation element, to have one (1) or more text descriptions
// of that element.
type TDocumentation struct {
	// This attribute is used to capture the text descriptions of a
	// BPMN element.
	Text string `xml:"text,text"`

	// This attribute identifies the format of the text. It MUST follow
	// the mime-type format. The default is "text/plain".
	Format string `xml:"textFormat,attr"`
}

type TBaseElement struct {
	// This attribute is used to uniquely identify BPMN elements. The id is
	// REQUIRED if this element is referenced or intended to be referenced by
	// something else. If the element is not currently referenced and is never
	// intended to be referenced, the id MAY be omitted.
	Id string `xml:"id,attr"`

	// This attribute is used to annotate the BPMN element, such as descriptions
	// and other documentation.
	//docs []*TDocumentation
}

type RootElement interface {
	getRootElementType() RootElementType
}

type RootElementType string

var registeredRootElements = map[RootElementType]func() RootElement{}

type TDefinitions struct {
	TBaseElement
	Name               string           `xml:"name,attr"`
	TargetNamespace    string           `xml:"targetNamespace,attr"`
	ExpressionLanguage string           `xml:"expressionLanguage,attr"`
	TypeLanguage       string           `xml:"typeLanguage,attr"`
	Exporter           string           `xml:"exporter,attr"`
	ExporterVersion    string           `xml:"exporterVersion,attr"`
	RootElements       RootElementsType `xml:"-"`
}
type RootElementsType []RootElement

func (ret *TDefinitions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type Alias TDefinitions
	aux := &struct {
		*Alias
		RawXML []byte `xml:",innerxml"`
	}{
		Alias: (*Alias)(ret),
	}

	if err := d.DecodeElement(aux, &start); err != nil {
		return err
	}
	innerDecoder := xml.NewDecoder(bytes.NewReader(aux.RawXML))
	for {
		token, err := innerDecoder.Token()
		var re RootElement

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		switch pe := token.(type) {
		case xml.StartElement:
			tp := registeredRootElements[RootElementType(pe.Name.Local)]
			if tp != nil {
				re = tp()
			} else {
				continue
			}

			if err := innerDecoder.DecodeElement(re, &pe); err != nil {
				return err
			}
			ret.RootElements = append(ret.RootElements, re)
		}
	}
	return nil
}

type TCallableElement struct {
	TBaseElement
	Name string `xml:"name,attr"`
}

type TFlowElement struct {
	TBaseElement
	Name string `xml:"name,attr"`
}

type TSequenceFlow struct {
	TFlowElement
	SourceRef           string        `xml:"sourceRef,attr"`
	TargetRef           string        `xml:"targetRef,attr"`
	ConditionExpression []TExpression `xml:"conditionExpression"`
}
type TFlowNode struct {
	TFlowElement
	IncomingAssociation []string `xml:"incoming"`
	OutgoingAssociation []string `xml:"outgoing"`
}

//type TFlowElementsContainer
