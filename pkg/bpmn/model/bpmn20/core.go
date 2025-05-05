package bpmn20

import (
	"html"
	"reflect"
	"strings"
)

// TDocumentation  BPMN elements that inherit from the BaseElement will have the capability,
// through the TDocumentation element, to have one (1) or more text descriptions
// of that element.
type TDocumentation struct {
	// Text attribute is used to capture the text descriptions of a
	// BPMN element.
	Text string `xml:"text,text"`

	// Format attribute identifies the format of the text. It MUST follow
	// the mime-type format. The default is "text/plain".
	Format string `xml:"textFormat,attr"`
}

func (documentation *TDocumentation) GetText() string {
	return documentation.Text
}

func (documentation *TDocumentation) GetFormat() string {
	return documentation.Format
}

type TBaseElement struct {
	// Id attribute is used to uniquely identify BPMN elements. The id is
	// REQUIRED if this element is referenced or intended to be referenced by
	// something else. If the element is not currently referenced and is never
	// intended to be referenced, the id MAY be omitted.
	Id string `xml:"id,attr"`

	// Documentation attribute is used to annotate the BPMN element, such as descriptions
	// and other documentation.
	Documentation []TDocumentation `xml:"documentation"`
}

func (baseElement *TBaseElement) GetId() string {
	return baseElement.Id
}
func (baseElement *TBaseElement) GetDocumentation() []Documentation {
	var docs []Documentation
	for _, doc := range baseElement.Documentation {
		docs = append(docs, &doc)
	}
	return docs
}

func collectBaseElements(element interface{}, refs *map[string]BaseElement) error {
	val := reflect.ValueOf(element)

	// If c is a pointer receiver, adjust:
	baseElement, ok := val.Interface().(BaseElement)
	if ok {
		// already registered
		if _, ok2 := (*refs)[baseElement.GetId()]; ok2 {
			return nil
		}
		(*refs)[baseElement.GetId()] = baseElement
	}

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if !val.IsValid() || val.Kind() != reflect.Struct {
		return nil // Skip invalid or non-struct values
	}

	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		if fieldVal.Kind() == reflect.Slice {
			for j := 0; j < fieldVal.Len(); j++ {
				arrEl := fieldVal.Index(j)
				if !arrEl.CanInterface() || arrEl.Kind() != reflect.Struct {
					continue
				}
				//aei := arrEl.Interface()
				var err = collectBaseElements(arrEl.Addr().Interface(), refs)
				if err != nil {
					return err
				}
			}
		} else {
			if !fieldVal.CanInterface() || fieldVal.Kind() != reflect.Struct {
				continue
			}
			//fvi := fieldVal.Interface()
			var err = collectBaseElements(fieldVal.Addr().Interface(), refs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Documentation interface {
	GetText() string
	GetFormat() string
}

type ResolvableReferences interface {
	resolveReferences(refs *map[string]BaseElement) error
}

// BaseElement is the abstract super class/interface for most BPMN elements.
// It provides the attributes id and documentation, which other elements will inherit
type BaseElement interface {
	GetId() string
	GetDocumentation() []Documentation
	//collectBaseElements(refs *map[string]BaseElement)
}

type TRootElementsContainer struct {
	Process  TProcess   `xml:"process"`
	Messages []TMessage `xml:"message"`
}

type TDefinitions struct {
	TBaseElement
	TRootElementsContainer
	Name               string `xml:"name,attr"`
	TargetNamespace    string `xml:"targetNamespace,attr"`
	ExpressionLanguage string `xml:"expressionLanguage,attr"`
	TypeLanguage       string `xml:"typeLanguage,attr"`
	Exporter           string `xml:"exporter,attr"`
	ExporterVersion    string `xml:"exporterVersion,attr"`
	baseElements       map[string]BaseElement
}

type TMessage struct {
	Id   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

type TCallableElement struct {
	TBaseElement
	Name string `xml:"name,attr"`
}

type FlowElement interface {
	BaseElement
	GetName() string
	GetType() ElementType
}

type TFlowElement struct {
	TBaseElement
	Name string `xml:"name,attr"`
}

func (fe *TFlowElement) GetName() string {
	return fe.Name
}
func (fe *TFlowElement) GetType() ElementType {
	return ElementTypeSequenceFlow
}

// FlowNode is the abstract super class/interface that provides a single element as the source and target Sequence Flow associations.
// Central abstraction used by the engine to traverse through the process flows.
type FlowNode interface {
	FlowElement
	GetIncomingAssociation() []SequenceFlow
	GetOutgoingAssociation() []SequenceFlow
}

type TFlowNode struct {
	TFlowElement
	IncomingAssociation []string `xml:"incoming"`
	OutgoingAssociation []string `xml:"outgoing"`
	incomingRefs        []SequenceFlow
	outgoingRefs        []SequenceFlow
}

func (flowNode *TFlowNode) GetIncomingAssociation() []SequenceFlow {
	return flowNode.incomingRefs
}

func (flowNode *TFlowNode) GetOutgoingAssociation() []SequenceFlow {
	return flowNode.outgoingRefs
}

type TSequenceFlow struct {
	TFlowElement
	SourceRefId         string      `xml:"sourceRef,attr"`
	TargetRefId         string      `xml:"targetRef,attr"`
	ConditionExpression TExpression `xml:"conditionExpression"`
	sourceRef           FlowNode
	targetRef           FlowNode
}

type SequenceFlow interface {
	FlowElement
	GetSourceRef() FlowNode
	GetTargetRef() FlowNode
	GetConditionExpression() string
}

type TExpression struct {
	Text string `xml:",innerxml" defelt:""`
}

func (sequenceFlow *TSequenceFlow) GetSourceRef() FlowNode {
	return sequenceFlow.sourceRef
}
func (sequenceFlow *TSequenceFlow) GetTargetRef() FlowNode {
	return sequenceFlow.targetRef
}
func (sequenceFlow *TSequenceFlow) GetConditionExpression() string {
	// GetConditionExpression returns the embedded expression. There will be a panic thrown, in case none exists!
	return strings.TrimSpace(html.UnescapeString(sequenceFlow.ConditionExpression.Text))
}
