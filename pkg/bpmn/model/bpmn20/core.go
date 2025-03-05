package bpmn20

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

func (documentation TDocumentation) GetText() string {
	return documentation.Text
}

func (documentation TDocumentation) GetFormat() string {
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

func (baseElement TBaseElement) GetId() string {
	return baseElement.Id
}
func (baseElement TBaseElement) GetDocumentation() []Documentation {
	var docs []Documentation
	for _, doc := range baseElement.Documentation {
		docs = append(docs, doc)
	}
	return docs
}

type Documentation interface {
	GetText() string
	GetFormat() string
}

// BaseElement is the abstract super class/interface for most BPMN elements.
// It provides the attributes id and documentation, which other elements will inherit
type BaseElement interface {
	GetId() string
	GetDocumentation() []Documentation
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

func (fe TFlowElement) GetName() string {
	return fe.Name
}

type TSequenceFlow struct {
	TFlowElement
	SourceRef           string        `xml:"sourceRef,attr"`
	TargetRef           string        `xml:"targetRef,attr"`
	ConditionExpression []TExpression `xml:"conditionExpression"`
}

// FlowNode is the abstract super class/interface that provides a single element as the source and target Sequence Flow associations.
// Central abstraction used by the engine to traverse through the process flows.
type FlowNode interface {
	FlowElement
	GetIncomingAssociation() []string
	GetOutgoingAssociation() []string
}

type TFlowNode struct {
	TFlowElement
	IncomingAssociation []string `xml:"incoming"`
	OutgoingAssociation []string `xml:"outgoing"`
}

func (flowNode TFlowNode) GetIncomingAssociation() []string {
	return flowNode.IncomingAssociation
}

func (flowNode TFlowNode) GetOutgoingAssociation() []string {
	return flowNode.OutgoingAssociation
}

//type TFlowElementsContainer

type TExpression struct {
	Text string `xml:",innerxml"`
}
