package bpmn20

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

func (doc TDocumentation) GetText() string {
	return doc.Text
}

func (doc TDocumentation) GetFormat() string {
	return doc.Format
}

type TBaseElement struct {
	// This attribute is used to uniquely identify BPMN elements. The id is
	// REQUIRED if this element is referenced or intended to be referenced by
	// something else. If the element is not currently referenced and is never
	// intended to be referenced, the id MAY be omitted.
	Id string `xml:"id,attr"`

	// This attribute is used to annotate the BPMN element, such as descriptions
	// and other documentation.
	Documentation []TDocumentation `xml:"documentation"`
}

func (t TBaseElement) GetId() string {
	return t.Id
}
func (t TBaseElement) GetDocumentation() []Documentation {
	var docs []Documentation
	for _, doc := range t.Documentation {
		docs = append(docs, doc)
	}
	return docs
}

type Documentation interface {
	GetText() string
	GetFormat() string
}

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

func (fn TFlowNode) GetIncomingAssociation() []string {
	return fn.IncomingAssociation
}

func (fn TFlowNode) GetOutgoingAssociation() []string {
	return fn.OutgoingAssociation
}

//type TFlowElementsContainer

type TExpression struct {
	Text string `xml:",innerxml"`
}
