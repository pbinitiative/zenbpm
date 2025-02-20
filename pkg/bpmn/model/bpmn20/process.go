package bpmn20

type TActivity struct {
	TFlowNode
	CompletionQuantity int  `xml:"completionQuantity,attr"`
	IsForCompensation  bool `xml:"isForCompensation,attr"`
	StartQuantity      int  `xml:"startQuantity,attr"`
}

type TTask = TActivity
type TEvent = TActivity

type TGateway struct {
	TFlowNode
	GatewayDirection GatewayDirection `xml:"gatewayDirection,attr"`
}

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

func init() {
	registeredRootElements[RootElementProcess] = func() RootElement { return &TProcess{} }
}

type FlowElementsType = []TFlowElement

func (p *TProcess) getRootElementType() RootElementType { return RootElementProcess }

func (d *RootElementsType) Process() *TProcess {
	for _, re := range []RootElement(*d) {
		if re.getRootElementType() == RootElementProcess {
			return re.(*TProcess)
		}
	}
	return nil
}

const (
	RootElementProcess RootElementType = "process"
)
