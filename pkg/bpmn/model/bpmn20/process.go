package bpmn20

import "encoding/xml"

type TFlowElementsContainer struct {
	StartEvents            []TStartEvent             `xml:"startEvent"`
	EndEvents              []TEndEvent               `xml:"endEvent"`
	SequenceFlows          []TSequenceFlow           `xml:"sequenceFlow"`
	ServiceTasks           []TServiceTask            `xml:"serviceTask"`
	UserTasks              []TUserTask               `xml:"userTask"`
	BusinessRuleTask       []TBusinessRuleTask       `xml:"businessRuleTask"`
	SendTask               []TSendTask               `xml:"sendTask"`
	ReceiveTask            []TReceiveTask            `xml:"receiveTask"`
	ParallelGateway        []TParallelGateway        `xml:"parallelGateway"`
	ExclusiveGateway       []TExclusiveGateway       `xml:"exclusiveGateway"`
	EventBasedGateway      []TEventBasedGateway      `xml:"eventBasedGateway"`
	InclusiveGateway       []TInclusiveGateway       `xml:"inclusiveGateway"`
	IntermediateCatchEvent []TIntermediateCatchEvent `xml:"intermediateCatchEvent"`
	IntermediateThrowEvent []TIntermediateThrowEvent `xml:"intermediateThrowEvent"`
	CallActivity           []TCallActivity           `xml:"callActivity"`
	BoundaryEvent          []TBoundaryEvent          `xml:"boundaryEvent"`
	SubProcess             []TSubProcess             `xml:"subProcess"`
	// Catches any XML element not matched by the fields above.
	// Used to detect unsupported BPMN elements at validation time.
	UnknownElements []TUnknownElement `xml:",any"`

	// Subtree indices built by ResolveReferences; IDs and slice membership must remain
	// unchanged afterward. Boundary events are intentionally excluded.
	flowNodesByID     map[string]FlowNode
	internalTasksByID map[string]InternalTask
}

// TUnknownElement captures any XML element not explicitly handled by TFlowElementsContainer.
// Presence of incoming or outgoing child elements indicates a flow node (i.e. unsupported).
type TUnknownElement struct {
	XMLName  xml.Name
	Id       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type TProcess struct {
	TCallableElement
	TFlowElementsContainer
	ProcessType                  string `xml:"processType,attr"`
	IsClosed                     bool   `xml:"isClosed,attr"`
	IsExecutable                 bool   `xml:"isExecutable,attr"`
	DefinitionalCollaborationRef string `xml:"definitionalCollaborationRef,attr"`
}

// GetFlowNodeById returns a flow node from this process subtree.
// Boundary events are excluded. ResolveReferences must be called first.
func (p *TProcess) GetFlowNodeById(id string) FlowNode {
	if id == "" {
		return nil
	}
	return p.flowNodesByID[id]
}

// GetInternalTaskById returns an internal task from this process subtree.
// ResolveReferences must be called first.
func (p *TProcess) GetInternalTaskById(id string) InternalTask {
	if id == "" {
		return nil
	}
	return p.internalTasksByID[id]
}

// GetSubprocessAndStartEventById recursively searches for a start event by ID within this process and all nested subprocesses.
func (p *TProcess) GetSubprocessAndStartEventById(id string) (*TSubProcess, *TStartEvent) {
	for _, subProcess := range p.SubProcess {
		// Check immediate subprocess start events
		for _, startEvent := range subProcess.StartEvents {
			if startEvent.GetId() == id {
				return &subProcess, &startEvent
			}
		}
		// Recursively search nested subprocesses within this subprocess
		if nestedSubprocess, startEvent := subProcess.GetSubprocessAndStartEventById(id); nestedSubprocess != nil {
			return nestedSubprocess, startEvent
		}
	}
	return nil, nil
}

type ElementType string

type TDefaultFlowExtension struct {
	DefaultFlowId string       `xml:"default,attr" default:""`
	DefaultFlow   SequenceFlow `idField:"DefaultFlowId"`
}

type DefaultFlowExtension interface {
	GetDefaultFlow() SequenceFlow
}

func (dfe TDefaultFlowExtension) GetDefaultFlow() SequenceFlow { return dfe.DefaultFlow }
