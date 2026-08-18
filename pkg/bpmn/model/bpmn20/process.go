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

// GetInternalTaskById returns the InternalTask with the given id by
// walking the process tree linearly. Prefer bpmn20.FindInternalTaskById
// for hot paths — it is O(1) and never returns range-loop copies.
//
// Uses for i := range so the returned pointer is stable.
// Recurses into subprocesses so an internal task inside a nested
// subprocess is reachable.
func (p *TProcess) GetInternalTaskById(id string) InternalTask {
	for i := range p.ServiceTasks {
		e := &p.ServiceTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.UserTasks {
		e := &p.UserTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.BusinessRuleTask {
		e := &p.BusinessRuleTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.SendTask {
		e := &p.SendTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.IntermediateThrowEvent {
		e := &p.IntermediateThrowEvent[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.EndEvents {
		e := &p.EndEvents[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.SubProcess {
		sp := &p.SubProcess[i]
		if res := sp.GetInternalTaskById(id); res != nil {
			return res
		}
	}

	return nil
}

// linearFindFlowNode is the legacy O(N) scan, retained as the
// benchmark baseline. Production hot paths use
// bpmn20.FindFlowNodeById, which relies on the typed index populated
// by ResolveReferences. It uses for i := range so it returns stable
// slice-element pointers.
func (p *TProcess) linearFindFlowNode(id string) FlowNode {
	c := &p.TFlowElementsContainer
	for i := range c.StartEvents {
		e := &c.StartEvents[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.EndEvents {
		e := &c.EndEvents[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.ServiceTasks {
		e := &c.ServiceTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.UserTasks {
		e := &c.UserTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.BusinessRuleTask {
		e := &c.BusinessRuleTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.SendTask {
		e := &c.SendTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.ReceiveTask {
		e := &c.ReceiveTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.ParallelGateway {
		e := &c.ParallelGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.ExclusiveGateway {
		e := &c.ExclusiveGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.EventBasedGateway {
		e := &c.EventBasedGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.InclusiveGateway {
		e := &c.InclusiveGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.IntermediateCatchEvent {
		e := &c.IntermediateCatchEvent[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.IntermediateThrowEvent {
		e := &c.IntermediateThrowEvent[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range c.CallActivity {
		e := &c.CallActivity[i]
		if e.GetId() == id {
			return e
		}
	}
	// Boundary events are intentionally not searched.
	for i := range c.SubProcess {
		sp := &c.SubProcess[i]
		if sp.GetId() == id {
			return sp
		}
		if res := sp.linearFindFlowNode(id); res != nil {
			return res
		}
	}
	return nil
}

// GetFlowNodeById returns the FlowNode with the given id by walking
// the process tree linearly. Prefer bpmn20.FindFlowNodeById for hot
// paths — it is O(1) and never returns range-loop copies.
func (p *TProcess) GetFlowNodeById(id string) FlowNode {
	return p.linearFindFlowNode(id)
}

// GetSubprocessAndStartEventById returns the (subprocess, startEvent)
// pair for the given start event id, recursively descending into
// nested subprocesses. Uses for i := range so the returned pointers
// are stable.
//
// Production callers should prefer bpmn20.FindSubprocessAndStartEventById,
// which is O(1). This method is the slow legacy path.
func (p *TProcess) GetSubprocessAndStartEventById(id string) (*TSubProcess, *TStartEvent) {
	for i := range p.SubProcess {
		sp := &p.SubProcess[i]
		for j := range sp.StartEvents {
			se := &sp.StartEvents[j]
			if se.GetId() == id {
				return sp, se
			}
		}
		if nested, start := sp.GetSubprocessAndStartEventById(id); nested != nil {
			return nested, start
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
