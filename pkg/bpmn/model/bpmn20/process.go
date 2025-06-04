package bpmn20

import (
	"fmt"
)

type TFlowElementsContainer struct {
	StartEvents            []TStartEvent             `xml:"startEvent"`
	EndEvents              []TEndEvent               `xml:"endEvent"`
	SequenceFlows          []TSequenceFlow           `xml:"sequenceFlow"`
	ServiceTasks           []TServiceTask            `xml:"serviceTask"`
	UserTasks              []TUserTask               `xml:"userTask"`
	BusinessRuleTask       []TBusinessRuleTask       `xml:"businessRuleTask"`
	SendTask               []TSendTask               `xml:"sendTask"`
	ParallelGateway        []TParallelGateway        `xml:"parallelGateway"`
	ExclusiveGateway       []TExclusiveGateway       `xml:"exclusiveGateway"`
	EventBasedGateway      []TEventBasedGateway      `xml:"eventBasedGateway"`
	InclusiveGateway       []TInclusiveGateway       `xml:"inclusiveGateway"`
	IntermediateCatchEvent []TIntermediateCatchEvent `xml:"intermediateCatchEvent"`
	IntermediateThrowEvent []TIntermediateThrowEvent `xml:"intermediateThrowEvent"`
	CallActivity           []TCallActivity           `xml:"callActivity"`
}

type TProcess struct {
	TCallableElement
	TFlowElementsContainer
	ProcessType                  string `xml:"processType,attr"`
	IsClosed                     bool   `xml:"isClosed,attr"`
	IsExecutable                 bool   `xml:"isExecutable,attr"`
	DefinitionalCollaborationRef string `xml:"definitionalCollaborationRef,attr"`
}

func (p *TProcess) GetInternalTaskById(id string) InternalTask {
	for _, e := range p.ServiceTasks {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.UserTasks {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.BusinessRuleTask {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.SendTask {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.IntermediateCatchEvent {
		switch def := e.EventDefinition.(type) {
		case TMessageEventDefinition:
			if def.Id == id {
				return &def
			}
		default:
			panic(fmt.Sprintf("unexpected type in EventDefinition %T", def))
		}
	}
	return nil
}

func (p *TProcess) GetFlowNodeById(id string) FlowNode {
	for _, e := range p.StartEvents {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.EndEvents {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.ServiceTasks {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.UserTasks {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.BusinessRuleTask {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.SendTask {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.ParallelGateway {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.ExclusiveGateway {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.EventBasedGateway {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.InclusiveGateway {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.IntermediateCatchEvent {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.IntermediateThrowEvent {
		if e.GetId() == id {
			return &e
		}
	}
	for _, e := range p.CallActivity {
		if e.GetId() == id {
			return &e
		}
	}

	return nil
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
