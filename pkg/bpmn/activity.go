package bpmn

import (
	"encoding/json"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type activity interface {
	Key() int64
	State() bpmn20.ActivityState
	Element() bpmn20.FlowNode
}

type elementActivity struct {
	key     int64                `json:"k"`
	state   bpmn20.ActivityState `json:"s"`
	element bpmn20.FlowNode
}

func (a elementActivity) Key() int64 {
	return a.key
}

func (a elementActivity) State() bpmn20.ActivityState {
	return a.state
}

func (a elementActivity) Element() bpmn20.FlowNode {
	return a.element
}

// -------------------------------------------------------------------------

type gatewayActivity struct {
	key                     int64                `json:"k"`
	state                   bpmn20.ActivityState `json:"s"`
	element                 bpmn20.FlowNode
	parallel                bool
	inboundFlowIdsCompleted []string
}

func (ga *gatewayActivity) Key() int64 {
	return ga.key
}

func (ga *gatewayActivity) State() bpmn20.ActivityState {
	return ga.state
}

func (ga *gatewayActivity) Element() bpmn20.FlowNode {
	return ga.element
}

func (ga *gatewayActivity) AreInboundFlowsCompleted() bool {
	for _, association := range ga.element.GetIncomingAssociation() {
		if !contains(ga.inboundFlowIdsCompleted, association) {
			return false
		}
	}
	return true
}

func (ga *gatewayActivity) SetInboundFlowCompleted(flowId string) {
	ga.inboundFlowIdsCompleted = append(ga.inboundFlowIdsCompleted, flowId)
}

func (ga *gatewayActivity) SetState(state bpmn20.ActivityState) {
	ga.state = state
}

func (ga gatewayActivity) MarshalJSON() ([]byte, error) {
	type Alias gatewayActivity // Create an alias to avoid infinite recursion
	return json.Marshal(&struct {
		Key                     int64                `json:"key"`
		State                   bpmn20.ActivityState `json:"state"`
		ElementID               string               `json:"elementId"`
		Parallel                bool                 `json:"parallel"`
		InboundFlowIdsCompleted []string             `json:"inboundFlowIdsCompleted"`
	}{
		Key:                     ga.key,
		State:                   ga.state,
		ElementID:               ga.element.GetId(), // Get the ID from the element
		Parallel:                ga.parallel,
		InboundFlowIdsCompleted: ga.inboundFlowIdsCompleted,
	})
}

// -------------------------------------------------------------------------

type eventBasedGatewayActivity struct {
	key                       int64
	state                     bpmn20.ActivityState
	element                   bpmn20.FlowNode
	OutboundActivityCompleted string
}

func (ebg eventBasedGatewayActivity) Key() int64 {
	return ebg.key
}

func (ebg eventBasedGatewayActivity) State() bpmn20.ActivityState {
	return ebg.state
}

func (ebg eventBasedGatewayActivity) Element() bpmn20.FlowNode {
	return ebg.element
}

func (ebg eventBasedGatewayActivity) SetOutboundCompleted(id string) {
	ebg.OutboundActivityCompleted = id
}

func (ebg eventBasedGatewayActivity) OutboundCompleted() bool {
	return len(ebg.OutboundActivityCompleted) > 0
}
