package bpmn

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type elementActivity struct {
	key     int64                 `json:"k"`
	state   runtime.ActivityState `json:"s"`
	element bpmn20.FlowNode
}

func (a elementActivity) GetKey() int64 {
	return a.key
}

func (a elementActivity) GetState() runtime.ActivityState {
	return a.state
}

func (a elementActivity) Element() bpmn20.FlowNode {
	return a.element
}

// ----------------------------------------------------------------------------

type FlowNodeExecutor struct {
	flowNode *elementActivity
}

func (e *FlowNodeExecutor) Execute(ctx context.Context, batch storage.Batch, state *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance) (createFlowTransitions bool, activityResult *elementActivity, nextCommands []command, err error) {
	log.Debug("Executing flow node %s", e.flowNode.Element().GetId())
	return false, nil, []command{}, nil
}

type ActivityExecutor struct {
	FlowNodeExecutor
}

// TODO: this will handle all business logic common to all activities like for example boundary events
func (e *ActivityExecutor) Execute(ctx context.Context, batch storage.Batch, state *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance) (createFlowTransitions bool, activityResult *elementActivity, nextCommands []command, err error) {
	log.Debug("Executing activity %s", e.flowNode.Element().GetId())
	return e.FlowNodeExecutor.Execute(ctx, batch, state, process, instance)
}

// -------------------------------------------------------------------------

type gatewayActivity struct {
	key                     int64                 `json:"k"`
	state                   runtime.ActivityState `json:"s"`
	element                 bpmn20.FlowNode
	parallel                bool
	inboundFlowIdsCompleted []string
}

func (ga *gatewayActivity) GetKey() int64 {
	return ga.key
}

func (ga *gatewayActivity) GetState() runtime.ActivityState {
	return ga.state
}

func (ga *gatewayActivity) Element() bpmn20.FlowNode {
	return ga.element
}

func (ga *gatewayActivity) AreInboundFlowsCompleted() bool {
	for _, association := range ga.element.GetIncomingAssociation() {
		if !slices.Contains(ga.inboundFlowIdsCompleted, association) {
			return false
		}
	}
	return true
}

func (ga *gatewayActivity) SetInboundFlowCompleted(flowId string) {
	ga.inboundFlowIdsCompleted = append(ga.inboundFlowIdsCompleted, flowId)
}

func (ga *gatewayActivity) SetState(state runtime.ActivityState) {
	ga.state = state
}

func (ga gatewayActivity) MarshalJSON() ([]byte, error) {
	type Alias gatewayActivity // Create an alias to avoid infinite recursion
	return json.Marshal(&struct {
		Key                     int64                 `json:"key"`
		State                   runtime.ActivityState `json:"state"`
		ElementID               string                `json:"elementId"`
		Parallel                bool                  `json:"parallel"`
		InboundFlowIdsCompleted []string              `json:"inboundFlowIdsCompleted"`
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
	state                     runtime.ActivityState
	element                   bpmn20.FlowNode
	OutboundActivityCompleted string
}

func (ebg eventBasedGatewayActivity) GetKey() int64 {
	return ebg.key
}

func (ebg eventBasedGatewayActivity) GetState() runtime.ActivityState {
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
