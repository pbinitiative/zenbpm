package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type command interface {
}

// ---------------------------------------------------------------------

type flowTransitionCommand struct {
	sourceId       string
	sourceActivity runtime.Activity
	sequenceFlowId string
}

// ---------------------------------------------------------------------

type activityCommand struct {
	sourceId       string
	element        bpmn20.FlowNode
	originActivity runtime.Activity
}

// ---------------------------------------------------------------------

type continueActivityCommand struct {
	activity       runtime.Activity
	originActivity runtime.Activity
}

// ---------------------------------------------------------------------

type errorCommand struct {
	err         error
	elementId   string
	elementName string
}

// ---------------------------------------------------------------------

type checkExclusiveGatewayDoneCommand struct {
	gatewayActivity eventBasedGatewayActivity
}
