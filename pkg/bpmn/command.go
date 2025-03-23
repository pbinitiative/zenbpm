package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type commandType string

const (
	flowTransitionType            commandType = "flowTransition"
	activityType                  commandType = "Activity"
	continueActivityType          commandType = "continueActivity"
	errorType                     commandType = "error"
	checkExclusiveGatewayDoneType commandType = "checkExclusiveGatewayDone"
)

type command interface {
	Type() commandType
}

// ---------------------------------------------------------------------

type flowTransitionCommand struct {
	sourceId       string
	sourceActivity runtime.Activity
	sequenceFlowId string
}

func (f flowTransitionCommand) Type() commandType {
	return flowTransitionType
}

// ---------------------------------------------------------------------

type activityCommand struct {
	sourceId       string
	element        bpmn20.FlowNode
	originActivity runtime.Activity
}

func (a activityCommand) Type() commandType {
	return activityType
}

// ---------------------------------------------------------------------

type continueActivityCommand struct {
	activity       runtime.Activity
	originActivity runtime.Activity
}

func (ga continueActivityCommand) Type() commandType {
	return continueActivityType
}

// ---------------------------------------------------------------------

type errorCommand struct {
	err         error
	elementId   string
	elementName string
}

func (e errorCommand) Type() commandType {
	return errorType
}

// ---------------------------------------------------------------------

type checkExclusiveGatewayDoneCommand struct {
	gatewayActivity eventBasedGatewayActivity
}

func (t checkExclusiveGatewayDoneCommand) Type() commandType {
	return checkExclusiveGatewayDoneType
}
