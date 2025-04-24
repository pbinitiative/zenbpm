package bpmn20

type GatewayDirection string
type EventBasedGatewayType = string

type TGateway struct {
	TFlowNode
	GatewayDirection GatewayDirection `xml:"gatewayDirection,attr"`
}

const (
	ElementTypeParallelGateway   ElementType = "PARALLEL_GATEWAY"
	ElementTypeExclusiveGateway  ElementType = "EXCLUSIVE_GATEWAY"
	ElementTypeEventBasedGateway ElementType = "EVENT_BASED_GATEWAY"
	ElementTypeInclusiveGateway  ElementType = "INCLUSIVE_GATEWAY"

	Unspecified GatewayDirection = "Unspecified"
	Converging  GatewayDirection = "Converging"
	Diverging   GatewayDirection = "Diverging"
	Mixed       GatewayDirection = "Mixed"

	EventBasedGatewayTypeExclusive EventBasedGatewayType = "Exclusive"
	EventBasedGatewayTypeParallel  EventBasedGatewayType = "Parallel"
)

type TInclusiveGateway struct {
	TGateway
}

type TParallelGateway struct {
	TGateway
}

type TExclusiveGateway struct {
	TGateway
}

type TEventBasedGateway struct {
	TGateway
	Instantiate      bool                  `xml:"instantiate,attr"`
	EventGatewayType EventBasedGatewayType `xml:"eventGatewayType,attr"`
}

type GatewayElement interface {
	FlowNode
	IsParallel() bool
	IsExclusive() bool
	IsInclusive() bool
}

// Defaults
func (parallelGateway TGateway) IsParallel() bool  { return false }
func (parallelGateway TGateway) IsExclusive() bool { return false }
func (parallelGateway TGateway) IsInclusive() bool { return false }

func (parallelGateway TParallelGateway) GetType() ElementType { return ElementTypeParallelGateway }
func (parallelGateway TParallelGateway) IsParallel() bool     { return true }

func (exclusiveGateway TExclusiveGateway) GetType() ElementType { return ElementTypeExclusiveGateway }
func (exclusiveGateway TExclusiveGateway) IsExclusive() bool    { return true }

func (inclusiveGateway TInclusiveGateway) GetType() ElementType { return ElementTypeInclusiveGateway }
func (inclusiveGateway TInclusiveGateway) IsInclusive() bool    { return true }

func (eventBasedGateway TEventBasedGateway) GetType() ElementType {
	return ElementTypeEventBasedGateway
}
func (eventBasedGateway TEventBasedGateway) IsExclusive() bool { return true }
