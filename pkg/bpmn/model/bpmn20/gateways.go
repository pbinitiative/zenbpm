package bpmn20

type GatewayDirection string
type EventBasedGatewayType = string

type GatewayElement interface {
	FlowNode
	IsParallel() bool
	IsExclusive() bool
	IsInclusive() bool
	GetDefaultFlow() SequenceFlow
}
type TGateway struct {
	TFlowNode
	GatewayDirection GatewayDirection `xml:"gatewayDirection,attr"`
}

func (gateway *TGateway) IsParallel() bool             { return false }
func (gateway *TGateway) IsExclusive() bool            { return false }
func (gateway *TGateway) IsInclusive() bool            { return false }
func (gateway *TGateway) GetDefaultFlow() SequenceFlow { return nil }

const (
	ElementTypeParallelGateway   ElementType = "PARALLEL_GATEWAY"
	ElementTypeExclusiveGateway  ElementType = "EXCLUSIVE_GATEWAY"
	ElementTypeEventBasedGateway ElementType = "EVENT_BASED_GATEWAY"
	ElementTypeInclusiveGateway  ElementType = "INCLUSIVE_GATEWAY"

	GatewayDirectionUnspecified GatewayDirection = "Unspecified"
	GatewayDirectionConverging  GatewayDirection = "Converging"
	GatewayDirectionDiverging   GatewayDirection = "Diverging"
	GatewayDirectionMixed       GatewayDirection = "Mixed"

	EventBasedGatewayTypeExclusive EventBasedGatewayType = "Exclusive"
	EventBasedGatewayTypeParallel  EventBasedGatewayType = "Parallel"
)

type TInclusiveGateway struct {
	TGateway
	TDefaultFlowExtension
}

func (inclusiveGateway *TInclusiveGateway) GetType() ElementType { return ElementTypeInclusiveGateway }
func (inclusiveGateway *TInclusiveGateway) IsInclusive() bool    { return true }
func (inclusiveGateway *TInclusiveGateway) GetDefaultFlow() SequenceFlow {
	return inclusiveGateway.DefaultFlow
}

type TParallelGateway struct {
	TGateway
}

func (parallelGateway *TParallelGateway) GetType() ElementType { return ElementTypeParallelGateway }
func (parallelGateway *TParallelGateway) IsParallel() bool     { return true }

type TExclusiveGateway struct {
	TGateway
	TDefaultFlowExtension
}

func (exclusiveGateway *TExclusiveGateway) GetType() ElementType { return ElementTypeExclusiveGateway }
func (exclusiveGateway *TExclusiveGateway) IsExclusive() bool    { return true }
func (exclusiveGateway *TExclusiveGateway) GetDefaultFlow() SequenceFlow {
	return exclusiveGateway.DefaultFlow
}

type TEventBasedGateway struct {
	TGateway
	Instantiate      bool                  `xml:"instantiate,attr"`
	EventGatewayType EventBasedGatewayType `xml:"eventGatewayType,attr"`
}

func (eventBasedGateway *TEventBasedGateway) GetType() ElementType {
	return ElementTypeEventBasedGateway
}
func (eventBasedGateway *TEventBasedGateway) IsExclusive() bool { return true }
