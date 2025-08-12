package bpmn20

import "testing"

// tests to get quick compiler warnings, when interface is not correctly implemented

func Test_all_interfaces_implemented(t *testing.T) {
	var _ InternalTask = &TServiceTask{}
	var _ InternalTask = &TUserTask{}
	var _ InternalTask = &TBusinessRuleTask{}

	var _ BaseElement = &TStartEvent{}
	var _ BaseElement = &TEndEvent{}
	var _ BaseElement = &TServiceTask{}
	var _ BaseElement = &TUserTask{}
	var _ BaseElement = &TParallelGateway{}
	var _ BaseElement = &TExclusiveGateway{}
	var _ BaseElement = &TIntermediateCatchEvent{}
	var _ BaseElement = &TIntermediateThrowEvent{}
	var _ BaseElement = &TEventBasedGateway{}
	var _ BaseElement = &TInclusiveGateway{}
	var _ BaseElement = &TCallActivity{}
	var _ BaseElement = &TBusinessRuleTask{}
}
