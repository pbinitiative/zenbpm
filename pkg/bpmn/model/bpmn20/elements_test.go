package bpmn20

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// tests to get quick compiler warnings, when interface is not correctly implemented

func TestActivityElementTypes(t *testing.T) {
	assert.Equal(t, ElementType("CALL_ACTIVITY"), (&TCallActivity{}).GetType())
	assert.Equal(t, ElementType("SUB_PROCESS"), (&TSubProcess{}).GetType())
}

func TestAllInterfacesImplemented(t *testing.T) {
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
