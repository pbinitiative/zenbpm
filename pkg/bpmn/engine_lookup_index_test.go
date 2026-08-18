package bpmn

import (
	"context"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateInstanceProgrammaticDefinitionRequiresResolveReferences pins
// down the *positive* contract: programmatic definitions must invoke
// ResolveReferences explicitly before being passed to the engine. This
// mirrors the contract that UnmarshalXML satisfies for XML-loaded
// definitions.
func TestCreateInstanceProgrammaticDefinitionRequiresResolveReferences(t *testing.T) {
	definitions := bpmn20.TDefinitions{
		TRootElementsContainer: bpmn20.TRootElementsContainer{
			Process: bpmn20.TProcess{
				TCallableElement: bpmn20.TCallableElement{
					TBaseElement: bpmn20.TBaseElement{Id: "programmatic-process"},
				},
				TFlowElementsContainer: bpmn20.TFlowElementsContainer{
					StartEvents: []bpmn20.TStartEvent{{
						TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{
							TFlowElement:            bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "start"}},
							OutgoingAssociationsIDs: []string{"flow"},
						}},
					}},
					EndEvents: []bpmn20.TEndEvent{{
						TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{
							TFlowElement:            bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "end"}},
							IncomingAssociationsIDs: []string{"flow"},
						}},
					}},
					SequenceFlows: []bpmn20.TSequenceFlow{{
						TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "flow"}},
						SourceRefId:  "start",
						TargetRefId:  "end",
					}},
				},
			},
		},
	}

	// Programmatic constructors must invoke ResolveReferences explicitly,
	// just like UnmarshalXML does for XML-loaded definitions.
	require.NoError(t, definitions.ResolveReferences())

	process := &runtime.ProcessDefinition{
		BpmnProcessId: "programmatic-process",
		Key:           4242,
		Definitions:   definitions,
	}

	instance, err := bpmnEngine.CreateInstance(context.Background(), process, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

// TestCreateInstanceProgrammaticDefinitionWithoutResolveReferences pins
// down the *negative* contract: when a programmatic definition is
// passed to the engine without calling ResolveReferences, token
// resolution fails with a clear error rather than silently looking like
// a missing BPMN element. This is the behaviour change called out in
// the plan — maintainers must accept it before merging.
//
// The error must mention the unresolved index (so a reader
// immediately understands the cause), and it must come from the
// engine hot path rather than from XML unmarshalling.
func TestCreateInstanceProgrammaticDefinitionWithoutResolveReferences(t *testing.T) {
	definitions := bpmn20.TDefinitions{
		TRootElementsContainer: bpmn20.TRootElementsContainer{
			Process: bpmn20.TProcess{
				TCallableElement: bpmn20.TCallableElement{
					TBaseElement: bpmn20.TBaseElement{Id: "unresolved-programmatic-process"},
				},
				TFlowElementsContainer: bpmn20.TFlowElementsContainer{
					StartEvents: []bpmn20.TStartEvent{{
						TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{
							TFlowElement:            bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "start"}},
							OutgoingAssociationsIDs: []string{"flow"},
						}},
					}},
					EndEvents: []bpmn20.TEndEvent{{
						TEvent: bpmn20.TEvent{TFlowNode: bpmn20.TFlowNode{
							TFlowElement:            bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "end"}},
							IncomingAssociationsIDs: []string{"flow"},
						}},
					}},
					SequenceFlows: []bpmn20.TSequenceFlow{{
						TFlowElement: bpmn20.TFlowElement{TBaseElement: bpmn20.TBaseElement{Id: "flow"}},
						SourceRefId:  "start",
						TargetRefId:  "end",
					}},
				},
			},
		},
	}

	// Deliberately skip definitions.ResolveReferences().

	process := &runtime.ProcessDefinition{
		BpmnProcessId: "unresolved-programmatic-process",
		Key:           4243,
		Definitions:   definitions,
	}
	instance, err := bpmnEngine.CreateInstance(context.Background(), process, nil)

	// The engine may successfully create the instance but fail at the
	// typed-index lookup when it tries to resolve the first token's
	// element id. The important assertion is that the typed-index
	// sentinel is propagated so callers can distinguish unresolved
	// definitions from any other engine failure.
	require.Error(t, err, "unresolved programmatic definition must not complete token execution")
	require.NotNil(t, instance, "engine returns the partially-created instance along with the error")

	// The sentinel distinguishes "definition not resolved" from any
	// other engine failure (e.g. persistence, marshalling). The
	// surrounding message includes "ResolveReferences" so a reader can
	// diagnose the cause without unwrapping.
	assert.ErrorIs(t, err, bpmn20.ErrLookupIndexNotInitialised,
		"engine must propagate the unresolved-index sentinel from the typed lookup helper")
}