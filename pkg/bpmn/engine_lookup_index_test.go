package bpmn

import (
	"context"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

// TestCreateInstanceProgrammaticDefinitionUsesLookupFallback preserves
// the public behavior that existed before indexed token lookup. A caller
// may provide an already-wired process model without invoking
// ResolveReferences; token resolution must use the read-only fallback and
// must not require lazy mutation of the shared definition.
func TestCreateInstanceProgrammaticDefinitionUsesLookupFallback(t *testing.T) {
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

	start := &definitions.Process.StartEvents[0]
	end := &definitions.Process.EndEvents[0]
	flow := &definitions.Process.SequenceFlows[0]
	start.OutgoingAssociations = []bpmn20.SequenceFlow{flow}
	end.IncomingAssociations = []bpmn20.SequenceFlow{flow}
	flow.SourceRef = start
	flow.TargetRef = end

	process := &runtime.ProcessDefinition{
		BpmnProcessId: "programmatic-process",
		Key:           4242,
		Definitions:   definitions,
	}

	instance, err := bpmnEngine.CreateInstance(context.Background(), process, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}
