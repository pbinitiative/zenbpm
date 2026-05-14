package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalInstance builds a DefaultProcessInstance whose Definition contains a single
// subprocess with a single start event, identified by the given IDs.
func minimalInstance(subProcessId, startEventId string) runtime.ProcessInstance {
	return &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: &runtime.ProcessDefinition{
				Definitions: bpmn20.TDefinitions{
					TRootElementsContainer: bpmn20.TRootElementsContainer{
						Process: bpmn20.TProcess{
							TFlowElementsContainer: bpmn20.TFlowElementsContainer{
								SubProcess: []bpmn20.TSubProcess{
									{
										TProcess: bpmn20.TProcess{
											TCallableElement: bpmn20.TCallableElement{
												TBaseElement: bpmn20.TBaseElement{Id: subProcessId},
											},
											TFlowElementsContainer: bpmn20.TFlowElementsContainer{
												StartEvents: []bpmn20.TStartEvent{
													{
														TEvent: bpmn20.TEvent{
															TFlowNode: bpmn20.TFlowNode{
																TFlowElement: bpmn20.TFlowElement{
																	TBaseElement: bpmn20.TBaseElement{Id: startEventId},
																},
															},
														},
														IsInterrupting: true,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestResolveEventSubprocessDefs_Found(t *testing.T) {
	const subID, startID = "sub-1", "start-1"
	instance := minimalInstance(subID, startID)

	subDef, startDef, err := resolveEventSubprocessDefs(instance, startID)

	require.NoError(t, err)
	require.NotNil(t, subDef)
	require.NotNil(t, startDef)
	assert.Equal(t, subID, subDef.GetId())
	assert.Equal(t, startID, startDef.GetId())
}

func TestResolveEventSubprocessDefs_StartEventNotFound(t *testing.T) {
	instance := minimalInstance("sub-1", "start-1")

	_, _, err := resolveEventSubprocessDefs(instance, "nonexistent-start")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent-start")
}

func TestResolveEventSubprocessDefs_EmptyProcess(t *testing.T) {
	// Process with no subprocesses at all — both lookups must fail gracefully.
	instance := &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: &runtime.ProcessDefinition{
				Definitions: bpmn20.TDefinitions{
					TRootElementsContainer: bpmn20.TRootElementsContainer{
						Process: bpmn20.TProcess{},
					},
				},
			},
		},
	}

	_, _, err := resolveEventSubprocessDefs(instance, "any-start")

	require.Error(t, err)
}
