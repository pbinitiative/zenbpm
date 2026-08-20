package bpmn20

import (
	"encoding/xml"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubProcessWithMultipleStartEventsIsRejected(t *testing.T) {
	startEventFor := func(id string) string {
		switch id {
		case "message":
			return `<bpmn:startEvent id="start-1"><bpmn:messageEventDefinition id="med-1" messageRef="Msg_1" /></bpmn:startEvent>
				<bpmn:startEvent id="start-2"><bpmn:messageEventDefinition id="med-2" messageRef="Msg_2" /></bpmn:startEvent>`
		case "timer":
			return `<bpmn:startEvent id="start-1"><bpmn:timerEventDefinition id="ted-1"><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition></bpmn:startEvent>
				<bpmn:startEvent id="start-2"><bpmn:timerEventDefinition id="ted-2"><bpmn:timeDuration>PT2S</bpmn:timeDuration></bpmn:timerEventDefinition></bpmn:startEvent>`
		default: // error
			return `<bpmn:startEvent id="start-1"><bpmn:errorEventDefinition id="eed-1" errorRef="Error_1" /></bpmn:startEvent>
				<bpmn:startEvent id="start-2"><bpmn:errorEventDefinition id="eed-2" errorRef="Error_1" /></bpmn:startEvent>`
		}
	}

	for _, kind := range []string{"message", "timer", "error"} {
		t.Run(kind, func(t *testing.T) {
			xmlData := fmt.Sprintf(`
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="Definitions_multiple_start">
  <bpmn:process id="multiple-start-%s" isExecutable="true">
    <bpmn:subProcess id="event-subprocess" triggeredByEvent="true">
      %s
    </bpmn:subProcess>
  </bpmn:process>
  <bpmn:error id="Error_1" name="Error_1" errorCode="1" />
</bpmn:definitions>`, kind, startEventFor(kind))

			var definitions TDefinitions
			err := xml.Unmarshal([]byte(xmlData), &definitions)
			require.Error(t, err, "a sub process with multiple start events must be rejected at parse/deployment time")
			assert.Contains(t, err.Error(), "start events")
		})
	}
}

func TestNoExpressionWhenOnlyBlanks(t *testing.T) {
	flow := TSequenceFlow{
		ConditionExpression: TExpression{Text: "   "},
	}

	result := flow.GetConditionExpression() != ""
	assert.False(t, result)
}

func TestHasExpressionWhenSomeCharactersPresent(t *testing.T) {
	flow := TSequenceFlow{
		ConditionExpression: TExpression{
			Text: " x>y ",
		},
	}

	result := flow.GetConditionExpression() != ""

	assert.True(t, result)
}

func TestUnmarshallingWithReferenceResolution(t *testing.T) {
	var definitions TDefinitions
	var xmlData, err = os.ReadFile("./test-cases/simple_task.bpmn")

	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	err1 := xml.Unmarshal(xmlData, &definitions)
	if err1 != nil {
		t.Fatalf("failed to unmarshal XML: %v", err)
	}
	// Check that references in FlowNodes are correctly resolved
	assert.Equal(t, 1, len(definitions.Process.ServiceTasks))
	var serviceTask = definitions.Process.ServiceTasks[0]
	assert.Equal(t, 1, len(definitions.Process.StartEvents))
	var startEvent = definitions.Process.StartEvents[0]
	assert.Equal(t, 1, len(definitions.Process.EndEvents))
	var endEvent = definitions.Process.EndEvents[0]
	assert.Equal(t, 2, len(definitions.Process.SequenceFlows))
	var startToTask, taskToEnd = definitions.Process.SequenceFlows[0], definitions.Process.SequenceFlows[1]

	assert.Equal(t, 1, len(startEvent.GetOutgoingAssociation()))
	assert.Equal(t, 0, len(startEvent.GetIncomingAssociation()))
	assert.Equal(t, &startToTask, startEvent.GetOutgoingAssociation()[0])

	assert.Equal(t, 1, len(serviceTask.GetOutgoingAssociation()))
	assert.Equal(t, 1, len(serviceTask.GetIncomingAssociation()))
	assert.Equal(t, &startToTask, serviceTask.GetIncomingAssociation()[0])
	assert.Equal(t, &taskToEnd, serviceTask.GetOutgoingAssociation()[0])

	assert.Equal(t, 0, len(endEvent.GetOutgoingAssociation()))
	assert.Equal(t, 1, len(endEvent.GetIncomingAssociation()))
	assert.Equal(t, &taskToEnd, endEvent.GetIncomingAssociation()[0])
	assert.Equal(t, &startEvent, startToTask.GetSourceRef())
	assert.Equal(t, &serviceTask, startToTask.GetTargetRef())
	assert.Equal(t, &serviceTask, taskToEnd.GetSourceRef())
	assert.Equal(t, &endEvent, taskToEnd.GetTargetRef())
}

func TestResolveReferencesSuccess(t *testing.T) {
	ts := TDefinitions{
		TBaseElement: TBaseElement{
			Id: "definitions_1",
		},
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{
					TBaseElement: TBaseElement{
						Id: "process_1",
					},
					Name: "Example Process",
				},
				TFlowElementsContainer: TFlowElementsContainer{
					SequenceFlows: []TSequenceFlow{
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "one_to_two",
								},
							},
							SourceRefId: "one",
							TargetRefId: "two",
						},
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "two_to_three",
								},
							},
							SourceRefId: "two",
							TargetRefId: "three",
						},
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "two_to_four",
								},
							},
							SourceRefId: "two",
							TargetRefId: "four",
						},
					},
					StartEvents: []TStartEvent{
						{
							TEvent: TEvent{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: "one",
										},
										Name: "Start Event",
									},
									IncomingAssociationsIDs: []string{},
									OutgoingAssociationsIDs: []string{"one_to_two"}},
							},
						},
					},
					ServiceTasks: []TServiceTask{
						{
							TExternallyProcessedTask: TExternallyProcessedTask{
								TTask: TTask{
									TActivity: TActivity{
										TFlowNode: TFlowNode{
											TFlowElement: TFlowElement{
												TBaseElement: TBaseElement{
													Id: "two",
												},
												Name: "Task 1",
											},
											IncomingAssociationsIDs: []string{"one_to_two"},
											OutgoingAssociationsIDs: []string{"two_to_three", "two_to_four"},
										},
									},
								},
							},
						},
						{
							TExternallyProcessedTask: TExternallyProcessedTask{
								TTask: TTask{
									TActivity: TActivity{
										TFlowNode: TFlowNode{
											TFlowElement: TFlowElement{
												TBaseElement: TBaseElement{
													Id: "three",
												},
												Name: "Task 2",
											},
											IncomingAssociationsIDs: []string{"two_to_three"},
										},
									},
								},
							},
						},
					},

					EndEvents: []TEndEvent{
						{
							TEvent: TEvent{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: "four",
										},
										Name: "End Event",
									},
									IncomingAssociationsIDs: []string{"two_to_four"},
								},
							},
						},
					},
				},
			},
		},
	}

	err := ts.ResolveReferences()
	assert.NoError(t, err)
	assert.Equal(t, ts.baseElements["one_to_two"], ts.baseElements["one"].(FlowNode).GetOutgoingAssociation()[0])
	assert.Equal(t, ts.baseElements["one_to_two"], ts.baseElements["two"].(FlowNode).GetIncomingAssociation()[0])
	assert.Equal(t, ts.baseElements["two_to_three"], ts.baseElements["two"].(FlowNode).GetOutgoingAssociation()[0])
	assert.Equal(t, ts.baseElements["two_to_three"], ts.baseElements["three"].(FlowNode).GetIncomingAssociation()[0])
	assert.Equal(t, ts.baseElements["two_to_four"], ts.baseElements["two"].(FlowNode).GetOutgoingAssociation()[1])
	assert.Equal(t, ts.baseElements["two_to_four"], ts.baseElements["four"].(FlowNode).GetIncomingAssociation()[0])
	assert.Equal(t, ts.baseElements["one"], ts.baseElements["one_to_two"].(SequenceFlow).GetSourceRef())
	assert.Equal(t, ts.baseElements["two"], ts.baseElements["one_to_two"].(SequenceFlow).GetTargetRef())
	assert.Equal(t, ts.baseElements["two"], ts.baseElements["two_to_three"].(SequenceFlow).GetSourceRef())
	assert.Equal(t, ts.baseElements["three"], ts.baseElements["two_to_three"].(SequenceFlow).GetTargetRef())
	assert.Equal(t, ts.baseElements["two"], ts.baseElements["two_to_four"].(SequenceFlow).GetSourceRef())
	assert.Equal(t, ts.baseElements["four"], ts.baseElements["two_to_four"].(SequenceFlow).GetTargetRef())

	// ...
}

func TestResolveReferencesFailNotFoundBaseElement(t *testing.T) {
	ts := TDefinitions{
		TBaseElement: TBaseElement{
			Id: "definitions_1",
		},
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{
					TBaseElement: TBaseElement{
						Id: "process_1",
					},
					Name: "Example Process",
				},
				TFlowElementsContainer: TFlowElementsContainer{
					SequenceFlows: []TSequenceFlow{
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "one_to_two",
								},
							},
							SourceRefId: "one",
							TargetRefId: "not_existing_id",
						},
					},
					StartEvents: []TStartEvent{
						{
							TEvent: TEvent{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: "one",
										},
										Name: "Start Event",
									},
									IncomingAssociationsIDs: []string{},
									OutgoingAssociationsIDs: []string{"one_to_two"}},
							},
						},
					},
				},
			},
		},
	}
	err := ts.ResolveReferences()
	assert.ErrorContains(t, err, "not_existing_id")
}
func TestResolveReferencesSuccessEmptyIds(t *testing.T) {
	ts := TDefinitions{
		TBaseElement: TBaseElement{
			Id: "definitions_1",
		},
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{
					TBaseElement: TBaseElement{
						Id: "process_1",
					},
					Name: "Example Process",
				},
				TFlowElementsContainer: TFlowElementsContainer{
					SequenceFlows: []TSequenceFlow{
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "one_to_two",
								},
							},
							SourceRefId: "one",
							TargetRefId: "one",
						},
					},
					StartEvents: []TStartEvent{
						{
							TEvent: TEvent{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: "one",
										},
										Name: "Start Event",
									},
									IncomingAssociationsIDs: []string{""},
									OutgoingAssociationsIDs: []string{""}},
							},
						},
					},
				},
			},
		},
	}
	err := ts.ResolveReferences()
	assert.NoError(t, err)
}
func TestResolveReferencesFailWrongType(t *testing.T) {
	ts := TDefinitions{
		TBaseElement: TBaseElement{
			Id: "definitions_1",
		},
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{
					TBaseElement: TBaseElement{
						Id: "process_1",
					},
					Name: "Example Process",
				},
				TFlowElementsContainer: TFlowElementsContainer{
					SequenceFlows: []TSequenceFlow{
						{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: "one_to_two",
								},
							},
							SourceRefId: "one",
							TargetRefId: "one_to_two",
						},
					},
					StartEvents: []TStartEvent{
						{
							TEvent: TEvent{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: "one",
										},
										Name: "Start Event",
									},
									IncomingAssociationsIDs: []string{},
									OutgoingAssociationsIDs: []string{"one_to_two"}},
							},
						},
					},
				},
			},
		},
	}
	err := ts.ResolveReferences()
	assert.ErrorContains(t, err, "[one_to_two] is not assignable to FlowNode")
}

func TestFindBoundaryEventsForActivity(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	assert.NoError(t, err)
	var definitions TDefinitions
	err = xml.Unmarshal(xmlData, &definitions)
	assert.NoError(t, err)

	res := FindBoundaryEventsForActivity(&definitions.Process.TFlowElementsContainer, "Activity_1f5yxes")
	assert.Len(t, res, 1)
	event, ok := res[0].EventDefinition.(TMessageEventDefinition)
	assert.True(t, ok)
	m, err := definitions.GetMessageByRef(event.MessageRef)
	assert.NoError(t, err)
	assert.Equal(t, "OuterTestMessage", m.Name)
}

func TestFindBoundaryEventsForActivityRecursive(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	assert.NoError(t, err)
	var definitions TDefinitions
	err = xml.Unmarshal(xmlData, &definitions)
	assert.NoError(t, err)

	res := FindBoundaryEventsForActivity(&definitions.Process.TFlowElementsContainer, "Activity_1gbwlgl")
	assert.Len(t, res, 1)
	event, ok := res[0].EventDefinition.(TMessageEventDefinition)
	assert.True(t, ok)
	m, err := definitions.GetMessageByRef(event.MessageRef)
	assert.NoError(t, err)
	assert.Equal(t, "InnerInnerTestMessage", m.Name)
}

func TestGetFlowNodeByIdReturnsElementFromSlice(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &definitions))

	serviceTask, ok := definitions.Process.GetFlowNodeById("id").(*TServiceTask)
	require.True(t, ok)
	require.NotNil(t, serviceTask)

	serviceTask.Name = "Mutated"
	assert.Equal(t, "Mutated", definitions.Process.ServiceTasks[0].Name,
		"GetFlowNodeById must return a pointer into the underlying slice memory, not a copy")
}

func buildAllFlowNodeTypesProcess() *TDefinitions {
	p := &TProcess{
		TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: "P_root"}},
		TFlowElementsContainer: TFlowElementsContainer{
			StartEvents: []TStartEvent{
				{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_start"}}}}},
			},
			EndEvents: []TEndEvent{
				{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_end"}}}}},
			},
			ServiceTasks: []TServiceTask{
				{TExternallyProcessedTask: TExternallyProcessedTask{TTask: TTask{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_service"}}}}}}},
			},
			UserTasks: []TUserTask{
				{TTask: TTask{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_user"}}}}}},
			},
			BusinessRuleTask: []TBusinessRuleTask{
				{TTask: TTask{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_businessRule"}}}}}},
			},
			SendTask: []TSendTask{
				{TExternallyProcessedTask: TExternallyProcessedTask{TTask: TTask{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_send"}}}}}}},
			},
			ReceiveTask: []TReceiveTask{
				{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_receive"}}}}},
			},
			ParallelGateway: []TParallelGateway{
				{TGateway: TGateway{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_parallel"}}}}},
			},
			ExclusiveGateway: []TExclusiveGateway{
				{TGateway: TGateway{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_exclusive"}}}}},
			},
			EventBasedGateway: []TEventBasedGateway{
				{TGateway: TGateway{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_eventBased"}}}}},
			},
			InclusiveGateway: []TInclusiveGateway{
				{TGateway: TGateway{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_inclusive"}}}}},
			},
			IntermediateCatchEvent: []TIntermediateCatchEvent{
				{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_intermediateCatch"}}}}},
			},
			IntermediateThrowEvent: []TIntermediateThrowEvent{
				{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_intermediateThrow"}}}}},
			},
			CallActivity: []TCallActivity{
				{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_callActivity"}}}}},
			},
			BoundaryEvent: []TBoundaryEvent{
				{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_boundary"}}}}},
			},
			SubProcess: []TSubProcess{
				// TSubProcess embeds both TActivity and TProcess; GetId() on
				// TSubProcess resolves to the embedded TProcess.Id (the
				// last/most-nested embedder wins). The XML decoder writes
				// the BPMN id attribute to both embedded bases.
				{TActivity: TActivity{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_subProcess"}}}}, TProcess: TProcess{TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: "P_subProcess"}}}},
			},
			SequenceFlows: []TSequenceFlow{
				{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "P_sequence"}}, SourceRefId: "P_start", TargetRefId: "P_end"},
			},
		},
	}
	definitions := &TDefinitions{
		TBaseElement:           TBaseElement{Id: "D_all"},
		TRootElementsContainer: TRootElementsContainer{Process: *p},
	}
	if err := definitions.ResolveReferences(); err != nil {
		panic(fmt.Sprintf("buildAllFlowNodeTypesProcess: ResolveReferences failed: %v", err))
	}
	return definitions
}

func TestGetFlowNodeByIdCoversEveryFlowNodeType(t *testing.T) {
	definitions := buildAllFlowNodeTypesProcess()

	// TSubProcess embeds both TActivity and TProcess, each with their own
	// TBaseElement.Id. GetId() resolves to the shallower TProcess path, which
	// is the id populateContainerIndex uses to register the sub-process in
	// its parent container's index.
	subProcess := definitions.Process.SubProcess[0]
	require.Equal(t, "P_subProcess", subProcess.GetId())

	cases := []struct {
		id   string
		want reflect.Type
	}{
		{"P_start", reflect.TypeOf((*TStartEvent)(nil))},
		{"P_end", reflect.TypeOf((*TEndEvent)(nil))},
		{"P_service", reflect.TypeOf((*TServiceTask)(nil))},
		{"P_user", reflect.TypeOf((*TUserTask)(nil))},
		{"P_businessRule", reflect.TypeOf((*TBusinessRuleTask)(nil))},
		{"P_send", reflect.TypeOf((*TSendTask)(nil))},
		{"P_receive", reflect.TypeOf((*TReceiveTask)(nil))},
		{"P_parallel", reflect.TypeOf((*TParallelGateway)(nil))},
		{"P_exclusive", reflect.TypeOf((*TExclusiveGateway)(nil))},
		{"P_eventBased", reflect.TypeOf((*TEventBasedGateway)(nil))},
		{"P_inclusive", reflect.TypeOf((*TInclusiveGateway)(nil))},
		{"P_intermediateCatch", reflect.TypeOf((*TIntermediateCatchEvent)(nil))},
		{"P_intermediateThrow", reflect.TypeOf((*TIntermediateThrowEvent)(nil))},
		{"P_callActivity", reflect.TypeOf((*TCallActivity)(nil))},
		{"P_subProcess", reflect.TypeOf((*TSubProcess)(nil))},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			node := definitions.Process.GetFlowNodeById(tc.id)
			require.NotNil(t, node, "expected a flow node for id %q", tc.id)
			assert.Equal(t, tc.want, reflect.TypeOf(node),
				"id %q resolved to wrong concrete type", tc.id)
		})
	}

	assert.Nil(t, definitions.Process.GetFlowNodeById("P_boundary"))
	assert.Nil(t, definitions.Process.GetFlowNodeById("P_sequence"), "sequence flows must not be indexed as flow nodes")
	assert.Nil(t, definitions.Process.GetFlowNodeById(""))
	assert.Nil(t, definitions.Process.GetFlowNodeById("does-not-exist"))
}

func TestGetInternalTaskByIdCoversEveryInternalTaskType(t *testing.T) {
	definitions := buildAllFlowNodeTypesProcess()

	cases := []struct {
		id   string
		want reflect.Type
	}{
		{"P_service", reflect.TypeOf((*TServiceTask)(nil))},
		{"P_user", reflect.TypeOf((*TUserTask)(nil))},
		{"P_businessRule", reflect.TypeOf((*TBusinessRuleTask)(nil))},
		{"P_send", reflect.TypeOf((*TSendTask)(nil))},
		{"P_end", reflect.TypeOf((*TEndEvent)(nil))},
		{"P_intermediateThrow", reflect.TypeOf((*TIntermediateThrowEvent)(nil))},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			task := definitions.Process.GetInternalTaskById(tc.id)
			require.NotNil(t, task, "expected an internal task for id %q", tc.id)
			assert.Equal(t, tc.want, reflect.TypeOf(task),
				"id %q resolved to wrong concrete type", tc.id)
		})
	}

	for _, id := range []string{
		"P_start", "P_parallel", "P_exclusive", "P_eventBased", "P_inclusive",
		"P_intermediateCatch", "P_callActivity", "P_subProcess", "P_boundary",
	} {
		assert.Nil(t, definitions.Process.GetInternalTaskById(id),
			"id %q must not resolve to an internal task", id)
	}

	assert.Nil(t, definitions.Process.GetInternalTaskById(""))
}

func TestGetFlowNodeByIdDescendsIntoNestedSubprocesses(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	require.NoError(t, err)
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &definitions))

	parent, ok := definitions.Process.GetFlowNodeById("Activity_0z4w1h2").(*TSubProcess)
	require.True(t, ok)
	require.NotNil(t, parent)

	grandChild, ok := parent.GetFlowNodeById("Activity_1gbwlgl").(*TServiceTask)
	require.True(t, ok)
	require.NotNil(t, grandChild)
	assert.Equal(t, "Test", grandChild.Name)
}

func TestGetFlowNodeByIdScopesToSubprocess(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	require.NoError(t, err)
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &definitions))

	// subA contains subB; "id" belongs to subA, Activity_1gbwlgl belongs to subB.
	subA, ok := definitions.Process.GetFlowNodeById("Activity_1f5yxes").(*TSubProcess)
	require.True(t, ok)
	require.NotNil(t, subA)
	subB, ok := subA.GetFlowNodeById("Activity_0z4w1h2").(*TSubProcess)
	require.True(t, ok)
	require.NotNil(t, subB)

	require.NotNil(t, subA.GetFlowNodeById("id"))
	require.NotNil(t, subB.GetFlowNodeById("Activity_1gbwlgl"))
	require.NotNil(t, subA.GetFlowNodeById("Activity_1gbwlgl"))

	assert.Nil(t, subA.GetFlowNodeById(subA.GetId()), "a sub-process must not resolve itself in its child scope")
	assert.Nil(t, subB.GetFlowNodeById(subB.GetId()), "a sub-process must not resolve itself in its child scope")
	assert.Nil(t, subA.GetFlowNodeById("StartEvent_1"))
	assert.Nil(t, subA.GetFlowNodeById("Event_1j4mcqg"))
	assert.Nil(t, subB.GetFlowNodeById("StartEvent_1"))
	assert.Nil(t, subB.GetFlowNodeById("Event_1j4mcqg"))
	assert.Nil(t, subB.GetFlowNodeById("id"))
}

func TestGetInternalTaskByIdScopesToSubprocess(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	require.NoError(t, err)
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &definitions))

	subA, ok := definitions.Process.GetFlowNodeById("Activity_1f5yxes").(*TSubProcess)
	require.True(t, ok)
	subB, ok := subA.GetFlowNodeById("Activity_0z4w1h2").(*TSubProcess)
	require.True(t, ok)

	require.NotNil(t, subA.GetInternalTaskById("id"))
	require.NotNil(t, subB.GetInternalTaskById("Activity_1gbwlgl"))
	require.NotNil(t, subA.GetInternalTaskById("Activity_1gbwlgl"))

	assert.Nil(t, subA.GetInternalTaskById("StartEvent_1"))
	assert.Nil(t, subB.GetInternalTaskById("id"))
}

func buildLargeProcessDefinitionForLookupBenchmark(numTasks int) *TDefinitions {
	definitions := &TDefinitions{
		TBaseElement: TBaseElement{Id: "definitions-bench"},
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: "process-bench"}},
				TFlowElementsContainer: TFlowElementsContainer{
					StartEvents:   []TStartEvent{{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "start"}}, OutgoingAssociationsIDs: []string{"flow-start"}}}}},
					EndEvents:     []TEndEvent{{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "end"}}, IncomingAssociationsIDs: []string{fmt.Sprintf("flow-%d", numTasks-1)}}}}},
					SequenceFlows: make([]TSequenceFlow, 0, numTasks+1),
					ServiceTasks:  make([]TServiceTask, 0, numTasks),
				},
			},
		},
	}
	definitions.Process.SequenceFlows = append(definitions.Process.SequenceFlows,
		TSequenceFlow{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "flow-start"}}, SourceRefId: "start", TargetRefId: "task-0"})
	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		flowID := fmt.Sprintf("flow-%d", i)
		incomingFlowID := "flow-start"
		if i > 0 {
			incomingFlowID = fmt.Sprintf("flow-%d", i-1)
		}
		targetRef := fmt.Sprintf("task-%d", i+1)
		if i == numTasks-1 {
			targetRef = "end"
		}
		definitions.Process.ServiceTasks = append(definitions.Process.ServiceTasks,
			TServiceTask{
				TExternallyProcessedTask: TExternallyProcessedTask{
					TTask: TTask{
						TActivity: TActivity{
							TFlowNode: TFlowNode{
								TFlowElement:            TFlowElement{TBaseElement: TBaseElement{Id: taskID}, Name: taskID},
								IncomingAssociationsIDs: []string{incomingFlowID},
								OutgoingAssociationsIDs: []string{flowID},
							},
						},
					},
				},
			})
		definitions.Process.SequenceFlows = append(definitions.Process.SequenceFlows,
			TSequenceFlow{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: flowID}}, SourceRefId: taskID, TargetRefId: targetRef})
	}
	if err := definitions.ResolveReferences(); err != nil {
		panic(err)
	}
	return definitions
}

func buildSubProcessHeavyProcessDefinitionForLookupBenchmark(numSubProcesses int) *TDefinitions {
	process := TProcess{
		TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: "process-bench-sub"}},
		TFlowElementsContainer: TFlowElementsContainer{
			StartEvents: []TStartEvent{{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "start"}}}}}},
		},
	}
	process.SubProcess = make([]TSubProcess, numSubProcesses)
	for i := 0; i < numSubProcesses; i++ {
		sp := TSubProcess{
			TActivity: TActivity{
				TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{
					Id: fmt.Sprintf("sub-%d", i),
				}}},
			},
			TProcess: TProcess{
				TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: fmt.Sprintf("sub-%d", i)}},
				TFlowElementsContainer: TFlowElementsContainer{
					StartEvents: []TStartEvent{{TEvent: TEvent{TFlowNode: TFlowNode{TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: fmt.Sprintf("start-in-sub-%d", i)}}}}}},
					ServiceTasks: []TServiceTask{{
						TExternallyProcessedTask: TExternallyProcessedTask{
							TTask: TTask{
								TActivity: TActivity{
									TFlowNode: TFlowNode{
										TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: fmt.Sprintf("task-in-sub-%d", i)}},
									},
								},
							},
						},
					}},
				},
			},
		}
		process.SubProcess[i] = sp
	}
	definitions := &TDefinitions{
		TBaseElement: TBaseElement{Id: "definitions-bench-sub"},
		TRootElementsContainer: TRootElementsContainer{
			Process: process,
		},
	}
	if err := definitions.ResolveReferences(); err != nil {
		panic(err)
	}
	return definitions
}

func BenchmarkGetFlowNodeById_SubProcessSiblings(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("Indexed/SubProcesses=%d", n), func(b *testing.B) {
			definitions := buildSubProcessHeavyProcessDefinitionForLookupBenchmark(n)
			// Target the last subprocess to exercise the linear-scan worst case.
			id := fmt.Sprintf("task-in-sub-%d", n-1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if node := definitions.Process.GetFlowNodeById(id); node == nil {
					b.Fatalf("expected flow node %s", id)
				}
			}
		})

		b.Run(fmt.Sprintf("LinearScan/SubProcesses=%d", n), func(b *testing.B) {
			definitions := buildSubProcessHeavyProcessDefinitionForLookupBenchmark(n)
			id := fmt.Sprintf("task-in-sub-%d", n-1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if node := legacyLinearScanFlowNodeByID(&definitions.Process, id); node == nil {
					b.Fatalf("expected flow node %s", id)
				}
			}
		})
	}
}

func BenchmarkGetFlowNodeById(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("Indexed/N=%d", n), func(b *testing.B) {
			definitions := buildLargeProcessDefinitionForLookupBenchmark(n)
			id := fmt.Sprintf("task-%d", n-1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if node := definitions.Process.GetFlowNodeById(id); node == nil {
					b.Fatalf("expected flow node %s", id)
				}
			}
		})
		b.Run(fmt.Sprintf("LinearScan/N=%d", n), func(b *testing.B) {
			definitions := buildLargeProcessDefinitionForLookupBenchmark(n)
			id := fmt.Sprintf("task-%d", n-1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if node := legacyLinearScanFlowNodeByID(&definitions.Process, id); node == nil {
					b.Fatalf("expected flow node %s", id)
				}
			}
		})
	}
}

func legacyLinearScanFlowNodeByID(p *TProcess, id string) FlowNode {
	for i := range p.StartEvents {
		e := &p.StartEvents[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.EndEvents {
		e := &p.EndEvents[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.ServiceTasks {
		e := &p.ServiceTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.UserTasks {
		e := &p.UserTasks[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.BusinessRuleTask {
		e := &p.BusinessRuleTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.SendTask {
		e := &p.SendTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.ReceiveTask {
		e := &p.ReceiveTask[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.ParallelGateway {
		e := &p.ParallelGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.ExclusiveGateway {
		e := &p.ExclusiveGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.EventBasedGateway {
		e := &p.EventBasedGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.InclusiveGateway {
		e := &p.InclusiveGateway[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.IntermediateCatchEvent {
		e := &p.IntermediateCatchEvent[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.IntermediateThrowEvent {
		e := &p.IntermediateThrowEvent[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.CallActivity {
		e := &p.CallActivity[i]
		if e.GetId() == id {
			return e
		}
	}
	for i := range p.SubProcess {
		sp := &p.SubProcess[i]
		if sp.GetId() == id {
			return sp
		}
		if res := legacyLinearScanFlowNodeByID(&sp.TProcess, id); res != nil {
			return res
		}
	}
	return nil
}
