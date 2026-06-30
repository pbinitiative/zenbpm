package bpmn20

import (
	"encoding/xml"
	"fmt"
	"os"
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
