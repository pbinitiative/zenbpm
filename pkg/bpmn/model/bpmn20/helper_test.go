package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoExpressionWhenOnlyBlanks(t *testing.T) {
	// given
	flow := TSequenceFlow{
		ConditionExpression: TExpression{Text: "   "},
	}
	// when
	result := flow.GetConditionExpression() != ""
	assert.False(t, result)
}

func TestHasExpressionWhenSomeCharactersPresent(t *testing.T) {
	// given
	flow := TSequenceFlow{
		ConditionExpression: TExpression{
			Text: " x>y ",
		},
	}
	// when
	result := flow.GetConditionExpression() != ""
	// then
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
	var start_to_task, task_to_end = definitions.Process.SequenceFlows[0], definitions.Process.SequenceFlows[1]

	assert.Equal(t, 1, len(startEvent.GetOutgoingAssociation()))
	assert.Equal(t, 0, len(startEvent.GetIncomingAssociation()))
	assert.Equal(t, &start_to_task, startEvent.GetOutgoingAssociation()[0])

	assert.Equal(t, 1, len(serviceTask.GetOutgoingAssociation()))
	assert.Equal(t, 1, len(serviceTask.GetIncomingAssociation()))
	assert.Equal(t, &start_to_task, serviceTask.GetIncomingAssociation()[0])
	assert.Equal(t, &task_to_end, serviceTask.GetOutgoingAssociation()[0])

	assert.Equal(t, 0, len(endEvent.GetOutgoingAssociation()))
	assert.Equal(t, 1, len(endEvent.GetIncomingAssociation()))
	assert.Equal(t, &task_to_end, endEvent.GetIncomingAssociation()[0])
	assert.Equal(t, &startEvent, start_to_task.GetSourceRef())
	assert.Equal(t, &serviceTask, start_to_task.GetTargetRef())
	assert.Equal(t, &serviceTask, task_to_end.GetSourceRef())
	assert.Equal(t, &endEvent, task_to_end.GetTargetRef())
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
