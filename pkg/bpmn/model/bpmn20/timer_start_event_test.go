package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimerStartEventParsesFromBpmn(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/timer-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)

	var definitions TDefinitions
	err = xml.Unmarshal(xmlData, &definitions)
	require.NoError(t, err)

	// The main process should have one plain start event (no event definitions)
	assert.Equal(t, 1, len(definitions.Process.StartEvents))
	mainStart := definitions.Process.StartEvents[0]
	assert.Equal(t, 0, len(mainStart.EventDefinitions), "main process start event should be plain (no event definitions)")

	// The process should have one event subprocess
	assert.Equal(t, 1, len(definitions.Process.SubProcess))
	eventSubProcess := definitions.Process.SubProcess[0]
	assert.True(t, eventSubProcess.TriggeredByEvent, "subprocess should be triggered by event")

	// The event subprocess should have one timer start event
	assert.Equal(t, 1, len(eventSubProcess.TProcess.StartEvents))
	subStart := eventSubProcess.TProcess.StartEvents[0]
	require.Equal(t, 1, len(subStart.EventDefinitions), "event subprocess start event should have one event definition")
	timerDef, isTimer := subStart.EventDefinitions[0].(TTimerEventDefinition)
	assert.True(t, isTimer, "event subprocess start event should be a timer event definition")
	assert.NotNil(t, timerDef.Id)
	assert.NotNil(t, timerDef.TimeDuration)
	assert.Equal(t, "PT1S", timerDef.TimeDuration.XMLText)
}

func TestStartEventIsInterruptingDefaultsToTrue(t *testing.T) {
	// When isInterrupting is not specified, it defaults to true per BPMN spec
	xmlStr := `<startEvent xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" id="start1"></startEvent>`

	var startEvent TStartEvent
	err := xml.Unmarshal([]byte(xmlStr), &startEvent)
	require.NoError(t, err)
	assert.True(t, startEvent.IsInterrupting)
}

func TestStartEventIsInterruptingExplicitFalse(t *testing.T) {
	xmlStr := `<startEvent xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" id="start1" isInterrupting="false"></startEvent>`

	var startEvent TStartEvent
	err := xml.Unmarshal([]byte(xmlStr), &startEvent)
	require.NoError(t, err)
	assert.False(t, startEvent.IsInterrupting)
}

func TestStartEventTimerWithTimeDate(t *testing.T) {
	xmlStr := `<startEvent xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" id="start1">
					<timerEventDefinition id="timer1">
						<timeDate>2026-01-01T00:00:00Z</timeDate>
					</timerEventDefinition>
			   </startEvent>`

	var startEvent TStartEvent
	err := xml.Unmarshal([]byte(xmlStr), &startEvent)
	require.NoError(t, err)

	require.Equal(t, 1, len(startEvent.EventDefinitions))
	timerDef, ok := startEvent.EventDefinitions[0].(TTimerEventDefinition)
	require.True(t, ok)
	assert.NotNil(t, timerDef.TimeDate)
	assert.Equal(t, "2026-01-01T00:00:00Z", timerDef.TimeDate.XMLText)
}

func TestFindEventSubProcesses(t *testing.T) {
	container := &TFlowElementsContainer{
		SubProcess: []TSubProcess{
			{TriggeredByEvent: false},
			{TriggeredByEvent: true},
			{TriggeredByEvent: true},
			{TriggeredByEvent: false},
		},
	}

	result := FindEventSubProcesses(container)
	assert.Equal(t, 2, len(result))
	for _, sp := range result {
		assert.True(t, sp.TriggeredByEvent)
	}
}

func TestFindEventSubProcesses_Empty(t *testing.T) {
	container := &TFlowElementsContainer{
		SubProcess: []TSubProcess{
			{TriggeredByEvent: false},
		},
	}
	result := FindEventSubProcesses(container)
	assert.Equal(t, 0, len(result))
}

func TestFindEventSubProcesses_Nil(t *testing.T) {
	container := &TFlowElementsContainer{}
	result := FindEventSubProcesses(container)
	assert.Equal(t, 0, len(result))
}

func TestGetSubprocessAndStartEventById(t *testing.T) {
	process := TProcess{
		TFlowElementsContainer: TFlowElementsContainer{
			SubProcess: []TSubProcess{
				{
					TProcess: TProcess{
						TCallableElement: TCallableElement{
							TBaseElement: TBaseElement{Id: "sub1"},
						},
						TFlowElementsContainer: TFlowElementsContainer{
							StartEvents: []TStartEvent{
								{
									TEvent: TEvent{
										TFlowNode: TFlowNode{
											TFlowElement: TFlowElement{
												TBaseElement: TBaseElement{Id: "start-in-sub1"},
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

	subProcess, startEvent := process.GetSubprocessAndStartEventById("start-in-sub1")
	assert.NotNil(t, subProcess)
	assert.NotNil(t, startEvent)
	assert.Equal(t, "sub1", subProcess.GetId())
	assert.Equal(t, "start-in-sub1", startEvent.GetId())
}

func TestGetSubprocessAndStartEventById_NotFound(t *testing.T) {
	process := TProcess{
		TFlowElementsContainer: TFlowElementsContainer{
			SubProcess: []TSubProcess{
				{
					TProcess: TProcess{
						TFlowElementsContainer: TFlowElementsContainer{
							StartEvents: []TStartEvent{
								{
									TEvent: TEvent{
										TFlowNode: TFlowNode{
											TFlowElement: TFlowElement{
												TBaseElement: TBaseElement{Id: "start-in-sub1"},
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

	subProcess, startEvent := process.GetSubprocessAndStartEventById("nonexistent")
	assert.Nil(t, subProcess)
	assert.Nil(t, startEvent)
}

func TestGetSubprocessAndStartEventById_Nested(t *testing.T) {
	process := TProcess{
		TFlowElementsContainer: TFlowElementsContainer{
			SubProcess: []TSubProcess{
				{
					TProcess: TProcess{
						TCallableElement: TCallableElement{
							TBaseElement: TBaseElement{Id: "outer-sub"},
						},
						TFlowElementsContainer: TFlowElementsContainer{
							StartEvents: []TStartEvent{
								{
									TEvent: TEvent{
										TFlowNode: TFlowNode{
											TFlowElement: TFlowElement{
												TBaseElement: TBaseElement{Id: "outer-start"},
											},
										},
									},
								},
							},
							SubProcess: []TSubProcess{
								{
									TProcess: TProcess{
										TCallableElement: TCallableElement{
											TBaseElement: TBaseElement{Id: "inner-sub"},
										},
										TFlowElementsContainer: TFlowElementsContainer{
											StartEvents: []TStartEvent{
												{
													TEvent: TEvent{
														TFlowNode: TFlowNode{
															TFlowElement: TFlowElement{
																TBaseElement: TBaseElement{Id: "inner-start"},
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
			},
		},
	}

	// Should find nested start event
	subProcess, startEvent := process.GetSubprocessAndStartEventById("inner-start")
	assert.NotNil(t, subProcess)
	assert.NotNil(t, startEvent)
	assert.Equal(t, "inner-sub", subProcess.GetId())
	assert.Equal(t, "inner-start", startEvent.GetId())
}

func TestTimerEventDefinitionId_NilSafety(t *testing.T) {
	// TTimerEventDefinition.Id is *string; GetId() must handle nil gracefully
	td := TTimerEventDefinition{
		Id:           nil,
		TimeDuration: &TTimeInfo{XMLText: "PT1S"},
	}
	// eventDefinition interface marker should not panic
	td.eventDefinition()

	// GetId with nil Id should return "" (not panic)
	assert.NotPanics(t, func() {
		id := td.GetId()
		assert.Equal(t, "", id)
	})
}

func TestFindBaseElementById(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/timer-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)

	var definitions TDefinitions
	err = xml.Unmarshal(xmlData, &definitions)
	require.NoError(t, err)

	// Should find the service task
	elem, ok := FindBaseElementById(&definitions, "service-task-1")
	assert.True(t, ok)
	assert.NotNil(t, elem)

	// Should not find non-existent element
	_, ok = FindBaseElementById(&definitions, "does-not-exist")
	assert.False(t, ok)
}
