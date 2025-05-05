package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_no_expression_when_only_blanks(t *testing.T) {
	// given
	flow := TSequenceFlow{
		ConditionExpression: TExpression{Text: "   "},
	}
	// when
	result := flow.GetConditionExpression() != ""
	assert.False(t, result)
}

func Test_has_expression_when_some_characters_present(t *testing.T) {
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

func Test_unmarshalling_with_reference_resolution(t *testing.T) {
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
