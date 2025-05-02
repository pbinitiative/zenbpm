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
	result := !flow.IsDefault()
	// then
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
	result := !(flow.IsDefault())
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
	assert.Equal(t, "id", definitions.Process.ServiceTasks[0].GetId())
	assert.Equal(t, &definitions.Process.SequenceFlows[0], definitions.Process.StartEvents[0].GetOutgoingAssociation()[0])
	assert.Equal(t, &definitions.Process.SequenceFlows[0], definitions.Process.ServiceTasks[0].GetIncomingAssociation()[0])
	assert.Equal(t, &definitions.Process.SequenceFlows[1], definitions.Process.ServiceTasks[0].GetOutgoingAssociation()[0])
	assert.Equal(t, &definitions.Process.SequenceFlows[1], definitions.Process.EndEvents[0].GetIncomingAssociation()[0])
	// Check that references in SequenceFlow are correctly resolved
	assert.Equal(t, &definitions.Process.StartEvents[0], definitions.Process.SequenceFlows[0].GetSourceRef())
	assert.Equal(t, &definitions.Process.ServiceTasks[0], definitions.Process.SequenceFlows[0].GetTargetRef())
	assert.Equal(t, &definitions.Process.ServiceTasks[0], definitions.Process.SequenceFlows[1].GetSourceRef())
	assert.Equal(t, &definitions.Process.EndEvents[0], definitions.Process.SequenceFlows[1].GetTargetRef())
	//assert.Equal(t, definitions.Process.EndEvents[0], definitions.Process.ServiceTasks[0].GetOutgoingAssociation()[0])
}
