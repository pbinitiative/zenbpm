package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTDefinitions_HasTypedIndexes(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	assert.NotNil(t, defs.flowNodes)
	assert.NotNil(t, defs.internalTasks)
	assert.NotNil(t, defs.elementOwner)
	assert.NotNil(t, defs.subprocessParent)
}

func TestResolveReferences_PopulatesTypedIndexes(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	// Flow-node index covers every token-driven flow node.
	assert.Contains(t, defs.flowNodes, "StartEvent_1")
	assert.Contains(t, defs.flowNodes, "id")
	assert.Contains(t, defs.flowNodes, "Event_1j4mcqg")
	assert.NotContains(t, defs.flowNodes, "Flow_0xt1d7q",
		"sequence flows are not flow nodes")

	// Internal-task index covers every InternalTask implementor, including TEndEvent.
	assert.Contains(t, defs.internalTasks, "id", "service task is an internal task")
	assert.Contains(t, defs.internalTasks, "Event_1j4mcqg",
		"TEndEvent implements InternalTask and must be registered via interface assertion")
	assert.NotContains(t, defs.internalTasks, "StartEvent_1",
		"TStartEvent does not implement InternalTask")

	// Top-level elements have no owning subprocess.
	assert.Equal(t, "", defs.elementOwner["StartEvent_1"])
	assert.Equal(t, "", defs.elementOwner["id"])
	assert.Equal(t, "", defs.elementOwner["Event_1j4mcqg"])
}