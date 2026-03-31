package bpmn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScriptTaskDeploymentReturnsUnsupportedError(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/unsupported-script-task.bpmn")

	require.Error(t, err)
	assert.ErrorContains(t, err, "scriptTask")
	assert.ErrorContains(t, err, "Activity_09chd67")
}

func TestManualTaskDeploymentReturnsUnsupportedError(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/unsupported-manual-task.bpmn")

	require.Error(t, err)
	assert.ErrorContains(t, err, "manualTask")
	assert.ErrorContains(t, err, "Activity_09chd67")
}

func TestReceiveTaskDeploymentReturnsUnsupportedError(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/unsupported-receive-task.bpmn")

	require.Error(t, err)
	assert.ErrorContains(t, err, "receiveTask")
	assert.ErrorContains(t, err, "Activity_09chd67")
}

func TestPlainTaskDeploymentReturnsUnsupportedError(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/unsupported-plain-task.bpmn")

	require.Error(t, err)
	assert.ErrorContains(t, err, "task")
}
