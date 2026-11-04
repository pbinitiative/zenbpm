package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManualTaskProcessCanBeDeployed(t *testing.T) {
	definition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/manual_task/manual-task.bpmn")

	require.NoError(t, err)
	assert.Equal(t, "manual-task-process", definition.BpmnProcessId)
	manualTask := definition.Definitions.Process.GetFlowNodeById("manual-task")
	require.NotNil(t, manualTask)
	assert.Equal(t, bpmn20.ElementTypeManualTask, manualTask.GetType())
}
