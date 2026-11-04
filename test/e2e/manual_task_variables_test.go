package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	manualTaskWithIoMappingPath         = "testdata/manual_task/manual_task_with_io_mapping.bpmn"
	manualTaskMultiInstanceParallelPath = "testdata/manual_task/manual_task_multi_instance_parallel.bpmn"
)

func TestManualTaskVariables(t *testing.T) {
	t.Run("Input mapping stays local and output mapping is propagated to the process", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskWithIoMappingPath, map[string]any{
			"customer": "Alice",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"customer":  "Alice",
			"signature": "signed by Alice",
		})
		assertFlowElementInputVariables(t, processInstance.Key, "manual-task", map[string]any{
			"customer": "Alice",
		})
		assertFlowElementOutputVariables(t, processInstance.Key, "manual-task", map[string]any{
			"signature": "signed by Alice",
		})
	})

	t.Run("Multi-instance manual task collects the output element of every iteration", func(t *testing.T) {
		contracts := []any{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskMultiInstanceParallelPath, map[string]any{
			"contracts": contracts,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		completedInstance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		assert.Equal(t, contracts, completedInstance.Variables["contracts"])
		// Parallel iterations do not guarantee the order of the output collection.
		assert.ElementsMatch(t, []any{"first-signed", "second-signed", "third-signed"}, completedInstance.Variables["signedContracts"])
		assert.Len(t, completedInstance.Variables, 2, "only the input and the output collection reach the process scope")
	})
}
