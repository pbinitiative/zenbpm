package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	manualTaskWithIoMappingPath           = "testdata/manual_task/manual_task_with_io_mapping.bpmn"
	manualTaskMultiInstanceParallelPath   = "testdata/manual_task/manual_task_multi_instance_parallel.bpmn"
	manualTaskMultiInstanceSequentialPath = "testdata/manual_task/manual_task_multi_instance_sequential.bpmn"
	manualTaskInvalidInputMappingPath     = "testdata/manual_task/manual_task_invalid_input_mapping.bpmn"
	manualTaskInvalidOutputMappingPath    = "testdata/manual_task/manual_task_invalid_output_mapping.bpmn"
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

	t.Run("Empty sequential collection completes without iterations and records an empty output collection", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskMultiInstanceSequentialPath, map[string]any{
			"contracts": []any{},
		})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, processInstance.Key) })

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertEmptyManualTaskCollection(t, processInstance.Key)
	})

	t.Run("Empty parallel collection completes without iterations and records an empty output collection", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskMultiInstanceParallelPath, map[string]any{
			"contracts": []any{},
		})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, processInstance.Key) })

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertEmptyManualTaskCollection(t, processInstance.Key)
	})

	t.Run("Invalid input mapping fails with an incident and keeps mapped variables out of the process", func(t *testing.T) {
		variables := map[string]any{"customer": "Alice", "signature": "original"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskInvalidInputMappingPath, variables)
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, processInstance.Key) })

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, variables)
		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, "manual-task", incidents[0].ElementId)
		assert.Contains(t, incidents[0].Message, "failed to evaluate input mappings of manual task")
		assert.Nil(t, incidents[0].ResolvedAt)
		assertFailedManualTaskHistory(t, processInstance.Key)
	})

	t.Run("Invalid output mapping fails atomically without propagating an earlier valid output", func(t *testing.T) {
		variables := map[string]any{"customer": "Alice", "signature": "original"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskInvalidOutputMappingPath, variables)
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, processInstance.Key) })

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, variables)
		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, "manual-task", incidents[0].ElementId)
		assert.Contains(t, incidents[0].Message, "failed to evaluate output mappings of manual task")
		assert.Nil(t, incidents[0].ResolvedAt)
		assertFailedManualTaskHistory(t, processInstance.Key)
	})
}

func assertEmptyManualTaskCollection(t *testing.T, processInstanceKey int64) {
	t.Helper()

	assertProcessInstanceVariables(t, processInstanceKey, map[string]any{
		"contracts":       []any{},
		"signedContracts": []any{},
	})
	assertExactCompletedProcessInstanceHistory(t, processInstanceKey, []string{
		"start", "flow-start-to-manual-task", "manual-task", "flow-manual-task-to-end", "end",
	})
	response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
	require.NotNil(t, response.JSON200.Items)
	for _, element := range *response.JSON200.Items {
		if element.ElementId == "manual-task" {
			require.NotNil(t, element.CompletedAt)
			require.NotNil(t, element.OutputVariables)
			assert.Equal(t, map[string]any{"signedContracts": []any{}}, *element.OutputVariables)
		}
	}
	children, err := getChildInstances(t, processInstanceKey)
	require.NoError(t, err)
	assert.Zero(t, children.TotalCount, "an empty collection creates no child instance or iteration")
	assertProcessInstanceIncidentsLength(t, processInstanceKey, 0)
}

// A mapping incident must not complete the manual task or take its outgoing flow.
// The incident batch may discard the task's queued history row altogether.
func assertFailedManualTaskHistory(t *testing.T, processInstanceKey int64) {
	t.Helper()

	response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
	require.NotNil(t, response.JSON200.Items)
	for _, element := range *response.JSON200.Items {
		assert.NotEqual(t, "flow-manual-task-to-end", element.ElementId)
		assert.NotEqual(t, "end", element.ElementId)
		if element.ElementId == "manual-task" {
			assert.Nil(t, element.CompletedAt, "a failed manual task cannot have completed history")
			if element.OutputVariables != nil {
				assert.Empty(t, *element.OutputVariables, "failed mappings cannot leak outputs into history")
			}
		}
	}
}
