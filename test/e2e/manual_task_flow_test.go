package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const manualTaskPath = "testdata/manual_task/manual_task.bpmn"

func TestManualTaskFlow(t *testing.T) {
	t.Run("Manual task completes without a job and the process continues to the next task", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service-task")

		// User tasks are jobs as well, so no job in any state means neither was created.
		assertProcessInstanceHasNoJobByElementID(t, processInstance.Key, "manual-task")
		assertProcessInstanceTokenCount(t, processInstance.Key, "manual-task", 0)
		assertExactProcessInstanceHistoryStates(t, processInstance.Key,
			completedFlowElementHistory("start"),
			completedFlowElementHistory("flow-start-to-manual-task"),
			completedFlowElementHistory("manual-task"),
			completedFlowElementHistory("flow-manual-task-to-service-task"),
			activeFlowElementHistory("service-task"),
		)
		assertHistoryElementType(t, processInstance.Key, "manual-task", "MANUAL_TASK")
	})

	t.Run("Process with a manual task completes after the following task completes", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, manualTaskPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobForElementId(t, processInstance.Key, "service-task", nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertExactCompletedProcessInstanceHistory(t, processInstance.Key, []string{
			"start",
			"flow-start-to-manual-task",
			"manual-task",
			"flow-manual-task-to-service-task",
			"service-task",
			"flow-service-task-to-end",
			"end",
		})
	})
}

// assertProcessInstanceHasNoJobByElementID checks that the process instance has no
// job for the element in any state, completed jobs included.
func assertProcessInstanceHasNoJobByElementID(t testing.TB, processInstanceKey int64, elementID string) {
	t.Helper()

	jobs, err := getProcessInstanceJobs(t, processInstanceKey)
	require.NoError(t, err)
	for _, job := range jobs {
		assert.NotEqual(t, elementID, job.ElementId, "process instance %d has job %d in state %s for element %s", processInstanceKey, job.Key, job.State, elementID)
	}
}

// assertHistoryElementType checks the element type the public history API
// reports for an element of the process instance.
func assertHistoryElementType(t testing.TB, processInstanceKey int64, elementID string, expectedType string) {
	t.Helper()

	response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
	require.NotNil(t, response.JSON200.Items)

	for _, item := range *response.JSON200.Items {
		if item.ElementId == elementID {
			assert.Equal(t, expectedType, item.ElementType)
			return
		}
	}
	require.Failf(t, "flow element not found in history", "element %q is not in the history of process instance %d", elementID, processInstanceKey)
}
