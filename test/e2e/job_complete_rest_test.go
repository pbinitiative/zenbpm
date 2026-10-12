package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestRestJobCompleteErrorResponse(t *testing.T) {
	t.Run("a job completed before is completed again", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")

		for range 2 {
			response, err := app.restClient.CompleteJobWithResponse(t.Context(), job.Key, zenclient.CompleteJobJSONRequestBody{})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, response.StatusCode(), "unexpected complete job response: %s body: %s", response.Status(), string(response.Body))
		}
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("a job which no longer waits for a worker answers 409", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		// the model names no retries, so the first failure exhausts them
		failJob(t, job.Key, nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		response, err := app.restClient.CompleteJobWithResponse(t.Context(), job.Key, zenclient.CompleteJobJSONRequestBody{})

		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, response.StatusCode(), "unexpected complete job response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON409)
		require.Contains(t, response.JSON409.Message, "already failed")
	})
}
