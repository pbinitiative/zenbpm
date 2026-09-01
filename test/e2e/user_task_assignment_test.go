package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestUserTaskAssignment(t *testing.T) {

	t.Run("The user task assignee is mapped from the configured static value.", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_tasks_with_static_assignment.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task_static", public.JobStateActive)
		require.Equal(t, "approval", job.Type)
		require.Equal(t, "john.doe", ptr.Deref(job.Assignee, ""))
	})

	t.Run("The user task assignee is mapped from a process instance variable value.", func(t *testing.T) {

		assignee := "mario"

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_tasks_with_dynamic_assignment.bpmn", map[string]any{
			"dynamic_assignee": assignee,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task_dynamic", public.JobStateActive)
		require.Equal(t, assignee, ptr.Deref(job.Assignee, ""))
	})

	t.Run("A user task without assignment returns an empty assignee.", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateActive)
		require.NotNil(t, job.Assignee)
		require.Empty(t, *job.Assignee)
	})

	t.Run("A missing dynamic assignee is omitted from the jobs response.", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_tasks_with_dynamic_assignment.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task_dynamic", public.JobStateActive)
		require.Nil(t, job.Assignee)

		state := zenclient.JobStateActive
		response, err := app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{
			ProcessInstanceKey: &processInstance.Key,
			State:              &state,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())
		require.NotContains(t, string(response.Body), `"assignee"`)
		require.NotContains(t, string(response.Body), "<nil>")
	})
}
