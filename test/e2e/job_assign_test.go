package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobAssignPersistsAfterCompletion(t *testing.T) {
	_, err := deployDefinition(t, "user-tasks-with-assignments.bpmn")
	require.NoError(t, err)

	definitions, err := listProcessDefinitions(t)
	require.NoError(t, err)

	var definition zenclient.ProcessDefinitionSimple
	for _, def := range definitions {
		if def.BpmnProcessId == "user-tasks-with-assignments" {
			definition = def
			break
		}
	}
	require.NotZero(t, definition.Key, "process definition not found")

	instance, err := createProcessInstance(t, &definition.Key, nil)
	require.NoError(t, err)

	jobs, err := getJobs(t, zenclient.GetJobsParams{
		ProcessInstanceKey: &instance.Key,
		State:              ptr.To(zenclient.JobStateActive),
	})
	require.NoError(t, err)
	require.NotEmpty(t, jobs.Partitions[0].Items, "expected at least one active job")

	var assigneeJob zenclient.Job
	for _, j := range jobs.Partitions[0].Items {
		if j.ElementId == "assignee-task" {
			assigneeJob = j
			break
		}
	}
	require.NotZero(t, assigneeJob.Key, "expected to find assignee-task job")
	assert.Equal(t, "john.doe", ptr.Deref(assigneeJob.Assignee, ""))

	completeResp, err := app.restClient.CompleteJobWithResponse(t.Context(), assigneeJob.Key, zenclient.CompleteJobJSONRequestBody{})
	require.NoError(t, err)
	require.Equal(t, 201, completeResp.StatusCode())

	jobResp, err := app.restClient.GetJobWithResponse(t.Context(), assigneeJob.Key)
	require.NoError(t, err)
	require.Equal(t, 200, jobResp.StatusCode())
	assert.Equal(t, zenclient.JobStateCompleted, jobResp.JSON200.State)
	assert.Equal(t, "john.doe", ptr.Deref(jobResp.JSON200.Assignee, ""), "assignee must persist after job completion")
}
