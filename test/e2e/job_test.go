package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiJob(t *testing.T) {
	var instance public.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	err := deployDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "service-task-input-output" {
			definition = def
			break
		}
	}
	instance, err = createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	var jobToComplete zenclient.Job
	var jobsProcessInstance public.ProcessInstance
	t.Run("read waiting jobs", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "input-task-1")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, zenclient.JobStateActive, jobToComplete.State)

		jobsProcessInstance, err = getProcessInstance(t, jobToComplete.ProcessInstanceKey)
		assert.NoError(t, err)
	})

	t.Run("complete job", func(t *testing.T) {
		err := completeJob(t, jobToComplete, map[string]any{
			"city": "test",
		})
		assert.NoError(t, err)

		jobsProcessInstance, err = getProcessInstance(t, jobToComplete.ProcessInstanceKey)
		assert.NoError(t, err)
		assert.Contains(t, jobsProcessInstance.Variables, "dstcity", "Process instance should contain variable from completedJob")
		assert.Equal(t, "test", jobsProcessInstance.Variables["dstcity"])
	})

	instance2, err := createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 124,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance2.Key)

	t.Run("test filter by state", func(t *testing.T) {
		jobs, err := app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{JobType: ptr.To("input-task-1")})
		assert.NoError(t, err)

		items := jobs.JSON200.Partitions[0].Items
		assert.Len(t, items, 2)

		jobs, err = app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{
			JobType: ptr.To("input-task-1"),
			State:   ptr.To(zenclient.JobStateActive),
		})
		assert.NoError(t, err)

		items = jobs.JSON200.Partitions[0].Items
		assert.Len(t, items, 1)
		assert.Equal(t, zenclient.JobStateActive, items[0].State)
		assert.Equal(t, instance2.Key, items[0].ProcessInstanceKey)
	})

	t.Run("test sorting by key", func(t *testing.T) {
		jobs, err := app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{
			SortBy:    ptr.To(zenclient.GetJobsParamsSortByKey),
			SortOrder: ptr.To(zenclient.GetJobsParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		items := jobs.JSON200.Partitions[0].Items
		assert.True(t, items[0].Key < items[1].Key)

		jobs, err = app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{
			SortBy:    ptr.To(zenclient.GetJobsParamsSortByKey),
			SortOrder: ptr.To(zenclient.GetJobsParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		items = jobs.JSON200.Partitions[0].Items
		assert.True(t, items[0].Key > items[1].Key)
	})

	t.Run("test getting job by key - ok", func(t *testing.T) {
		jobs, err := app.restClient.GetJobsWithResponse(t.Context(), &zenclient.GetJobsParams{JobType: ptr.To("input-task-1")})
		assert.NoError(t, err)
		assert.Equal(t, 200, jobs.StatusCode())
		assert.NotEmpty(t, jobs.JSON200)
		assert.NotEmpty(t, jobs.JSON200.Partitions)
		assert.NotEmpty(t, jobs.JSON200.Partitions[0].Items)

		jobKey := jobs.JSON200.Partitions[0].Items[0].Key

		job, err := app.restClient.GetJobWithResponse(t.Context(), jobKey)
		assert.NoError(t, err)
		assert.Equal(t, 200, job.StatusCode())
		assert.Equal(t, jobKey, job.JSON200.Key)
	})

}

func readWaitingJobs(t testing.TB, jobType string) (zenclient.JobPartitionPage, error) {
	return getJobs(t, zenclient.GetJobsParams{JobType: &jobType, State: ptr.To(zenclient.JobStateActive)})
}

func getJobs(t testing.TB, params zenclient.GetJobsParams) (zenclient.JobPartitionPage, error) {
	jobs, err := app.restClient.GetJobsWithResponse(t.Context(), &params)

	if err != nil {
		return zenclient.JobPartitionPage{}, fmt.Errorf("failed to get jobs: %w", err)
	}

	if jobs.StatusCode() != 200 {
		return zenclient.JobPartitionPage{}, fmt.Errorf("failed to get jobs: %s", jobs.Status())
	}

	return ptr.Deref(jobs.JSON200, zenclient.JobPartitionPage{}), nil

}

func completeJob(t testing.TB, job zenclient.Job, vars map[string]any) error {
	response, err := app.restClient.CompleteJobWithResponse(t.Context(), job.Key, zenclient.CompleteJobJSONRequestBody{
		Variables: &vars,
	})
	if response.StatusCode() != 201 {
		return fmt.Errorf("status should be 201")
	}
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	return nil
}
