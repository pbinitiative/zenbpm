package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestRestApiJob(t *testing.T) {
	var instance public.ProcessInstance
	var definition public.ProcessDefinitionSimple
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

	var jobToComplete public.Job
	var jobsProcessInstance public.ProcessInstance
	t.Run("read waiting jobs", func(t *testing.T) {
		jobsPartitionPage, err := getJobs(t, "input-task-1")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, public.JobStateActive, jobToComplete.State)

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

}

// JobsOptions holds all available parameters for the getJobs function
type JobsOptions struct {
	JobType            string
	State              string
	Assignee           string
	ProcessInstanceKey int64
	Page               int32
	Size               int32
	SortBy             string // enum: [createdAt, key, type, state]
	SortOrder          string // enum: [asc, desc]
}

func getJobs(t testing.TB, jobType string) (public.JobPartitionPage, error) {
	return getJobsWithOptions(t, JobsOptions{
		JobType: jobType,
		State:   string(public.JobStateActive),
	})
}

func getJobsWithOptions(t testing.TB, options JobsOptions) (public.JobPartitionPage, error) {
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	// Build query parameters
	queryParams := make([]string, 0)

	if options.JobType != "" {
		queryParams = append(queryParams, fmt.Sprintf("jobType=%s", options.JobType))
	}
	if options.State != "" {
		queryParams = append(queryParams, fmt.Sprintf("state=%s", options.State))
	}
	if options.Assignee != "" {
		queryParams = append(queryParams, fmt.Sprintf("assignee=%s", options.Assignee))
	}
	if options.ProcessInstanceKey > 0 {
		queryParams = append(queryParams, fmt.Sprintf("processInstanceKey=%d", options.ProcessInstanceKey))
	}
	if options.Page > 0 {
		queryParams = append(queryParams, fmt.Sprintf("page=%d", options.Page))
	}
	if options.Size > 0 {
		queryParams = append(queryParams, fmt.Sprintf("size=%d", options.Size))
	}
	if options.SortBy != "" {
		queryParams = append(queryParams, fmt.Sprintf("sortBy=%s", options.SortBy))
	}
	if options.SortOrder != "" {
		queryParams = append(queryParams, fmt.Sprintf("sortOrder=%s", options.SortOrder))
	}

	path := "/v1/jobs"
	if len(queryParams) > 0 {
		path = fmt.Sprintf("%s?%s", path, strings.Join(queryParams, "&"))
	}

	respBytes, err := app.NewRequest(t).
		WithPath(path).
		WithMethod("GET").
		WithContext(ctx).
		DoOk()
	if err != nil {
		return public.JobPartitionPage{}, fmt.Errorf("failed to get jobs: %w", err)
	}
	resp := public.JobPartitionPage{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return resp, fmt.Errorf("failed to unmarshal jobs response: %w", err)
	}
	return resp, nil
}

func completeJob(t testing.TB, job public.Job, vars map[string]any) error {
	_, status, _, err := app.NewRequest(t).
		WithPath("/v1/jobs").
		WithMethod("POST").
		WithBody(public.CompleteJobJSONBody{
			JobKey:    job.Key,
			Variables: &vars,
		}).
		Do()
	if status != 201 {
		return fmt.Errorf("status should be 201")
	}
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	return nil
}
