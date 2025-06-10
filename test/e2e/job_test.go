package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestRestApiJob(t *testing.T) {
	var instance public.ProcessInstance
	var definition public.ProcessDefinitionSimple
	err := deployDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	defintitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range defintitions {
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
	t.Run("activate job", func(t *testing.T) {
		jobs, err := activateJobs(t, "input-task-1")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs)
		jobToComplete = jobs[0]
		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, runtime.ActivityStateActive.String(), jobToComplete.State)

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

func activateJobs(t testing.TB, jobType string) ([]public.Job, error) {
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	respBytes, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/jobs/%s/activate", jobType)).
		WithMethod("POST").
		WithContext(ctx).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to activate job: %w", err)
	}
	resp := []public.Job{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return resp, fmt.Errorf("failed to unmarshal activated jobs: %w", err)
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
