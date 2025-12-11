package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestRestApiProcessInstance(t *testing.T) {
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

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("read instance state", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		fetchedDefinition, err := getDefinitionDetail(t, fetchedInstance.ProcessDefinitionKey)
		assert.NoError(t, err)
		assert.Equal(t, fetchedDefinition.Key, fetchedInstance.ProcessDefinitionKey)
		assert.Nil(t, fetchedInstance.ParentProcessInstanceKey)
		assert.Equal(t, fetchedDefinition.BpmnProcessId, "service-task-input-output")
		assert.Equal(t, map[string]any{"testVar": float64(123)}, fetchedInstance.Variables)
	})

	t.Run("read process instance jobs", func(t *testing.T) {
		jobs, err := getProcessInstanceJobs(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs)
		for _, job := range jobs {
			assert.Equal(t, instance.Key, job.ProcessInstanceKey)
			assert.NotEmpty(t, job.Key)
		}
	})
	t.Run("read process instance activities", func(t *testing.T) {
		// TODO: we dont have activities now
	})
}

func TestRestApiParentProcessInstance(t *testing.T) {
	var instance public.ProcessInstance
	var definition public.ProcessDefinitionSimple
	err := deployDefinition(t, "call-activity-simple.bpmn")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	defintitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range defintitions {
		if def.BpmnProcessId == "Simple_CallActivity_Process" {
			definition = def
			break
		}
	}

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("read instance state", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		fetchedDefinition, err := getDefinitionDetail(t, fetchedInstance.ProcessDefinitionKey)
		assert.NoError(t, err)
		assert.Equal(t, fetchedDefinition.Key, fetchedInstance.ProcessDefinitionKey)
		assert.Nil(t, fetchedInstance.ParentProcessInstanceKey)
		assert.Equal(t, fetchedDefinition.BpmnProcessId, "Simple_CallActivity_Process")

	})

	t.Run("read instance children", func(t *testing.T) {
		childrenPage, err := getChildInstances(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, 1, childrenPage.Count)
		assert.NotEmpty(t, childrenPage.Partitions)
		assert.NotEmpty(t, childrenPage.Partitions[0].Items)
		assert.Equal(t, instance.Key, *childrenPage.Partitions[0].Items[0].ParentProcessInstanceKey)
	})
}

func createProcessInstance(t testing.TB, processDefinitionKey int64, variables map[string]any) (public.ProcessInstance, error) {
	req := public.CreateProcessInstanceJSONBody{
		ProcessDefinitionKey: processDefinitionKey,
		Variables:            &variables,
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/process-instances").
		WithMethod("POST").
		WithBody(req).
		DoOk()
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to create process instance: %w", err)
	}
	instance := public.ProcessInstance{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return instance, nil
}

func getProcessInstance(t testing.TB, key int64) (public.ProcessInstance, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d", key)).
		DoOk()
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to read process instance: %w", err)
	}
	instance := public.ProcessInstance{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return instance, nil
}

func getChildInstances(t testing.TB, key int64) (public.ProcessInstancePage, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances?parentProcessInstanceKey=%d", key)).
		DoOk()
	if err != nil {
		return public.ProcessInstancePage{}, fmt.Errorf("failed to read process instance: %w", err)
	}
	page := public.ProcessInstancePage{}

	err = json.Unmarshal(resp, &page)
	if err != nil {
		return public.ProcessInstancePage{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return page, nil
}

func getProcessInstanceJobs(t testing.TB, key int64) ([]public.Job, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d/jobs", key)).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to read process instance jobs: %w", err)
	}
	jobPage := public.JobPage{}

	err = json.Unmarshal(resp, &jobPage)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job page: %w", err)
	}
	return jobPage.Items, nil
}

func getProcessInstanceIncidents(t testing.TB, key int64) ([]public.Incident, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d/incidents", key)).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to read process instance incidents: %w", err)
	}
	incidentPage := public.IncidentPage{}

	err = json.Unmarshal(resp, &incidentPage)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal incident page: %w", err)
	}
	return incidentPage.Items, nil
}
