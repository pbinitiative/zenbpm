package e2e

import (
	"encoding/json"
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRestApiMigrationStartProcessInstanceOnElements(t *testing.T) {
	var instance public.ProcessInstance
	var definition public.ProcessDefinitionSimple
	err := deployDefinition(t, "fork-uncontrolled-join.bpmn")
	assert.NoError(t, err)
	defintitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range defintitions {
		if def.BpmnProcessId == "fork-uncontrolled-join" {
			definition = def
			break
		}
	}

	t.Run("start process instance on elements", func(t *testing.T) {
		startingElementIds := []string{"id-a-1", "id-a-2"}
		instance, err = startProcessInstanceOnElements(t, definition.Key, startingElementIds, map[string]any{
			"order": map[string]any{"name": "test-order-name"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("read instance state", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		fetchedDefinition, err := getDefinitionDetail(t, fetchedInstance.ProcessDefinitionKey)
		assert.NoError(t, err)
		assert.Equal(t, fetchedDefinition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, fetchedDefinition.BpmnProcessId, "fork-uncontrolled-join")
		assert.Equal(t, map[string]any{"order": map[string]any{"name": "test-order-name"}}, instance.Variables)
	})

	t.Run("read process instance jobs", func(t *testing.T) {
		jobs, err := getProcessInstanceJobs(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobs))
		for _, job := range jobs {
			assert.Equal(t, instance.Key, job.ProcessInstanceKey)
			assert.NotEmpty(t, job.Key)
		}
	})
}

func startProcessInstanceOnElements(t testing.TB, processDefinitionKey string, startingElementIds []string, variables map[string]any) (public.ProcessInstance, error) {
	req := public.StartProcessInstanceOnElementsJSONBody{
		ProcessDefinitionKey: processDefinitionKey,
		StartingElementIds:   startingElementIds,
		Variables:            &variables,
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/migration/start-process-instance").
		WithMethod("POST").
		WithBody(req).
		DoOk()
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to start process instance: %w", err)
	}
	instance := public.ProcessInstance{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return instance, nil
}
