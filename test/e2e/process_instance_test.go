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
	})
	t.Run("read instance state", func(t *testing.T) {
		instance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		definition, err := getDefinitionDetail(t, instance.ProcessDefinitionKey)
		assert.NoError(t, err)
		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, definition.BpmnProcessId, "service-task-input-output")
		assert.Equal(t, map[string]any{"testVar": float64(123)}, instance.Variables)
	})
}

func createProcessInstance(t testing.TB, processDefinitionKey string, variables map[string]any) (public.ProcessInstance, error) {
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

func getProcessInstance(t testing.TB, key string) (public.ProcessInstance, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%s", key)).
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
