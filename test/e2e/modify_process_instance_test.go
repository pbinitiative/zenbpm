package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestRestApiStartProcessInstanceOnElements(t *testing.T) {
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

func TestRestApiModifyProcessInstance(t *testing.T) {
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

	t.Run("modify process instance tokens", func(t *testing.T) {
		executionTokens, err := getExecutionTokens(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, executionTokens)
		assert.Equal(t, 1, len(executionTokens))
		assert.NotEmpty(t, executionTokens[0].Key)

		executionTokensToTerminate := make([]public.TerminateExecutionData, 0, 1)
		executionTokensToTerminate = append(executionTokensToTerminate, public.TerminateExecutionData{
			ExecutionTokenKey: executionTokens[0].Key,
		})
		executionTokensToStart := make([]public.StartExecutionData, 0, 1)
		executionTokensToStart = append(executionTokensToStart, public.StartExecutionData{
			ElementId: "user-task-2",
		})

		instance, tokens, err := modifyProcessInstanceTokens(t, instance.Key, executionTokensToTerminate, executionTokensToStart, map[string]any{
			"order": map[string]any{"name": "test-order-name"},
		})
		assert.NoError(t, err)
		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, map[string]any{"name": "test-order-name"}, instance.Variables["order"])
		assert.Equal(t, float64(123), instance.Variables["testVar"])
		assert.NotEmpty(t, tokens)
		assert.Equal(t, 1, len(tokens))
		assert.NotEmpty(t, tokens[0].Key)
		assert.Equal(t, tokens[0].ElementId, "user-task-2")
		assert.Equal(t, tokens[0].ProcessInstanceKey, instance.Key)

		instance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, map[string]any{"name": "test-order-name"}, instance.Variables["order"])
		assert.Equal(t, float64(123), instance.Variables["testVar"])
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

	t.Run("modify process instance variables", func(t *testing.T) {
		instance, err := modifyProcessInstanceVariables(t, instance.Key, map[string]any{
			"order": map[string]any{"name": "edited-variable-name"},
		})
		assert.NoError(t, err)
		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, map[string]any{"name": "edited-variable-name"}, instance.Variables["order"])
		assert.Equal(t, float64(123), instance.Variables["testVar"])

		instance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, map[string]any{"name": "edited-variable-name"}, instance.Variables["order"])
		assert.Equal(t, float64(123), instance.Variables["testVar"])
	})
}

func startProcessInstanceOnElements(t testing.TB, processDefinitionKey string, startingElementIds []string, variables map[string]any) (public.ProcessInstance, error) {
	req := public.StartProcessInstanceOnElementsJSONBody{
		ProcessDefinitionKey: processDefinitionKey,
		StartingElementIds:   startingElementIds,
		Variables:            &variables,
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/modify/start-process-instance").
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

func modifyProcessInstanceTokens(t testing.TB, processInstanceKey string, executionTokensToTerminate []public.TerminateExecutionData, executionTokensToStart []public.StartExecutionData, variables map[string]any) (public.ProcessInstance, []public.ExecutionToken, error) {
	req := public.ModifyProcessInstanceJSONBody{
		ExecutionTokensToStart:     &executionTokensToStart,
		ExecutionTokensToTerminate: &executionTokensToTerminate,
		ProcessInstanceKey:         processInstanceKey,
		Variables:                  &variables,
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/modify/process-instance").
		WithMethod("POST").
		WithBody(req).
		DoOk()
	if err != nil {
		return public.ProcessInstance{}, []public.ExecutionToken{}, fmt.Errorf("failed to modify process instance: %w", err)
	}
	unmarshalledResp := public.ModifyProcessInstance201JSONResponse{}

	err = json.Unmarshal(resp, &unmarshalledResp)
	if err != nil {
		return public.ProcessInstance{}, []public.ExecutionToken{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return *unmarshalledResp.ProcessInstance, *unmarshalledResp.ExecutionTokens, nil
}

func getExecutionTokens(t testing.TB, processInstanceKey string) ([]public.ExecutionToken, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instance/%s/execution-tokens", processInstanceKey)).
		WithMethod("GET").
		DoOk()
	if err != nil {
		return []public.ExecutionToken{}, fmt.Errorf("failed to get execution tokens in process instance: %w", err)
	}

	unmarshalledResp := public.GetExecutionTokens200JSONResponse{}
	err = json.Unmarshal(resp, &unmarshalledResp)
	if err != nil {
		return []public.ExecutionToken{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return unmarshalledResp.Items, nil
}

func modifyProcessInstanceVariables(t testing.TB, processInstanceKey string, variables map[string]any) (public.ProcessInstance, error) {
	req := public.ModifyProcessInstanceVariablesJSONBody{
		ProcessInstanceKey: processInstanceKey,
		Variables:          &variables,
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/modify/process-instance/variables").
		WithMethod("POST").
		WithBody(req).
		DoOk()
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to modify process instance variables: %w", err)
	}
	instance := public.ProcessInstance{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return public.ProcessInstance{}, fmt.Errorf("failed to unmarshal process instance: %w", err)
	}
	return instance, nil
}
