package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiStartProcessInstanceOnElements(t *testing.T) {
	var instance zenclient.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "fork-uncontrolled-join.bpmn")
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "fork-uncontrolled-join" {
			definition = def
			break
		}
	}

	t.Run("start process instance on elements", func(t *testing.T) {
		startingElementIds := []string{"id-a-1", "id-a-2"}
		resp, err := app.restClient.StartProcessInstanceOnElementsWithResponse(t.Context(), zenclient.StartProcessInstanceOnElementsJSONRequestBody{
			ProcessDefinitionKey: definition.Key,
			StartingElementIds:   startingElementIds,
			Variables:            &map[string]any{"order": map[string]any{"name": "test-order-name"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode())
		instance = *resp.JSON201

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
	var instance zenclient.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "service-task-input-output" {
			definition = def
			break
		}
	}

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("modify process instance", func(t *testing.T) {
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.ActiveElementInstances)
		assert.Equal(t, 1, len(processInstance.ActiveElementInstances))
		assert.NotEmpty(t, processInstance.ActiveElementInstances[0].ElementInstanceKey)

		ElementInstancesToTerminate := make([]zenclient.TerminateElementInstanceData, 0, 1)
		ElementInstancesToTerminate = append(ElementInstancesToTerminate, zenclient.TerminateElementInstanceData{
			ElementInstanceKey: processInstance.ActiveElementInstances[0].ElementInstanceKey,
		})
		ElementInstancesToStart := make([]zenclient.StartElementInstanceData, 0, 1)
		ElementInstancesToStart = append(ElementInstancesToStart, zenclient.StartElementInstanceData{
			ElementId: "user-task-2",
		})

		resp, err := app.restClient.ModifyProcessInstanceWithResponse(t.Context(), zenclient.ModifyProcessInstanceJSONRequestBody{
			ElementInstancesToStart:     &ElementInstancesToStart,
			ElementInstancesToTerminate: &ElementInstancesToTerminate,
			ProcessInstanceKey:          instance.Key,
			Variables:                   &map[string]any{"order": map[string]any{"name": "test-order-name"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode())
		instance = *resp.JSON201.ProcessInstance
		activeElementInstances := *resp.JSON201.ActiveElementInstances

		assert.Equal(t, definition.Key, instance.ProcessDefinitionKey)
		assert.Equal(t, map[string]any{"name": "test-order-name"}, instance.Variables["order"])
		assert.Equal(t, float64(123), instance.Variables["testVar"])
		assert.NotEmpty(t, activeElementInstances)
		assert.Equal(t, 1, len(activeElementInstances))
		assert.NotEmpty(t, activeElementInstances[0].ElementInstanceKey)
		assert.Equal(t, activeElementInstances[0].ElementId, "user-task-2")
		assert.NotEmpty(t, activeElementInstances[0].State)
		assert.NotEmpty(t, activeElementInstances[0].CreatedAt)

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
}

func TestStartProcessInstanceOnElementsWithResponse404Response(t *testing.T) {
	t.Run("StartProcessInstanceOnElementsWithResponse should return NOT_FOUND(404) on non existing ProcessDefinitionKey", func(t *testing.T) {
		var nonExistingProcessDefinitionKey int64 = -1
		response, _ := app.restClient.StartProcessInstanceOnElementsWithResponse(t.Context(),
			zenclient.StartProcessInstanceOnElementsJSONRequestBody{
				ProcessDefinitionKey: nonExistingProcessDefinitionKey,
			})
		assert.NotNil(t, response.JSON404)
		assert.Equal(t, "NOT_FOUND", response.JSON404.Code)
		assert.Contains(t, response.JSON404.Message, "no process definition with key -1 was found (prior loaded into the engine)")
	})
}
