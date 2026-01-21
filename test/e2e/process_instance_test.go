package e2e

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiProcessInstance(t *testing.T) {
	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")

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
	definition, err := deployGetDefinition(t, "call-activity-simple.bpmn", "Simple_CallActivity_Process")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

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

func TestBusinessKey(t *testing.T) {
	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")
	assert.NoError(t, err)

	randNum := fmt.Sprintf("%d", rand.Intn(10000000000))
	bk := "testBusinessKey-" + randNum

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstanceWithBusinessKey(t, definition.Key, &bk, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("read instance state", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		log.Printf("instance: %+v", fetchedInstance)
		assert.Equal(t, bk, ptr.Deref(fetchedInstance.BusinessKey, ""))
	})

	t.Run("find process instances by business key", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BusinessKey: &bk,
		})
		assert.NoError(t, err)
		assert.True(t, processInstances.JSON200.TotalCount > 0)
		for _, pi := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, bk, ptr.Deref(pi.BusinessKey, ""))
			assert.NotEmpty(t, pi.Key)
		}
	})
}

func TestCreatedAt(t *testing.T) {
	var instance1, instance2 public.ProcessInstance
	definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)

	t.Run("create process instance1", func(t *testing.T) {
		instance1, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
	})
	t.Run("create process instance2", func(t *testing.T) {
		instance2, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
	})

	past := time.Now().AddDate(0, 0, -1)
	future := time.Now().AddDate(0, 0, 1)
	t.Run("find process instances by createdAt in past sorted desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
			CreatedFrom:   ptr.To(past),
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     ptr.To(zenclient.GetProcessInstancesParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		createdAtSlice := make([]int64, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			createdAtSlice = append(createdAtSlice, part.CreatedAt.UnixMilli())
		}
		assert.True(t, sort.SliceIsSorted(createdAtSlice, func(p, q int) bool { return createdAtSlice[p] > createdAtSlice[q] })) // createdAt's are sorted desc
	})
	t.Run("find process instances by createdAt in past sorted asc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
			CreatedFrom:   ptr.To(past),
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     ptr.To(zenclient.GetProcessInstancesParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		createdAtSlice := make([]int64, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			createdAtSlice = append(createdAtSlice, part.CreatedAt.UnixMilli())
		}
		assert.True(t, sort.SliceIsSorted(createdAtSlice, func(p, q int) bool { return createdAtSlice[p] < createdAtSlice[q] }))
	})
	t.Run("find process instances by createdAt in past by default created_at desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
			CreatedFrom:   ptr.To(past),
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByCreatedAt),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		createdAtSlice := make([]int64, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			createdAtSlice = append(createdAtSlice, part.CreatedAt.UnixMilli())
		}
		assert.True(t, sort.SliceIsSorted(createdAtSlice, func(p, q int) bool { return createdAtSlice[p] > createdAtSlice[q] }))
	})
	t.Run("find process instances by createdAt in future", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
			CreatedFrom:   ptr.To(future),
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, processInstances.JSON200.TotalCount)
	})
}

func TestBpmnProcessId(t *testing.T) {
	serviceTaskIODefinition, _ := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	simpleCountLoopDefinition, _ := deployGetUniqueDefinition(t, "simple-count-loop.bpmn")

	t.Run("create process instance1 for service-task-input-output.bpmn", func(t *testing.T) {
		instance1, err := createProcessInstance(t, serviceTaskIODefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
	})
	t.Run("create process instance2 for simple-count-loop.bpmn", func(t *testing.T) {
		instance2, err := createProcessInstance(t, simpleCountLoopDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
	})

	t.Run("find process instances by bpmnProcessId=simple-count-loop", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &simpleCountLoopDefinition.BpmnProcessId,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, simpleCountLoopDefinition.BpmnProcessId, *part.BpmnProcessId)
		}
	})
}

func TestState(t *testing.T) {
	validDefinition, _ := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	invalidDefinition, _ := deployGetUniqueDefinition(t, "service-task-invalid-input.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		instance1, err := createProcessInstance(t, validDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
		instance2, err := createProcessInstance(t, validDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
	})
	t.Run("create process instance for service-task-invalid-input.bpmn", func(t *testing.T) {
		invalidInstance, err := createProcessInstance(t, invalidDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.Error(t, err)
		assert.Empty(t, invalidInstance.Key)
	})

	t.Run("find process instances by state=failed", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &invalidDefinition.BpmnProcessId,
			State:         ptr.To(zenclient.GetProcessInstancesParamsStateFailed),
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, zenclient.ProcessInstanceState("failed"), part.State)
		}
	})
	t.Run("find process instances sorted by state asc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &validDefinition.BpmnProcessId,
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     ptr.To(zenclient.GetProcessInstancesParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		stateSlice := make([]string, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			stateSlice = append(stateSlice, (string)(part.State))
		}
		assert.True(t, sort.SliceIsSorted(stateSlice, func(p, q int) bool { return stateSlice[p] < stateSlice[q] }))
	})
	t.Run("find process instances sorted by state desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &validDefinition.BpmnProcessId,
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     ptr.To(zenclient.GetProcessInstancesParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		stateSlice := make([]string, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			stateSlice = append(stateSlice, (string)(part.State))
		}
		assert.True(t, sort.SliceIsSorted(stateSlice, func(p, q int) bool { return stateSlice[p] > stateSlice[q] }))
	})
}

func TestUpdateProcessInstanceVariables(t *testing.T) {
	var processInstanceKey int64
	definition, _ := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		instance, err := createProcessInstance(t, definition.Key, map[string]any{
			"var1": "var1 value",
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
		processInstanceKey = instance.Key
	})

	t.Run("testUpdateProcessInstanceVariables", func(t *testing.T) {
		_, err := app.restClient.UpdateProcessInstanceVariablesWithResponse(t.Context(), processInstanceKey, zenclient.UpdateProcessInstanceVariablesJSONRequestBody{
			Variables: map[string]any{
				"var1":    "var1 value changed",
				"newVar2": "var2 value",
			},
		})
		assert.NoError(t, err)
		fetchedInstance, err := getProcessInstance(t, processInstanceKey)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"var1": "var1 value changed", "newVar2": "var2 value"}, fetchedInstance.Variables)
	})
}

func TestDeleteProcessInstanceVariable(t *testing.T) {
	var processInstanceKey int64
	definition, _ := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		instance, err := createProcessInstance(t, definition.Key, map[string]any{
			"var1": "var1 value",
			"var2": "var2 value",
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
		processInstanceKey = instance.Key
	})

	t.Run("TestDeleteProcessInstanceVariable for existing variable", func(t *testing.T) {
		_, err := app.restClient.DeleteProcessInstanceVariableWithResponse(t.Context(), processInstanceKey, "var1")
		assert.NoError(t, err)
		fetchedInstance, err := getProcessInstance(t, processInstanceKey)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"var2": "var2 value"}, fetchedInstance.Variables)
	})

	t.Run("TestDeleteProcessInstanceVariable for non-existing variable", func(t *testing.T) {
		deleteProcessInstanceVariableResponse, _ := app.restClient.DeleteProcessInstanceVariableWithResponse(t.Context(), processInstanceKey, "non-existing-variable")
		assert.Equal(t, "NOT_FOUND", deleteProcessInstanceVariableResponse.JSON404.Code)
	})
}

func createProcessInstance(t testing.TB, processDefinitionKey int64, variables map[string]any) (public.ProcessInstance, error) {
	return createProcessInstanceWithBusinessKey(t, processDefinitionKey, nil, variables)
}

func createProcessInstanceWithBusinessKey(t testing.TB, processDefinitionKey int64, businessKey *string, variables map[string]any) (public.ProcessInstance, error) {
	req := public.CreateProcessInstanceJSONBody{
		ProcessDefinitionKey: processDefinitionKey,
		BusinessKey:          businessKey,
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

func deployGetDefinition(t *testing.T, filename string, bpmnProcessId string) (zenclient.ProcessDefinitionSimple, error) {
	var definition zenclient.ProcessDefinitionSimple
	err := deployDefinition(t, filename)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == bpmnProcessId {
			definition = def
			break
		}
	}
	return definition, err
}

func deployGetUniqueDefinition(t *testing.T, filename string) (zenclient.ProcessDefinitionSimple, error) {
	var definition zenclient.ProcessDefinitionSimple
	uniqueDefinitionName, err := deployUniqueDefinition(t, filename)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *uniqueDefinitionName {
			definition = def
			break
		}
	}
	return definition, err
}
