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
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "service-task-input-output.bpmn", false)
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
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "call-activity-simple.bpmn", false)
	assert.NoError(t, err)
	_, err = deployDefinition(t, "simple_task.bpmn", false)
	assert.NoError(t, err)

	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
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

func TestBusinessKey(t *testing.T) {
	var instance public.ProcessInstance
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, "service-task-input-output.bpmn", false)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == "service-task-input-output" {
			definition = def
			break
		}
	}

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
	var definition zenclient.ProcessDefinitionSimple
	uniqueDefinitionName, err := deployDefinition(t, "service-task-input-output.bpmn", true)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *uniqueDefinitionName {
			definition = def
			break
		}
	}

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
			BpmnProcessId: uniqueDefinitionName,
			CreatedFrom:   ptr.To(past),
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     ptr.To(zenclient.Desc),
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
			BpmnProcessId: uniqueDefinitionName,
			CreatedFrom:   ptr.To(past),
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     ptr.To(zenclient.Asc),
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
			BpmnProcessId: uniqueDefinitionName,
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
			BpmnProcessId: uniqueDefinitionName,
			CreatedFrom:   ptr.To(future),
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, processInstances.JSON200.TotalCount)
	})
}

func TestBpmnProcessId(t *testing.T) {
	var serviceTaskIODefinition, simpleCountLoopDefinition zenclient.ProcessDefinitionSimple
	serviceTaskIODefinitionName, err := deployDefinition(t, "service-task-input-output.bpmn", true)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *serviceTaskIODefinitionName {
			serviceTaskIODefinition = def
			break
		}
	}
	simpleCountLoopDefinitionName, err := deployDefinition(t, "simple-count-loop.bpmn", true)
	assert.NoError(t, err)
	definitions, err = listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *simpleCountLoopDefinitionName {
			simpleCountLoopDefinition = def
			break
		}
	}

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
			BpmnProcessId: simpleCountLoopDefinitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, *simpleCountLoopDefinitionName, *part.BpmnProcessId)
		}
	})
}

func TestState(t *testing.T) {
	var validDefinition, invalidDefinition zenclient.ProcessDefinitionSimple
	validDefinitionName, err := deployDefinition(t, "service-task-input-output.bpmn", true)
	assert.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *validDefinitionName {
			validDefinition = def
			break
		}
	}
	invalidDefinitionName, err := deployDefinition(t, "service-task-invalid-input.bpmn", true)
	assert.NoError(t, err)
	definitions, err = listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.BpmnProcessId == *invalidDefinitionName {
			invalidDefinition = def
			break
		}
	}

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
			BpmnProcessId: invalidDefinitionName,
			State:         ptr.To(zenclient.Failed),
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, zenclient.ProcessInstanceState("ActivityStateFailed"), part.State)
		}
	})
	t.Run("find process instances sorted by state asc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: validDefinitionName,
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     ptr.To(zenclient.Asc),
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
			BpmnProcessId: validDefinitionName,
			SortBy:        ptr.To(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     ptr.To(zenclient.Asc),
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
