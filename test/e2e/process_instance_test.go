package e2e

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestApiProcessInstance(t *testing.T) {
	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")

	t.Run("create process instance - by definition key", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("create process instance - by bpmn id", func(t *testing.T) {
		bpmnProcessId := "usertask-assignee-mapping-process"
		businessKey := "create-response-business-key"
		_, err := deployGetDefinition(t, "usertask-assignee-mapping.bpmn", bpmnProcessId)
		require.NoError(t, err)
		resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			BpmnProcessId:        &bpmnProcessId,
			BusinessKey:          &businessKey,
			HistoryTimeToLive:    nil,
			ProcessDefinitionKey: nil,
			Variables:            nil,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
		require.NotNil(t, resp.JSON201)
		assert.Equal(t, zenclient.ProcessInstanceProcessTypeDefault, resp.JSON201.ProcessType)
		assert.Empty(t, resp.JSON201.ActiveElementInstances)
		assert.Equal(t, &bpmnProcessId, resp.JSON201.BpmnProcessId)
		assert.Equal(t, &businessKey, resp.JSON201.BusinessKey)
	})

	t.Run("create process instance - with no identification", func(t *testing.T) {
		resp, _ := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			BpmnProcessId:        nil,
			BusinessKey:          nil,
			HistoryTimeToLive:    nil,
			ProcessDefinitionKey: nil,
			Variables:            nil,
		})
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
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

func TestCancelProcessInstance(t *testing.T) {
	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})
	t.Run("read instance, state is active", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)
	})
	t.Run("cancel process instance", func(t *testing.T) {
		cancelResponse, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, 204, cancelResponse.StatusCode())
	})
	t.Run("read instance, state is terminated", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateTerminated, fetchedInstance.State)

		assertExactCompletedProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_1pv0o34",
			"service-task-1",
		})
	})
}

func TestRestApiParentProcessInstance(t *testing.T) {
	cleanProcessInstances(t)

	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "call-activity-simple.bpmn", "Simple_CallActivity_Process")
	assert.NoError(t, err)
	_, err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"variable_name": 123,
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
	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")
	assert.NoError(t, err)

	randNum := fmt.Sprintf("%d", rand.Intn(10000000000))
	bk := "testBusinessKey-" + randNum

	t.Run("create process instance", func(t *testing.T) {
		resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			BpmnProcessId:        nil,
			BusinessKey:          &bk,
			HistoryTimeToLive:    nil,
			ProcessDefinitionKey: &definition.Key,
			Variables: &map[string]any{
				"testVar": 123,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode())
		instance = *resp.JSON201
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

	t.Run("find process instances by definition key", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			ProcessDefinitionKey: &definition.Key,
		})
		assert.NoError(t, err)
		assert.True(t, processInstances.JSON200.TotalCount > 0)
		for _, pi := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, definition.Key, pi.ProcessDefinitionKey)
			assert.NotEmpty(t, pi.Key)
		}
	})

	t.Run("complete service task", func(t *testing.T) {
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			ProcessInstanceKey: &instance.Key,
			State:              new(zenclient.JobStateActive),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs.Partitions)
		assert.NotEmpty(t, jobs.Partitions[0].Items)
		err = completeJob(t, jobs.Partitions[0].Items[0].Key, map[string]any{
			"city": "test",
		})
		assert.NoError(t, err)
	})

	t.Run("complete user task", func(t *testing.T) {
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			ProcessInstanceKey: &instance.Key,
			State:              new(zenclient.JobStateActive),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs.Partitions)
		assert.NotEmpty(t, jobs.Partitions[0].Items)
		err = completeJob(t, jobs.Partitions[0].Items[0].Key, map[string]any{})
		assert.NoError(t, err)
	})

	t.Run("business key present after process completes", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State)
		assert.Equal(t, bk, ptr.Deref(fetchedInstance.BusinessKey, ""))
	})

	t.Run("find completed process instances by business key", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BusinessKey: &bk,
		})
		assert.NoError(t, err)
		assert.True(t, processInstances.JSON200.TotalCount > 0)
		for _, pi := range processInstances.JSON200.Partitions[0].Items {
			assert.Equal(t, bk, ptr.Deref(pi.BusinessKey, ""))
		}
	})
}

func TestActiveElementInstances(t *testing.T) {

	definition, _ := deployGetDefinition(t, "process_instance/message_boundary_event_on_sub_process_interrupting.bpmn", "message_boundary_event_on_sub_process_interrupting")

	instance, _ := createProcessInstance(t, &definition.Key, map[string]any{
		"variable_name":  123,
		"correlationKey": "1fdafa4664das",
	})

	t.Run("Get process instance detail should return one active element instance with the correct values", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		log.Printf("instance: %+v", fetchedInstance)

		assert.NotNil(t, fetchedInstance.ActiveElementInstances)
		assert.Len(t, fetchedInstance.ActiveElementInstances, 1)

		activeElementInstance := fetchedInstance.ActiveElementInstances[0]

		assert.NotNil(t, activeElementInstance.ElementInstanceKey)
		assert.Equal(t, "sub_process", activeElementInstance.ElementId)
		assert.Equal(t, "TokenStateWaiting", activeElementInstance.State)
		require.WithinDuration(t, time.Now(), activeElementInstance.CreatedAt, time.Second)
	})
}

func TestCreatedAt(t *testing.T) {
	var instance1, instance2 zenclient.ProcessInstance
	var err error
	definition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-input-output.bpmn")

	t.Run("create process instance1", func(t *testing.T) {
		instance1, err = createProcessInstance(t, &definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
	})
	t.Run("create process instance2", func(t *testing.T) {
		instance2, err = createProcessInstance(t, &definition.Key, map[string]any{
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
			CreatedFrom:   new(past),
			SortBy:        new(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     new(zenclient.GetProcessInstancesParamsSortOrderDesc),
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
			CreatedFrom:   new(past),
			SortBy:        new(zenclient.GetProcessInstancesParamsSortByCreatedAt),
			SortOrder:     new(zenclient.GetProcessInstancesParamsSortOrderAsc),
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
			CreatedFrom:   new(past),
			SortBy:        new(zenclient.GetProcessInstancesParamsSortByCreatedAt),
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
			CreatedFrom:   new(future),
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, processInstances.JSON200.TotalCount)
	})
}

func TestBpmnProcessId(t *testing.T) {
	serviceTaskIODefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-input-output.bpmn")
	simpleCountLoopDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/simple-count-loop.bpmn")

	t.Run("create process instance1 for service-task-input-output.bpmn", func(t *testing.T) {
		instance1, err := createProcessInstance(t, &serviceTaskIODefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
	})
	t.Run("create process instance2 for simple-count-loop.bpmn", func(t *testing.T) {
		instance2, err := createProcessInstance(t, &simpleCountLoopDefinition.Key, map[string]any{
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
	validDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-input-output.bpmn")
	invalidDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-invalid-input.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		activeInstance, err := createProcessInstance(t, &validDefinition.Key, map[string]any{
			"testVar": 123,
		})
		require.NoError(t, err)
		require.NotEmpty(t, activeInstance.Key)
		require.Equal(t, zenclient.ProcessInstanceStateActive, activeInstance.State)

		terminatedInstance, err := createProcessInstance(t, &validDefinition.Key, map[string]any{
			"testVar": 123,
		})
		require.NoError(t, err)
		require.NotEmpty(t, terminatedInstance.Key)

		response, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), terminatedInstance.Key)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, response.StatusCode())
	})
	t.Run("create process instance for service-task-invalid-input.bpmn", func(t *testing.T) {
		invalidInstance, err := createProcessInstance(t, &invalidDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateFailed, invalidInstance.State)
	})

	t.Run("find process instances by state=failed", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &invalidDefinition.BpmnProcessId,
			State:         new(zenclient.GetProcessInstancesParamsStateFailed),
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
			SortBy:        new(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     new(zenclient.GetProcessInstancesParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		stateSlice := make([]zenclient.ProcessInstanceState, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			stateSlice = append(stateSlice, part.State)
		}
		assert.Equal(t, []zenclient.ProcessInstanceState{
			zenclient.ProcessInstanceStateActive,
			zenclient.ProcessInstanceStateTerminated,
		}, stateSlice)
	})
	t.Run("find process instances sorted by state desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &validDefinition.BpmnProcessId,
			SortBy:        new(zenclient.GetProcessInstancesParamsSortByState),
			SortOrder:     new(zenclient.GetProcessInstancesParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		stateSlice := make([]zenclient.ProcessInstanceState, 0, len(processInstances.JSON200.Partitions[0].Items))
		for _, part := range processInstances.JSON200.Partitions[0].Items {
			stateSlice = append(stateSlice, part.State)
		}
		assert.Equal(t, []zenclient.ProcessInstanceState{
			zenclient.ProcessInstanceStateTerminated,
			zenclient.ProcessInstanceStateActive,
		}, stateSlice)
	})
}

func TestIncludeChildProcesses(t *testing.T) {
	cleanProcessInstances(t)

	multiInstanceDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/multi_instance_service_task.bpmn")

	callActivityDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/call-activity-simple.bpmn")
	_, err := deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/simple_sub_process_task.bpmn")

	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, &multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)

		instance2, err := createProcessInstance(t, &callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)

		instance3, err := createProcessInstance(t, &subprocessDefinition.Key, map[string]any{
			"variable_name": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance3.Key)
	})

	time.Sleep(500 * time.Millisecond)

	t.Run("find process instances by IncludeChildProcesses=true", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			IncludeChildProcesses: new(true),
			State:                 new(zenclient.GetProcessInstancesParamsState("active")),
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		for _, instance := range processInstances.JSON200.Partitions[0].Items {
			assert.Contains(t, []zenclient.ProcessInstanceProcessType{"default", "callActivity", "multiInstance", "subprocess"}, instance.ProcessType)
		}
	})

	t.Run("find process instances by IncludeChildProcesses=false", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			IncludeChildProcesses: new(false),
			State:                 new(zenclient.GetProcessInstancesParamsState("active")),
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		for _, instance := range processInstances.JSON200.Partitions[0].Items {
			assert.Contains(t, []zenclient.ProcessInstanceProcessType{"default", "callActivity"}, instance.ProcessType)
		}
	})

	t.Run("find process instances by IncludeChildProcesses not filled out", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			State: new(zenclient.GetProcessInstancesParamsState("active")),
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		for _, instance := range processInstances.JSON200.Partitions[0].Items {
			assert.Contains(t, []zenclient.ProcessInstanceProcessType{"default", "callActivity"}, instance.ProcessType)
		}
	})
	assert.NoError(t, err)
}

func TestFindChildProcesses(t *testing.T) {
	cleanProcessInstances(t)

	multiInstanceDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/multi_instance_service_task.bpmn")

	callActivityDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/call-activity-simple.bpmn")
	_, err := deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/simple_sub_process_task.bpmn")

	var instance1Key int64
	var instance2Key int64
	var instance3Key int64
	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, &multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
		instance1Key = instance1.Key

		instance2, err := createProcessInstance(t, &callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
		instance2Key = instance2.Key

		instance3, err := createProcessInstance(t, &subprocessDefinition.Key, map[string]any{
			"variable_name": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance3.Key)
		instance3Key = instance3.Key
	})

	time.Sleep(500 * time.Millisecond)

	t.Run("find child process instances multiInstance", func(t *testing.T) {
		processInstances, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance1Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions[0].Items))
		assert.Equal(t, zenclient.ProcessInstanceProcessType("multiInstance"), processInstances.JSON200.Partitions[0].Items[0].ProcessType)
	})

	t.Run("find child process instances callActivity", func(t *testing.T) {
		processInstances, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance2Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions[0].Items))
		assert.Equal(t, zenclient.ProcessInstanceProcessType("callActivity"), processInstances.JSON200.Partitions[0].Items[0].ProcessType)
	})

	t.Run("find child process instances subprocess", func(t *testing.T) {
		processInstances, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance3Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions))
		assert.Equal(t, 1, len(processInstances.JSON200.Partitions[0].Items))
		assert.Equal(t, zenclient.ProcessInstanceProcessType("subprocess"), processInstances.JSON200.Partitions[0].Items[0].ProcessType)
	})
	assert.NoError(t, err)
}

func TestUpdateProcessInstanceVariables(t *testing.T) {
	var processInstanceKey int64
	definition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-input-output.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{
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
	definition := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/service-task-input-output.bpmn")

	t.Run("create process instance for service-task-input-output.bpmn", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{
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

func TestGetProcessInstanceErrorResponse(t *testing.T) {
	t.Run("read non existing process instance", func(t *testing.T) {
		var nonExistingProcessInstanceKey int64 = -1
		var resp *zenclient.GetProcessInstanceResponse
		resp, _ = app.restClient.GetProcessInstanceWithResponse(t.Context(), nonExistingProcessInstanceKey)

		assert.Nil(t, resp.JSON200)
		assert.NotNil(t, resp.JSON502)
		assert.Equal(t, "CLUSTER_ERROR", resp.JSON502.Code)
		assert.Equal(t, "failed to get follower node to get process instance: partition not found", resp.JSON502.Message)
	})
}

func TestCreateProcessInstanceNotFoundResponse(t *testing.T) {
	t.Run("try to create process instance with non-existing process definition key. Expect NOT_FOUND", func(t *testing.T) {
		var nonExistingProcessDefinitionKey int64 = -1
		var resp *zenclient.CreateProcessInstanceResponse
		resp, _ = app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
			ProcessDefinitionKey: &nonExistingProcessDefinitionKey,
		})

		assert.Nil(t, resp.JSON201)
		assert.NotNil(t, resp.JSON404)
		assert.Equal(t, "NOT_FOUND", resp.JSON404.Code)
		assert.Contains(t, resp.JSON404.Message, "no process definition with key -1 was found")
	})
}

func TestGetProcessInstancesBadRequest(t *testing.T) {
	t.Run("GetProcessInstances with invalid state would return a BadRequest", func(t *testing.T) {
		var resp *zenclient.GetProcessInstancesResponse
		resp, _ = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			State: (*zenclient.GetProcessInstancesParamsState)(new("invalid-state")),
		})

		assert.Nil(t, resp.JSON200)
		assert.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
		assert.Contains(t, resp.JSON400.Message, `parameter "state" in query has an error`)
		assert.Contains(t, resp.JSON400.Message, "value is not one of the allowed values")
	})
}

func TestDeleteAndUpdateProcessInstanceVariablesAndCancelReturnsConflict(t *testing.T) {
	var instanceKey int64
	t.Run("Create instance with completed state", func(t *testing.T) {
		var instance zenclient.ProcessInstance
		definition, err := deployGetDefinition(t, "parallel_flow_with_terminate_end_task.bpmn", "parallel_flow_with_terminate_end_task")
		assert.NoError(t, err)

		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"var1": "var1 value",
		})
		instanceKey = instance.Key
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("Return CONFLICT(409) response when trying to update process instance variable in non active state ", func(t *testing.T) {
		response, _ := app.restClient.UpdateProcessInstanceVariablesWithResponse(t.Context(), instanceKey, zenclient.UpdateProcessInstanceVariablesJSONRequestBody{
			Variables: map[string]any{
				"var1":    "var1 value changed",
				"newVar2": "var2 value",
			},
		})
		assert.NotNil(t, response.JSON409)
		assert.Equal(t, "CONFLICT", response.JSON409.Code)
		assert.Equal(t, "Can update variables only for process instances in active or failed state", response.JSON409.Message)
	})

	t.Run("Return CONFLICT(409) response when trying to delete process instance variable in non active state ", func(t *testing.T) {
		response, _ := app.restClient.DeleteProcessInstanceVariableWithResponse(t.Context(), instanceKey, "var1")
		assert.NotNil(t, response.JSON409)
		assert.Equal(t, "CONFLICT", response.JSON409.Code)
		assert.Equal(t, "can delete variables only for process instances in active state", response.JSON409.Message)
	})

	t.Run("Return CONFLICT(409) response when trying to cancel process instance variable in non active state ", func(t *testing.T) {
		cancelResponse, _ := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instanceKey)
		assert.NotNil(t, cancelResponse.JSON409)
		assert.Equal(t, "CONFLICT", cancelResponse.JSON409.Code)
		assert.Contains(t, cancelResponse.JSON409.Message, "cannot cancel process instance")
		assert.Contains(t, cancelResponse.JSON409.Message, "it is not in correct state, expected=ActivityStateActive, actual=ActivityStateCompleted")
	})
}

func deployGetDefinition(t *testing.T, filename string, bpmnProcessId string) (zenclient.ProcessDefinitionSimple, error) {
	var definition zenclient.ProcessDefinitionSimple
	_, err := deployDefinition(t, filename)
	if err != nil {
		return definition, err
	}
	definitions, err := listProcessDefinitions(t)
	if err != nil {
		return definition, err
	}
	for _, def := range definitions {
		if def.BpmnProcessId == bpmnProcessId {
			definition = def
			break
		}
	}
	return definition, nil
}
