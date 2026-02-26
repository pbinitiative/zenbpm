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

func TestCancelProcessInstance(t *testing.T) {
	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "service-task-input-output.bpmn", "service-task-input-output")

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})
	t.Run("read instance, state is active", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, public.ProcessInstanceStateActive, fetchedInstance.State)
	})
	t.Run("cancel process instance", func(t *testing.T) {
		cancelResponse, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, 204, cancelResponse.StatusCode())
	})
	t.Run("read instance, state is terminated", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, public.ProcessInstanceStateTerminated, fetchedInstance.State)
	})
}

func TestRestApiParentProcessInstance(t *testing.T) {
	cleanProcessInstances(t)

	var instance public.ProcessInstance
	definition, err := deployGetDefinition(t, "call-activity-simple.bpmn", "Simple_CallActivity_Process")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, definition.Key, map[string]any{
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
		assert.NoError(t, err)
		assert.Equal(t, public.ProcessInstanceStateFailed, invalidInstance.State)
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

func TestIncludeChildProcesses(t *testing.T) {
	cleanProcessInstances(t)

	multiInstanceDefinition, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	callActivityDefinition, err := deployGetUniqueDefinition(t, "call-activity-simple.bpmn")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition, err := deployGetUniqueDefinition(t, "simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)

		instance2, err := createProcessInstance(t, callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)

		instance3, err := createProcessInstance(t, subprocessDefinition.Key, map[string]any{
			"variable_name": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance3.Key)
	})

	time.Sleep(500 * time.Millisecond)

	t.Run("find process instances by IncludeChildProcesses=true", func(t *testing.T) {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			IncludeChildProcesses: ptr.To(true),
			State:                 ptr.To(zenclient.GetProcessInstancesParamsState("active")),
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
			IncludeChildProcesses: ptr.To(false),
			State:                 ptr.To(zenclient.GetProcessInstancesParamsState("active")),
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
			State: ptr.To(zenclient.GetProcessInstancesParamsState("active")),
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

	multiInstanceDefinition, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	callActivityDefinition, err := deployGetUniqueDefinition(t, "call-activity-simple.bpmn")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition, err := deployGetUniqueDefinition(t, "simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	var instance1Key int64
	var instance2Key int64
	var instance3Key int64
	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
		instance1Key = instance1.Key

		instance2, err := createProcessInstance(t, callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
		instance2Key = instance2.Key

		instance3, err := createProcessInstance(t, subprocessDefinition.Key, map[string]any{
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
			ProcessDefinitionKey: nonExistingProcessDefinitionKey,
		})

		assert.Nil(t, resp.JSON201)
		assert.NotNil(t, resp.JSON404)
		assert.Equal(t, "NOT_FOUND", resp.JSON404.Code)
		assert.Contains(t, resp.JSON404.Message, "no process definition with key -1 was found")
	})
}

func TestCancelProcessInstanceInWrongStateReturnsConflict(t *testing.T) {
	t.Run("Return CONFLICT(409) response when trying to cancel instance in non-cancellable state", func(t *testing.T) {
		var instance public.ProcessInstance
		definition, err := deployGetDefinition(t, "parallel_flow_with_terminate_end_task.bpmn", "parallel_flow_with_terminate_end_task")
		assert.NoError(t, err)

		instance, err = createProcessInstance(t, definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		cancelResponse, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		assert.NotNil(t, cancelResponse.JSON409)
		assert.Equal(t, "CONFLICT", cancelResponse.JSON409.Code)
		assert.Contains(t, cancelResponse.JSON409.Message, "cannot cancel process instance")
		assert.Contains(t, cancelResponse.JSON409.Message, "it is not in correct state, expected=ActivityStateActive, actual=ActivityStateCompleted")
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
		WithPath(fmt.Sprintf("/v1/process-instances?parentProcessInstanceKey=%d&includeChildProcesses=true", key)).
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
