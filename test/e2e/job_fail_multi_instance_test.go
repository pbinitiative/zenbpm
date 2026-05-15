package e2e

import (
	"fmt"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestGrpcJobFailOnParallelMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch all", ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event_and_output_mapping.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandledWithPropagatedVariables(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task incident", ptr.To("99"))

		assertMultiInstanceUnmatchedIncident(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task"})
	})
}

func TestRestJobFailOnParallelMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-rest-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event_and_output_mapping.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandledWithPropagatedVariables(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/parallel_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		assertMultiInstanceUnmatchedIncident(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task"})
	})
}

func TestGrpcJobFailOnSequentialMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task catch all", ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event_and_output_mapping.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandledWithPropagatedVariables(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-grpc-incident-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task incident", ptr.To("99"))

		assertMultiInstanceUnmatchedIncident(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task"})
	})
}

func TestRestJobFailOnSequentialMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("matching_error_code_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event_and_output_mapping.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandledWithPropagatedVariables(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})
	})

	t.Run("nested_matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_in_subprocess_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithVariables(t, definition.ProcessDefinitionKey, map[string]any{
			"variable_name":       "test-value",
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 0)

		subProcessInstance := waitForChildProcessInstanceByType(t, processInstance.Key, zenclient.ProcessInstanceProcessTypeSubprocess)

		multiInstanceProcess := waitForChildProcessInstanceByType(t, subProcessInstance.Key, zenclient.ProcessInstanceProcessTypeMultiInstance)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, multiInstanceProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)
		assertProcessInstanceTokenElements(t, multiInstanceProcess.Key, []string{"service_task"}, []string{"error_boundary_event_end", "end"})
		assertProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task", "Event_15f0ox7"})

		assertProcessInstanceTokenState(t, subProcessInstance.Key, "error_boundary_event_end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, subProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, subProcessInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, subProcessInstance.Key, []string{"error_boundary_event_end"}, []string{"end"})
		assertProcessInstanceHistory(t, subProcessInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})

		assertProcessInstanceTokenState(t, processInstance.Key, "Event_11axlot", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"testInputCollection": []any{"test1", "test2", "test3"},
			"variable_name":       "test-value",
		})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"Event_11axlot"}, []string{"Activity_11wye3s"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_1llmj1k", "Event_0m9r0ve", "Activity_11wye3s", "Flow_0n7mtnz", "Event_11axlot"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-incident-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		assertMultiInstanceUnmatchedIncident(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"service_task",
			"error_boundary_event_end",
			"end",
		)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task"})
	})
}

func createMultiInstanceProcessInstance(t testing.TB, definitionKey int64) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	processInstance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{
		"variable_name":       "test-value",
		"testInputCollection": []string{"test1", "test2", "test3"},
	})
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

	multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)

	return processInstance, multiInstanceProcess
}

func assertMultiInstanceBoundaryHandled(t testing.TB, parentKey int64, childKey int64, childElementId string, handledEndId string, defaultEndId string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateCompleted, childKey, zenclient.ProcessInstanceStateTerminated)
	assertProcessInstanceTokenState(t, childKey, childElementId, bpmnruntime.TokenStateCanceled)
	assertProcessInstanceIncidentsLength(t, childKey, 0)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertProcessInstanceTokenElements(t, childKey, []string{childElementId}, []string{handledEndId, defaultEndId})

	assertProcessInstanceTokenState(t, parentKey, handledEndId, bpmnruntime.TokenStateCompleted)
	assertProcessInstanceVariables(t, parentKey, map[string]any{
		"testInputCollection":   []any{"test1", "test2", "test3"},
		"variable_from_request": "request_variable",
		"variable_name":         "test-value",
	})
	assertProcessInstanceIncidentsLength(t, parentKey, 0)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 0, 1)
	assertProcessInstanceTokenElements(t, parentKey, []string{handledEndId}, []string{defaultEndId})
}

func assertMultiInstanceBoundaryHandledWithPropagatedVariables(t testing.TB, parentKey int64, childKey int64, childElementId string, handledEndId string, defaultEndId string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateCompleted, childKey, zenclient.ProcessInstanceStateTerminated)
	assertProcessInstanceTokenState(t, childKey, childElementId, bpmnruntime.TokenStateCanceled)
	assertProcessInstanceIncidentsLength(t, childKey, 0)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertProcessInstanceTokenElements(t, childKey, []string{childElementId}, []string{handledEndId, defaultEndId})

	assertProcessInstanceTokenState(t, parentKey, handledEndId, bpmnruntime.TokenStateCompleted)
	assertProcessInstanceVariables(t, parentKey, map[string]any{
		"testInputCollection": []any{"test1", "test2", "test3"},
		"variable_name":       "test-value",
	})
	assertProcessInstanceIncidentsLength(t, parentKey, 0)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 0, 1)
	assertProcessInstanceTokenElements(t, parentKey, []string{handledEndId}, []string{defaultEndId})
}

func assertMultiInstanceUnmatchedIncident(t testing.TB, parentKey int64, childKey int64, childElementId string, parentElementId string, handledEndId string, defaultEndId string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, childKey, childElementId, bpmnruntime.TokenStateWaiting)
	assertProcessInstanceIncidentsLength(t, childKey, 1)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertProcessInstanceTokenElements(t, childKey, []string{childElementId}, []string{handledEndId, defaultEndId})

	assertProcessInstanceTokenState(t, parentKey, parentElementId, bpmnruntime.TokenStateWaiting)
	assertProcessInstanceVariables(t, parentKey, map[string]any{
		"testInputCollection": []any{"test1", "test2", "test3"},
		"variable_name":       "test-value",
	})
	assertProcessInstanceIncidentsLength(t, parentKey, 0)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 1, 0)
	assertProcessInstanceTokenElements(t, parentKey, []string{parentElementId}, []string{handledEndId, defaultEndId})
}

func waitForChildProcessInstanceByType(t testing.TB, parentProcessInstanceKey int64, processType zenclient.ProcessInstanceProcessType) zenclient.ProcessInstancesSimple {
	t.Helper()

	var child zenclient.ProcessInstancesSimple
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) == 0 {
			return false
		}
		for _, item := range page.Partitions[0].Items {
			if item.ProcessType == processType {
				child = item
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a %s child process instance", parentProcessInstanceKey, processType)
	return child
}
