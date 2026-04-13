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
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch all", ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
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
	})
}

func TestRestJobFailOnParallelMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-rest-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_parallel_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
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
	})
}

func TestGrpcJobFailOnSequentialMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task catch all", ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sequential service task catch by error code", ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-grpc-incident-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
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
	})
}

func TestRestJobFailOnSequentialMultiInstance(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-catch-all-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_catch_all_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("44"))

		assertMultiInstanceBoundaryHandled(
			t,
			processInstance.Key,
			multiInstanceProcess.Key,
			"service_task",
			"error_boundary_event_end",
			"end",
		)
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-incident-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/multi_instance_sequential_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance, multiInstanceProcess := createMultiInstanceProcessInstance(t, definition.ProcessDefinitionKey)

		job := waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task")
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
	})
}

func createMultiInstanceProcessInstance(t testing.TB, definitionKey int64) (zenclient.ProcessInstance, zenclient.ProcessInstance) {
	t.Helper()

	processInstance := createErrorBoundaryProcessInstanceWithVariables(t, definitionKey, map[string]any{
		"variable_name":       "test-value",
		"testInputCollection": []string{"test1", "test2", "test3"},
	})
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

	multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key)
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

func assertMultiInstanceUnmatchedIncident(t testing.TB, parentKey int64, childKey int64, childElementId string, parentElementId string, handledEndId string, defaultEndId string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceTokenState(t, childKey, childElementId, bpmnruntime.TokenStateFailed)
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
