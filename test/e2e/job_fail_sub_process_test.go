package e2e

import (
	"testing"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestGrpcJobFailOnSubProcess(t *testing.T) {
	t.Run("catch_all_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_catch_all.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_and_output_mapping.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process matched error with output mapping", ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process matched error", ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process unmatched error", ptr.To("99"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "subprocess_1", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"subprocess_1"}, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnSubProcess(t *testing.T) {
	t.Run("catch_all_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_catch_all.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_and_output_mapping.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "subprocess_1", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"subprocess_1"}, []string{"handled-end", "should-not-happen-end"})
	})

	t.Run("nested_matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn", "nested_subprocess_with_error_boundery_event")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChildProcess := waitForChildProcessInstance(t, processInstance.Key)
		secondChildProcess := waitForChildProcessInstance(t, firstChildProcess.Key)
		serviceTaskProcess := waitForChildProcessInstance(t, secondChildProcess.Key)

		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 1, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)

		job := waitForProcessInstanceJobByElementId(t, serviceTaskProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, serviceTaskProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, serviceTaskProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, serviceTaskProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, serviceTaskProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)
		assertProcessInstanceTokenElements(t, serviceTaskProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})

		waitForProcessInstanceState(t, secondChildProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceVariables(t, secondChildProcess.Key, map[string]interface{}{"variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, secondChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)

		waitForProcessInstanceState(t, firstChildProcess.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, firstChildProcess.Key, map[string]interface{}{"variable_from_request": "request_variable", "variable_name": "test-value"}) // parent process vars are propagated to a child process
		assertProcessInstanceIncidentsLength(t, firstChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 0, 1)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "complete", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"complete"}, []string{})
	})
}

func deployUniqueSubProcessDefinitionKey(t *testing.T, filename string) int64 {
	t.Helper()

	definition, err := deployGetUniqueDefinition(t, filename)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}
