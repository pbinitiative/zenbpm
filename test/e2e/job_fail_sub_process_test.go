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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]any{})
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

	//TODO: Uncomment this once issue #501  https://github.com/pbinitiative/zenbpm/issues/501 is fixed.
	//t.Run("nested_matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
	//	definitionKey := deployProcessDefinitionKey(t, "error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn", "nested_subprocess_with_error_boundery_event")
	//
	//	processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
	//	t.Cleanup(func() {
	//		cleanupOwnedProcessInstance(t, processInstance.Key)
	//	})
	//
	//	firstChildProcess := waitForChildProcessInstance(t, processInstance.Key)
	//	secondChildProcess := waitForChildProcessInstance(t, firstChildProcess.Key)
	//	thirdChildProcess := waitForChildProcessInstance(t, secondChildProcess.Key)
	//
	//	job := waitForProcessInstanceJobByElementId(t, thirdChildProcess.Key, "service_task")
	//	callFailJobViaRest(t, job.Key, ptr.To("54"))
	//
	//	waitForProcessInstanceState(t, thirdChildProcess.Key, zenclient.ProcessInstanceStateTerminated)
	//	assertProcessInstanceTokenState(t, thirdChildProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
	//	assertProcessInstanceVariables(t, thirdChildProcess.Key, map[string]any{})
	//	assertProcessInstanceIncidentsLength(t, thirdChildProcess.Key, 0)
	//	assertProcessInstanceErrorSubscriptionsCountIsZero(t, thirdChildProcess.Key)
	//	assertProcessInstanceTokenElements(t, thirdChildProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
	//
	//	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	//	assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
	//	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
	//	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	//	assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
	//	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	//})
}

func deployUniqueSubProcessDefinitionKey(t *testing.T, filename string) int64 {
	t.Helper()

	definition, err := deployGetUniqueDefinition(t, filename)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}
