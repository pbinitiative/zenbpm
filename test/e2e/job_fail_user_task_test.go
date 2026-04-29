package e2e

import (
	"testing"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestGrpcJobFailOnUserTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "error_events/user_task/user_task_with_error_boundary_event_catch_all.bpmn")
		require.NoError(t, err)
		require.NotZero(t, definition.Key)

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definition.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")
		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc user task incident", ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "user-task-error-boundary", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnUserTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "error_events/user_task/user_task_with_error_boundary_event_catch_all.bpmn")
		require.NoError(t, err)
		require.NotZero(t, definition.Key)

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definition.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn", "user-task-with-error-boundary")

		processInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "user-task-error-boundary", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}
