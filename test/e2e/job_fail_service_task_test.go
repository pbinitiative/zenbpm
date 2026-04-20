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

func TestGrpcJobFailOnServiceTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_catch_all_error_boundary_event.bpmn", fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()), map[string]string{
			"TestType": fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()),
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-grpc-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailActiveJobViaGrpc(t, job, "grpc service task incident", ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "service-task-error-boundary", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnServiceTask(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_catch_all_error_boundary_event.bpmn", fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()), map[string]string{
			"TestType": fmt.Sprintf("service-task-catch-all-%d", time.Now().UnixNano()),
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To(""))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		jobType := fmt.Sprintf("error-boundary-rest-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/service_task/service_task_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithDefaultVariables(t, definition.ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "service-task-error-boundary", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}
