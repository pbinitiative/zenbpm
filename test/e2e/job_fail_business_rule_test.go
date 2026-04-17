package e2e

import (
	"testing"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

func TestGrpcJobFailOnBusinessRule(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_catch_all_error_boundary_event.bpmn", "business-rule-task-external-catch-all-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule catch by error code", ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailActiveJobViaGrpc(t, job, "grpc business rule incident", ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnBusinessRule(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_catch_all_error_boundary_event.bpmn", "business-rule-task-external-catch-all-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/business_rule_task/business_rule_task_external_with_error_boundary_event.bpmn", "business-rule-task-external-error-boundary")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}
