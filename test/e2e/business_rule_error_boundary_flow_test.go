package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

var (
	businessRuleErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"boundary-error-business-rule-external",
	}
	businessRuleErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"boundary-error-business-rule-external",
		"boundary-error-main-task",
		"Flow_boundary_handled",
		"handled-end",
	}
)

func TestBusinessRuleErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/business_rule/business_rule_task_external_with_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("42"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Catch-all error boundary catches any code and completes handled path", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/business_rule/business_rule_task_external_with_catch_all_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("any-error"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Non-matching error boundary keeps activity waiting and creates incident", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/business_rule/business_rule_task_external_with_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("99"), nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact handler instead of catch-all path", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/business_rule/business_rule_task_external_with_error_boundary_and_catch_all.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "boundary-error-business-rule-external")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "boundary-error-business-rule-external", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 2, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("42"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 2)
		assertExactProcessInstanceHistory(t, processInstance.Key, businessRuleErrorBoundaryHistoryAfterHandledFailure)
	})
}
