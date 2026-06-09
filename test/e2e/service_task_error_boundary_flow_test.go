package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

var (
	serviceTaskErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"service-task-error-boundary",
	}
	serviceTaskErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"service-task-error-boundary",
		"boundary-error-main-task",
		"Flow_boundary_handled",
		"handled-end",
	}
)

func TestServiceTaskErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, serviceTaskErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("42"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, serviceTaskErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Catch-all error boundary catches any code and completes handled path", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_catch_all_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"service_task",
		})

		failJob(t, job.Key, new("any-error"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"service_task",
			"boundary-error-main-task",
			"Flow_boundary_handled",
			"handled-end",
		})
	})

	t.Run("Non-matching error boundary keeps activity waiting and creates incident", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service-task-error-boundary")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, serviceTaskErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("56"), nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, serviceTaskErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact parent handler instead of catch-all path", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_error_boundary_and_catch_all.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 2, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"service_task",
		})

		failJob(t, job.Key, new("31"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "exact_match_end")
		assertProcessInstanceTokenState(t, processInstance.Key, "exact_match_end", runtime.TokenStateCompleted)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 2)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"service_task",
			"boundary_error_exact_match",
			"Flow_0xorbu0",
			"exact_match_end",
		})
	})
}
