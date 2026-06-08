package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

var (
	userTaskErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"user-task-error-boundary",
	}
	userTaskErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"user-task-error-boundary",
		"boundary-error-main-task",
		"Flow_boundary_handled",
		"handled-end",
	}
)

func TestUserTaskErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_with_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, userTaskErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, ptr.To("42"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, userTaskErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Catch-all error boundary catches any code and completes handled path", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_with_catch_all_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"user_task",
		})

		failJob(t, job.Key, ptr.To("any-error"), nil)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", runtime.TokenStateCompleted)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"user_task",
			"boundary-error-main-task",
			"Flow_boundary_handled",
			"handled-end",
		})
	})

	t.Run("Non-matching error boundary keeps the token on job and token with activity in active state", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_with_error_boundary_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user-task-error-boundary")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, userTaskErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, ptr.To("56"), nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user-task-error-boundary", runtime.TokenStateWaiting)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"user-task-error-boundary",
		})
	})
}
