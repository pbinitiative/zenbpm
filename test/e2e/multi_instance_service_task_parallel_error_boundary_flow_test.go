package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

const parallelMultiInstanceServiceTaskCount = 3

var (
	parallelMultiInstanceParentErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"service_task",
	}
	parallelMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"service_task",
		"Flow_104hlf4",
		"error_boundary_event_end",
	}
	parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure = []string{
		"service_task",
		"service_task",
		"service_task",
	}
	parallelMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure = []string{
		"service_task",
		"service_task",
		"service_task",
		"Event_15f0ox7",
	}
)

func TestParallelMultiInstanceErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createParallelMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_error_boundary_event.bpmn", "parallel-multi-instance-flow", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertParallelMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, parallelMultiInstanceParentErrorBoundaryHistoryBeforeFailure, parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("44"), nil)

		assertParallelMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, parallelMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure, parallelMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure, "error_boundary_event_end")
	})

	t.Run("Catch-all error boundary catches any code and completes handled parent path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createParallelMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_catch_all_error_boundary_event.bpmn", "parallel-multi-instance-flow-catch-all", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertParallelMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, parallelMultiInstanceParentErrorBoundaryHistoryBeforeFailure, parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("any-error"), nil)

		assertParallelMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, parallelMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure, parallelMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure, "error_boundary_event_end")
	})

	t.Run("Non-matching error boundary keeps parent and child active and creates child incident", func(t *testing.T) {
		processInstance, multiInstanceProcess := createParallelMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_error_boundary_event.bpmn", "parallel-multi-instance-flow-unmatched", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertParallelMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, parallelMultiInstanceParentErrorBoundaryHistoryBeforeFailure, parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("99"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateActive, multiInstanceProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenStates(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting, parallelMultiInstanceServiceTaskCount)
		assertProcessInstanceIncidentsLength(t, multiInstanceProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure)

		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, parallelMultiInstanceParentErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact parent handler instead of catch-all path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createParallelMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_error_boundary_and_catch_all.bpmn", "parallel-multi-instance-flow-priority", 2)
		parentBefore := []string{"StartEvent_1", "Flow_start_main", "service_task"}
		parentAfter := []string{"StartEvent_1", "Flow_start_main", "service_task", "Flow_boundary_handled", "handled-end"}
		childAfter := []string{"service_task", "service_task", "service_task", "boundary-error-main-task"}

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertParallelMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 2, parentBefore, parallelMultiInstanceChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("44"), nil)

		assertParallelMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 2, parentAfter, childAfter, "handled-end")
	})
}

func createParallelMultiInstanceErrorBoundaryFlowInstance(t *testing.T, filename string, processId string, subscriptionCount int) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	definitionKey := deployParallelMultiInstanceErrorBoundaryVariablesDefinition(t, filename, processId)
	processInstance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"testInputCollection": []string{"test1", "test2", "test3"}})
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, subscriptionCount, 0)

	multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)

	return processInstance, multiInstanceProcess
}

func assertParallelMultiInstanceErrorBoundaryWaitingFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int, parentHistory []string, childHistory []string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, parentKey, "service_task", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, subscriptionCount, 0)
	assertExactProcessInstanceHistory(t, parentKey, parentHistory)

	assertProcessInstanceTokenStates(t, childKey, "service_task", runtime.TokenStateWaiting, parallelMultiInstanceServiceTaskCount)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, childHistory)
}

func assertParallelMultiInstanceErrorBoundaryHandledFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int, parentHistory []string, childHistory []string, handledEndID string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateCompleted, childKey, zenclient.ProcessInstanceStateTerminated)
	assertProcessInstanceTokenStates(t, childKey, "service_task", runtime.TokenStateCanceled, parallelMultiInstanceServiceTaskCount)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, childHistory)

	assertProcessInstanceIsCompleted(t, parentKey, handledEndID)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 0, subscriptionCount)
	assertExactProcessInstanceHistory(t, parentKey, parentHistory)
}
