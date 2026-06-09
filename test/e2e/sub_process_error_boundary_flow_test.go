package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

var (
	subProcessParentErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"subprocess",
	}
	subProcessChildErrorBoundaryHistoryBeforeFailure = []string{
		"sub_process_start",
		"Flow_1jotui9",
		"service_task",
	}
	subProcessParentExactErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"subprocess",
		"Flow_0xorbu0",
		"exact_match_end",
	}
	subProcessChildExactErrorBoundaryHistoryAfterHandledFailure = []string{
		"sub_process_start",
		"Flow_1jotui9",
		"service_task",
		"boundary_error_exact_match",
	}
	subProcessParentCatchAllErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_start_main",
		"subprocess",
		"Flow_boundary_handled",
		"catch_all_end",
	}
	subProcessChildCatchAllErrorBoundaryHistoryAfterHandledFailure = []string{
		"sub_process_start",
		"Flow_1jotui9",
		"service_task",
		"boundary_error_catch_all",
	}
)

func TestSubProcessErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {
		processInstance, innerProcess := createSubProcessErrorBoundaryFlowInstance(t, "testdata/sub_process/subprocess_with_error_boundary_event.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		assertSubProcessErrorBoundaryWaitingFlow(t, processInstance.Key, innerProcess.Key, 1, subProcessParentErrorBoundaryHistoryBeforeFailure, subProcessChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("31"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertExactProcessInstanceHistory(t, innerProcess.Key, subProcessChildExactErrorBoundaryHistoryAfterHandledFailure)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "exact_match_end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, subProcessParentExactErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Catch-all error boundary catches any code and completes handled parent path", func(t *testing.T) {
		processInstance, innerProcess := createSubProcessErrorBoundaryFlowInstance(t, "testdata/sub_process/subprocess_with_error_boundary_event_catch_all.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		assertSubProcessErrorBoundaryWaitingFlow(t, processInstance.Key, innerProcess.Key, 1, subProcessParentErrorBoundaryHistoryBeforeFailure, subProcessChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("any-error"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertExactProcessInstanceHistory(t, innerProcess.Key, subProcessChildCatchAllErrorBoundaryHistoryAfterHandledFailure)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "catch_all_end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, subProcessParentCatchAllErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Non-matching error boundary keeps parent and child active and creates child incident", func(t *testing.T) {
		processInstance, innerProcess := createSubProcessErrorBoundaryFlowInstance(t, "testdata/sub_process/subprocess_with_error_boundary_event.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		assertSubProcessErrorBoundaryWaitingFlow(t, processInstance.Key, innerProcess.Key, 1, subProcessParentErrorBoundaryHistoryBeforeFailure, subProcessChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("99"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateActive, innerProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertExactProcessInstanceHistory(t, innerProcess.Key, subProcessChildErrorBoundaryHistoryBeforeFailure)

		assertProcessInstanceTokenState(t, processInstance.Key, "subprocess", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, subProcessParentErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact parent handler instead of catch-all path", func(t *testing.T) {
		processInstance, innerProcess := createSubProcessErrorBoundaryFlowInstance(t, "testdata/sub_process/subprocess_with_error_boundary_and_catch_all.bpmn")

		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		assertSubProcessErrorBoundaryWaitingFlow(t, processInstance.Key, innerProcess.Key, 2, subProcessParentErrorBoundaryHistoryBeforeFailure, subProcessChildErrorBoundaryHistoryBeforeFailure)

		failJob(t, job.Key, new("31"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertExactProcessInstanceHistory(t, innerProcess.Key, subProcessChildExactErrorBoundaryHistoryAfterHandledFailure)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "exact_match_end")
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 2)
		assertExactProcessInstanceHistory(t, processInstance.Key, subProcessParentExactErrorBoundaryHistoryAfterHandledFailure)
	})

	t.Run("Nested matching error boundary completes nearest matching subprocess", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn", "nested_subprocess_with_error_boundery_event")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChildProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		secondChildProcess := waitForChildProcessInstance(t, firstChildProcess.Key, 0)
		serviceTaskProcess := waitForChildProcessInstance(t, secondChildProcess.Key, 0)

		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 1, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, serviceTaskProcess.Key, "service_task")
		failJob(t, job.Key, new("54"), map[string]any{"variable_from_request": "request_variable"})

		waitForProcessInstanceState(t, serviceTaskProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, serviceTaskProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceVariables(t, serviceTaskProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, serviceTaskProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)
		assertProcessInstanceHistory(t, serviceTaskProcess.Key, []string{"Flow_0lr9l93", "Event_0vhkfv0", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, secondChildProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceVariables(t, secondChildProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, secondChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)
		assertProcessInstanceHistory(t, secondChildProcess.Key, []string{"Flow_0921sm2", "start_subprocess_0", "subprocess_0"})

		waitForProcessInstanceState(t, firstChildProcess.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, firstChildProcess.Key, map[string]interface{}{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, firstChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 0, 1)
		assertProcessInstanceHistory(t, firstChildProcess.Key, []string{"Flow_1ijsfip", "start_subprocess_1", "subprocess_1", "Flow_1nd1fpn", "Event_09haznn"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "complete", runtime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_07q4pdb", "start_subprocess_2", "subprocess_2", "Flow_17ropai", "complete"})
	})
}

func createSubProcessErrorBoundaryFlowInstance(t *testing.T, filename string) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	definitionKey := deployTestDataProcessDefinitionKey(t, filename)
	processInstance := createProcessInstanceWithVariables(t, definitionKey, nil)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
	return processInstance, innerProcess
}

func assertSubProcessErrorBoundaryWaitingFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int, parentHistory []string, childHistory []string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, parentKey, "subprocess", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, subscriptionCount, 0)
	assertExactProcessInstanceHistory(t, parentKey, parentHistory)

	assertProcessInstanceTokenState(t, childKey, "service_task", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, childHistory)
}
