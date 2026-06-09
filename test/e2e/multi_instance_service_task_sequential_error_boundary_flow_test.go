package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

var (
	sequentialMultiInstanceParentErrorBoundaryHistoryBeforeFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"service_task",
	}
	sequentialMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"service_task",
		"Flow_104hlf4",
		"error_boundary_event_end",
	}
	sequentialMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure = []string{
		"service_task",
		"Event_15f0ox7",
	}
)

func TestSequentialMultiInstanceErrorBoundaryFlow(t *testing.T) {
	t.Run("Matching error boundary moves activity token to handled path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createSequentialMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/sequential_multi_instance_service_task_with_error_boundary_event.bpmn", "sequential-multi-instance-flow", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertSequentialMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, sequentialMultiInstanceParentErrorBoundaryHistoryBeforeFailure, []string{"service_task"})

		failJob(t, job.Key, new("44"), nil)

		assertSequentialMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, sequentialMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure, sequentialMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure, "error_boundary_event_end")
	})

	t.Run("Catch-all error boundary catches any code and completes handled parent path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createSequentialMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/sequential_multi_instance_service_task_with_catch_all_error_boundary_event.bpmn", "sequential-multi-instance-flow-catch-all", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertSequentialMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, sequentialMultiInstanceParentErrorBoundaryHistoryBeforeFailure, []string{"service_task"})

		failJob(t, job.Key, new("any-error"), nil)

		assertSequentialMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, sequentialMultiInstanceParentErrorBoundaryHistoryAfterHandledFailure, sequentialMultiInstanceChildErrorBoundaryHistoryAfterHandledFailure, "error_boundary_event_end")
	})

	t.Run("Non-matching error boundary keeps parent and child active and creates child incident", func(t *testing.T) {
		processInstance, multiInstanceProcess := createSequentialMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/sequential_multi_instance_service_task_with_error_boundary_event.bpmn", "sequential-multi-instance-flow-unmatched", 1)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertSequentialMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 1, sequentialMultiInstanceParentErrorBoundaryHistoryBeforeFailure, []string{"service_task"})

		failJob(t, job.Key, new("99"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateActive, multiInstanceProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, multiInstanceProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task"})

		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertExactProcessInstanceHistory(t, processInstance.Key, sequentialMultiInstanceParentErrorBoundaryHistoryBeforeFailure)
	})

	t.Run("Exact-match error boundary completes exact parent handler instead of catch-all path", func(t *testing.T) {
		processInstance, multiInstanceProcess := createSequentialMultiInstanceErrorBoundaryFlowInstance(t, "testdata/multi_instance/sequential_multi_instance_service_task_with_error_boundary_and_catch_all.bpmn", "sequential-multi-instance-flow-priority", 2)
		parentBefore := []string{"StartEvent_1", "Flow_start_main", "service_task"}
		parentAfter := []string{"StartEvent_1", "Flow_start_main", "service_task", "Flow_boundary_handled", "handled-end"}
		childAfter := []string{"service_task", "boundary-error-main-task"}

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertSequentialMultiInstanceErrorBoundaryWaitingFlow(t, processInstance.Key, multiInstanceProcess.Key, 2, parentBefore, []string{"service_task"})

		failJob(t, job.Key, new("44"), nil)

		assertSequentialMultiInstanceErrorBoundaryHandledFlow(t, processInstance.Key, multiInstanceProcess.Key, 2, parentAfter, childAfter, "handled-end")
	})

	t.Run("Nested matching error boundary catches sequential multi-instance child error", func(t *testing.T) {
		jobType := fmt.Sprintf("service-task-sequential-rest-catch-by-code-%d", time.Now().UnixNano())
		definition, err := deployDefinitionWithJobType(t, "error_events/multi_instance/sequential_multi_instance_in_subprocess_with_error_boundary_event.bpmn", jobType, map[string]string{
			"TestType": jobType,
		})
		require.NoError(t, err)

		processInstance := createProcessInstanceWithVariables(t, definition.ProcessDefinitionKey, map[string]any{
			"variable_name":       "test-value",
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 0)

		subProcessInstance := waitForChildProcessInstanceByType(t, processInstance.Key, zenclient.ProcessInstanceProcessTypeSubprocess)
		multiInstanceProcess := waitForChildProcessInstanceByType(t, subProcessInstance.Key, zenclient.ProcessInstanceProcessTypeMultiInstance)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		failJob(t, job.Key, new("44"), map[string]any{"variable_from_request": "request_variable"})

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, multiInstanceProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)
		assertProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task", "Event_15f0ox7"})

		assertProcessInstanceTokenState(t, subProcessInstance.Key, "error_boundary_event_end", runtime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, subProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, subProcessInstance.Key, 0, 1)
		assertProcessInstanceHistory(t, subProcessInstance.Key, []string{"Flow_0xt1d7q", "StartEvent_1", "service_task", "Flow_104hlf4", "error_boundary_event_end"})

		assertProcessInstanceTokenState(t, processInstance.Key, "Event_11axlot", runtime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"testInputCollection": []any{"test1", "test2", "test3"},
			"variable_name":       "test-value",
		})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_1llmj1k", "Event_0m9r0ve", "Activity_11wye3s", "Flow_0n7mtnz", "Event_11axlot"})
	})
}

func createSequentialMultiInstanceErrorBoundaryFlowInstance(t *testing.T, filename string, processId string, subscriptionCount int) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	definitionKey := deploySequentialMultiInstanceErrorBoundaryVariablesDefinition(t, filename, processId)
	processInstance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"testInputCollection": []string{"test1", "test2"}})
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, subscriptionCount, 0)

	multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, multiInstanceProcess.Key)

	return processInstance, multiInstanceProcess
}

func assertSequentialMultiInstanceErrorBoundaryWaitingFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int, parentHistory []string, childHistory []string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateActive, childKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, parentKey, "service_task", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, subscriptionCount, 0)
	assertExactProcessInstanceHistory(t, parentKey, parentHistory)

	assertProcessInstanceTokenState(t, childKey, "service_task", runtime.TokenStateWaiting)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, childHistory)
}

func assertSequentialMultiInstanceErrorBoundaryHandledFlow(t testing.TB, parentKey int64, childKey int64, subscriptionCount int, parentHistory []string, childHistory []string, handledEndID string) {
	t.Helper()

	waitForTwoProcessInstanceStates(t, parentKey, zenclient.ProcessInstanceStateCompleted, childKey, zenclient.ProcessInstanceStateTerminated)
	assertProcessInstanceTokenState(t, childKey, "service_task", runtime.TokenStateCanceled)
	assertProcessInstanceErrorSubscriptionsCountIsZero(t, childKey)
	assertExactProcessInstanceHistory(t, childKey, childHistory)

	assertProcessInstanceIsCompleted(t, parentKey, handledEndID)
	assertProcessInstanceErrorSubscriptionCount(t, parentKey, 0, subscriptionCount)
	assertExactProcessInstanceHistory(t, parentKey, parentHistory)
}
