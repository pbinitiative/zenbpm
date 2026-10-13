package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

var (
	callActivityParentHistoryWhileWaiting = []string{
		"StartEvent_1",
		"Flow_start_call",
		"call_activity",
	}
	callActivityParentHistoryAfterCompletion = []string{
		"StartEvent_1",
		"Flow_start_call",
		"call_activity",
		"Flow_call_completed",
		"completed-end",
	}
	callActivityChildHistoryWhileWaiting = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"id",
	}
	callActivityChildHistoryAfterCompletion = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"id",
		"Flow_1vz4oo2",
		"End_Event",
	}
)

func TestCallActivityFlow(t *testing.T) {
	t.Run("Parent token waits at call activity and continues after child completes", func(t *testing.T) {
		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_input_output_mapping.bpmn", nil)

		require.Equal(t, zenclient.ProcessInstanceProcessTypeCallActivity, childInstance.ProcessType)
		require.NotNil(t, childInstance.ParentProcessInstanceKey)
		require.Equal(t, processInstance.Key, *childInstance.ParentProcessInstanceKey)

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "call_activity", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", runtime.TokenStateWaiting)
		assertProcessInstanceTokenCount(t, processInstance.Key, "completed-end", 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceHasNoActiveJobs(t, processInstance.Key)
		assertExactProcessInstanceHistory(t, processInstance.Key, callActivityParentHistoryWhileWaiting)
		assertExactProcessInstanceHistory(t, childInstance.Key, callActivityChildHistoryWhileWaiting)

		require.NoError(t, completeJob(t, job.Key, nil))

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		waitForProcessInstanceJobByElementId(t, childInstance.Key, "id", public.JobStateCompleted)
		assertProcessInstanceTokenState(t, childInstance.Key, "End_Event", runtime.TokenStateCompleted)
		assertExactCompletedProcessInstanceHistory(t, childInstance.Key, callActivityChildHistoryAfterCompletion)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "completed-end")
		assertProcessInstanceTokenCount(t, processInstance.Key, "handled-end", 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertExactCompletedProcessInstanceHistory(t, processInstance.Key, callActivityParentHistoryAfterCompletion)
	})

	t.Run("Child completion terminates the untriggered boundary message subscription", func(t *testing.T) {
		definitionKey, _, _ := deployCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_without_output_mapping.bpmn",
			"call-activity-message-boundary-event-without-output-mapping",
		)
		processInstance := createMessageBoundaryInstance(t, definitionKey)
		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)

		job := waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertProcessInstanceTokenState(t, processInstance.Key, "call_activity", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, processInstance.Key, "call_activity", zenclient.EventSubscriptionStateActive)

		require.NoError(t, completeJob(t, job.Key, nil))

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event_main")
		assertProcessInstanceTokenCount(t, processInstance.Key, "end_event_boundary", 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, "call_activity", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, "call_activity", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactCompletedProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"flow_to_call_activity",
			"call_activity",
			"flow_to_main_end",
			"end_event_main",
		})
	})
}
