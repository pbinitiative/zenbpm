package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

var (
	callActivityMessageBoundaryParentHistoryBeforeMessage = []string{
		"start_event",
		"flow_to_call_activity",
		"call_activity",
	}
	callActivityMessageBoundaryParentHistoryAfterMessage = []string{
		"start_event",
		"flow_to_call_activity",
		"call_activity",
		"boundary_message_event",
		"flow_to_boundary_end",
		"end_event_boundary",
	}
	callActivityMessageBoundaryChildHistoryBeforeCompletion = []string{
		"StartEvent_1",
		"Flow_0xt1d7q",
		"id",
	}
)

func TestCallActivityMessageBoundaryFlow(t *testing.T) {
	t.Run("Interrupting boundary message terminates the child and completes the boundary path", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployInterruptingCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_without_output_mapping.bpmn",
			"call-activity-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)
		childInstance := waitForChildProcessInstance(t, instance.Key, 0)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "call_activity", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, callActivityMessageBoundaryParentHistoryBeforeMessage)
		assertExactProcessInstanceHistory(t, childInstance.Key, callActivityMessageBoundaryChildHistoryBeforeCompletion)

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		waitForProcessInstanceJobByElementId(t, childInstance.Key, "id", public.JobStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", runtime.TokenStateCanceled)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary")
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, childInstance.Key, callActivityMessageBoundaryChildHistoryBeforeCompletion)
		assertExactProcessInstanceHistory(t, instance.Key, callActivityMessageBoundaryParentHistoryAfterMessage)
	})

	t.Run("Non-interrupting boundary message keeps the child active and completes both paths", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_without_output_mapping.bpmn",
			"call-activity-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)
		childInstance := waitForChildProcessInstance(t, instance.Key, 0)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "call_activity", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, callActivityMessageBoundaryParentHistoryBeforeMessage)
		assertExactProcessInstanceHistory(t, childInstance.Key, callActivityMessageBoundaryChildHistoryBeforeCompletion)

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateActive)
		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertProcessInstanceTokenState(t, instance.Key, "call_activity", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive, 1)
		assertExactProcessInstanceHistory(t, instance.Key, callActivityMessageBoundaryParentHistoryAfterMessage)

		completeJobForElementId(t, childInstance.Key, "id", map[string]any{"variable_name": "child-value"})

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, childInstance.Key, []string{
			"StartEvent_1",
			"Flow_0xt1d7q",
			"id",
			"Flow_1vz4oo2",
			"End_Event",
		})
		assertExactProcessInstanceHistory(t, instance.Key, append(append([]string{}, callActivityMessageBoundaryParentHistoryAfterMessage...), "flow_to_main_end", "end_event_main"))
	})
}
