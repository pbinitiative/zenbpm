package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestSendTaskMessageBoundaryFlow(t *testing.T) {
	t.Run("Interrupting boundary message cancels the send task and completes the boundary path", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployInterruptingMessageBoundaryDefinition(
			t,
			"testdata/send_task/send_task_with_message_boundary_event_without_output_mapping.bpmn",
			"send-task-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "send_task")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "send_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "send_task", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_send_task",
			"send_task",
		})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForProcessInstanceJobByElementId(t, instance.Key, "send_task", public.JobStateTerminated)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary")
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_send_task",
			"send_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})
	})

	t.Run("Non-interrupting boundary message keeps the send task active and completes both paths", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/send_task/send_task_with_message_boundary_event_without_output_mapping.bpmn",
			"send-task-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "send_task")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "send_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "send_task", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_send_task",
			"send_task",
		})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "send_task")
		assertProcessInstanceTokenState(t, instance.Key, "send_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateActive, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_send_task",
			"send_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})

		completeJobForElementId(t, instance.Key, "send_task", nil)

		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "send_task", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_send_task",
			"send_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
			"flow_to_main_end",
			"end_event_main",
		})
	})
}
