package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestSequentialMultiInstanceMessageBoundaryFlow(t *testing.T) {
	t.Run("Interrupting boundary message terminates the active sequential iteration and completes the boundary path", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployInterruptingMessageBoundaryDefinition(
			t,
			"testdata/multi_instance/sequential_multi_instance_service_task_with_message_boundary_event_without_output_mapping.bpmn",
			"sequential-multi-instance-message-boundary-event-without-output-mapping",
		)
		instance, multiInstanceProcess := createMultiInstanceMessageBoundaryVariablesProcessInstance(t, definitionKey, multiInstanceMessageBoundaryCreateVariables())

		waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, multiInstanceProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task"})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		waitForProcessInstanceJobByElementId(t, multiInstanceProcess.Key, "service_task", public.JobStateTerminated)
		assertProcessInstanceHasNoActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateCanceled)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary")
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task"})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})
	})

	t.Run("Non-interrupting boundary message keeps sequential iterations active and completes both paths", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/multi_instance/sequential_multi_instance_service_task_with_message_boundary_event_without_output_mapping.bpmn",
			"sequential-multi-instance-message-boundary-event-without-output-mapping",
		)
		instance, multiInstanceProcess := createMultiInstanceMessageBoundaryVariablesProcessInstance(t, definitionKey, multiInstanceMessageBoundaryCreateVariables())

		waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, multiInstanceProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{"service_task"})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateActive, multiInstanceProcess.Key, zenclient.ProcessInstanceStateActive)
		waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
		assertProcessInstanceTokenState(t, instance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})

		for i := range 3 {
			completeJobForElementId(t, multiInstanceProcess.Key, "service_task", map[string]any{"testJobOutput": fmt.Sprintf("output-%d", i)})
		}

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "service_task", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"service_task",
			"service_task",
			"service_task",
			"service_task",
		})
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
			"flow_to_main_end",
			"end_event_main",
		})
	})
}
