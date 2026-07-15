package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskMessageBoundaryVariables(t *testing.T) {
	t.Run("Message boundary without output mapping propagates all message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/service_task/service_task_with_message_boundary_event_without_output_mapping.bpmn",
			"message-boundary-event-without-output-mapping",
		)
		processVariables := map[string]any{
			"existing":    "process-value",
			"overwritten": "initial-value",
		}
		instance := createMessageBoundaryVariablesInstance(t, definitionKey, processVariables)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)

		messageVariables := map[string]any{
			"overwritten": "message-value",
			"unmapped":    "unmapped-value",
			"numeric":     float64(42),
		}
		err := publishMessage(t, messageName, correlationKey, &messageVariables)
		require.NoError(t, err)

		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertFlowElementOutputVariables(t, instance.Key, "boundary_message_event", messageVariables)
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(processVariables, messageVariables))

		completeJobForElementId(t, instance.Key, "service_task", nil)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
	})

	t.Run("Message boundary with output mapping propagates only mapped message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/service_task/service_task_with_message_boundary_event_noninterrupting.bpmn",
			"message-boundary-event-noninterrupting",
		)
		processVariables := map[string]any{"existing": "process-value"}
		instance := createMessageBoundaryVariablesInstance(t, definitionKey, processVariables)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service_task")
		assertMessageSubscriptionState(t, instance.Key, "service_task", zenclient.EventSubscriptionStateActive)

		messageVariables := map[string]any{
			"payload":  "mapped-value",
			"unmapped": "ignored-value",
			"numeric":  float64(42),
		}
		err := publishMessage(t, messageName, correlationKey, &messageVariables)
		require.NoError(t, err)

		expectedMappedVariables := map[string]any{"payload": "mapped-value"}
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertFlowElementOutputVariables(t, instance.Key, "boundary_message_event", expectedMappedVariables)
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(processVariables, expectedMappedVariables))

		completeJobForElementId(t, instance.Key, "service_task", nil)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
	})
}

func createMessageBoundaryVariablesInstance(t testing.TB, definitionKey int64, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	processInstance, err := createProcessInstance(t, &definitionKey, variables)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	return processInstance
}
