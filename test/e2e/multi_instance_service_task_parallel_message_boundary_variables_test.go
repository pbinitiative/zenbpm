package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestParallelMultiInstanceMessageBoundaryVariables(t *testing.T) {
	t.Run("Message boundary without output mapping propagates all message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/multi_instance/parallel_multi_instance_service_task_with_message_boundary_event_without_output_mapping.bpmn",
			"parallel-multi-instance-message-boundary-event-without-output-mapping",
		)
		processVariables := multiInstanceMessageBoundaryCreateVariables()
		instance, multiInstanceProcess := createMultiInstanceMessageBoundaryVariablesProcessInstance(t, definitionKey, processVariables)

		waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 3)
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
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(multiInstanceMessageBoundaryExpectedBaseVariables(), messageVariables))
	})

	t.Run("Message boundary with output mapping propagates only mapped message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/multi_instance/parallel_multi_instance_service_task_with_message_boundary_event_with_output_mapping.bpmn",
			"parallel-multi-instance-message-boundary-event-with-output-mapping",
		)
		processVariables := multiInstanceMessageBoundaryCreateVariables()
		instance, multiInstanceProcess := createMultiInstanceMessageBoundaryVariablesProcessInstance(t, definitionKey, processVariables)

		waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 3)
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
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(multiInstanceMessageBoundaryExpectedBaseVariables(), expectedMappedVariables))
	})
}

func createMultiInstanceMessageBoundaryVariablesProcessInstance(t testing.TB, definitionKey int64, variables map[string]any) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	processInstance := createProcessInstanceWithVariables(t, definitionKey, variables)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
	return processInstance, multiInstanceProcess
}

func multiInstanceMessageBoundaryCreateVariables() map[string]any {
	return map[string]any{
		"existing":            "process-value",
		"overwritten":         "initial-value",
		"testInputCollection": []string{"test1", "test2", "test3"},
	}
}

func multiInstanceMessageBoundaryExpectedBaseVariables() map[string]any {
	return map[string]any{
		"existing":            "process-value",
		"overwritten":         "initial-value",
		"testInputCollection": []any{"test1", "test2", "test3"},
	}
}
