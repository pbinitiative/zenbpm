package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageCatchEventVariables(t *testing.T) {
	t.Run("Without output mapping all message variables are propagated", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/message_event/message-intermediate-catch-event.bpmn")
		createInstanceVariables := map[string]any{
			"existing":    "process-value",
			"overwritten": "initial-value",
		}
		processInstance, err := createProcessInstance(t, &definitionKey, createInstanceVariables)
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertMessageSubscriptionState(t, processInstance.Key, "id-1", zenclient.EventSubscriptionStateActive)
		assertFlowElementInputVariables(t, processInstance.Key, "id-1", createInstanceVariables)

		messageVariables := map[string]any{
			"overwritten": "message-value",
			"unmapped":    "unmapped-value",
			"numeric":     float64(42),
		}
		err = publishMessage(t, "globalMsgRef", "correlation-key-one", &messageVariables)
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "Event_1jn3jqk")
		assertFlowElementOutputVariables(t, processInstance.Key, "id-1", messageVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, messageVariables))
	})

	t.Run("Input mapping is local and output mapping propagates only mapped message variables", func(t *testing.T) {
		definition, err := deployGetDefinition(t, "simple-intermediate-message-catch-event.bpmn", "simple-intermediate-message-catch-event")
		require.NoError(t, err)

		createInstanceVariables := map[string]any{
			"inputFoo":  "input-value",
			"unchanged": "process-value",
		}
		processInstance, err := createProcessInstance(t, &definition.Key, createInstanceVariables)
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertMessageSubscriptionState(t, processInstance.Key, "msg", zenclient.EventSubscriptionStateActive)
		assertFlowElementInputVariables(t, processInstance.Key, "msg", mergeMaps(createInstanceVariables, map[string]any{
			"mappedInputFoo": "input-value",
		}))

		err = publishMessage(t, "msg", "key", &map[string]any{
			"foo":     "mapped-value",
			"ignored": "ignored-value",
		})
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "EndEvent_1")
		assertFlowElementOutputVariables(t, processInstance.Key, "msg", map[string]any{
			"mappedFoo": "mapped-value",
		})
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, map[string]any{
			"mappedFoo": "mapped-value",
		}))
	})
}
