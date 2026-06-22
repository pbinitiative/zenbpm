package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestEventBasedGatewayVariables(t *testing.T) {
	t.Run("Message variables are propagated when no output mapping is defined", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayInstance(t, "PT1H")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"winner":          "message",
			"unmapped_value":  "propagated-without-mapping",
			"numeric_payload": float64(42),
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		expectedVariables := map[string]any{
			"correlationKey":  correlationKey,
			"winner":          "message",
			"unmapped_value":  "propagated-without-mapping",
			"numeric_payload": float64(42),
		}
		assertFlowElementInputVariables(t, processInstance.Key, eventBasedGatewayMessageTaskId, expectedVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
	})

	t.Run("Message output mapping propagates only mapped variables", func(t *testing.T) {
		definitionKey := deployEventBasedGatewayMappingDefinition(t)
		correlationKey := eventBasedGatewayCorrelationKey(definitionKey)
		processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{
			"correlationKey": correlationKey,
			"process_seed":   "seed-value",
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		err = publishMessage(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"event_payload":   "payload-value",
			"ignored_payload": "ignored-value",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		expectedVariables := map[string]any{
			"correlationKey":         correlationKey,
			"process_seed":           "seed-value",
			"mapped_message_payload": "payload-value",
			"mapped_process_seed":    "seed-value",
			"mapped_static_message":  "static-message",
		}
		assertFlowElementInputVariables(t, processInstance.Key, eventBasedGatewayMessageTaskId, expectedVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
	})

	t.Run("Late message variables are not propagated after timer wins", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayInstance(t, "PT1S")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId, public.JobStateActive)

		response, err := publishMessageWithResponse(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"late_message_payload": "should-not-propagate",
		})
		require.NoError(t, err)
		require.Equal(t, 404, response.StatusCode(), "late message should not match the cancelled gateway subscription")

		expectedVariables := map[string]any{
			"correlationKey": correlationKey,
		}
		assertFlowElementInputVariables(t, processInstance.Key, eventBasedGatewayTimerTaskId, expectedVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
	})
}

func deployEventBasedGatewayMappingDefinition(t testing.TB) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayMessageTimerPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-message-mapping-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-message-timer"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
		">PT1S<",
		">PT1H<",
		`<bpmn:intermediateCatchEvent id="catch_message">`,
		`<bpmn:intermediateCatchEvent id="catch_message">
      <bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:output source="=event_payload" target="mapped_message_payload" />
          <zenbpm:output source="=process_seed" target="mapped_process_seed" />
          <zenbpm:output source="=&#34;static-message&#34;" target="mapped_static_message" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>`,
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayMessageTimerPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}
