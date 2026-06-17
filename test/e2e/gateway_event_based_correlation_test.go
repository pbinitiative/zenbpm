package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const (
	eventBasedGatewayDelayedPath        = "testdata/gateway_event_based/event_based_gateway_after_service_task.bpmn"
	eventBasedGatewayDelayedMessageName = "event-gateway-delayed-message"
	eventBasedGatewayPreTaskId          = "pre_task"
)

func TestEventBasedGatewayCorrelation(t *testing.T) {
	t.Run("Wrong correlation key does not consume the gateway message subscription", func(t *testing.T) {
		processInstance, correlationKey := createEventBasedGatewayInstance(t, "PT1H")

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)

		response, err := publishMessageWithResponse(t, eventBasedGatewayMessageName, correlationKey+"-wrong", &map[string]any{
			"wrong_key_payload": "ignored",
		})
		require.NoError(t, err)
		require.Equal(t, 404, response.StatusCode())

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)

		err = publishMessage(t, eventBasedGatewayMessageName, correlationKey, &map[string]any{
			"winner": "message",
		})
		require.NoError(t, err)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
	})

	t.Run("Message is correlated to only the matching process instance", func(t *testing.T) {
		definitionKey := deployEventBasedGatewayDefinition(t, "PT1H")
		firstCorrelationKey := fmt.Sprintf("%s-first", eventBasedGatewayCorrelationKey(definitionKey))
		secondCorrelationKey := fmt.Sprintf("%s-second", eventBasedGatewayCorrelationKey(definitionKey))

		firstInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, firstCorrelationKey)
		secondInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, secondCorrelationKey)

		waitForEventBasedGatewayWaitingState(t, firstInstance.Key)
		waitForEventBasedGatewayWaitingState(t, secondInstance.Key)

		err := publishMessage(t, eventBasedGatewayMessageName, secondCorrelationKey, &map[string]any{
			"winner": "second",
		})
		require.NoError(t, err)

		waitForProcessInstanceActiveJobByElementId(t, secondInstance.Key, eventBasedGatewayMessageTaskId)
		assertMessageSubscriptionState(t, secondInstance.Key, eventBasedGatewayMessageElementId, zenclient.EventSubscriptionStateCompleted)
		assertTimerSubscriptionState(t, secondInstance.Key, eventBasedGatewayTimerElementId, zenclient.EventSubscriptionStateWithdrawn)

		waitForEventBasedGatewayWaitingState(t, firstInstance.Key)
		assertProcessInstanceHasNoActiveJobByElementId(t, firstInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, firstInstance.Key, eventBasedGatewayTimerTaskId)

		err = publishMessage(t, eventBasedGatewayMessageName, firstCorrelationKey, &map[string]any{
			"winner": "first",
		})
		require.NoError(t, err)
		waitForProcessInstanceActiveJobByElementId(t, firstInstance.Key, eventBasedGatewayMessageTaskId)
	})

	t.Run("Message published before the instance reaches the gateway is not buffered", func(t *testing.T) {
		definitionKey := deployDelayedEventBasedGatewayDefinition(t)
		correlationKey := fmt.Sprintf("delayed-event-gateway-%d", definitionKey)
		processInstance := createEventBasedGatewayInstanceForDefinition(t, definitionKey, correlationKey)

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayPreTaskId)

		response, err := publishMessageWithResponse(t, eventBasedGatewayDelayedMessageName, correlationKey, &map[string]any{
			"early_payload": "not-buffered",
		})
		require.NoError(t, err)
		require.Equal(t, 404, response.StatusCode())

		completeJobForElementId(t, processInstance.Key, eventBasedGatewayPreTaskId, nil)

		waitForEventBasedGatewayWaitingState(t, processInstance.Key)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, eventBasedGatewayTimerTaskId)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"correlationKey": correlationKey,
		})

		err = publishMessage(t, eventBasedGatewayDelayedMessageName, correlationKey, &map[string]any{
			"late_payload": "delivered",
		})
		require.NoError(t, err)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, eventBasedGatewayMessageTaskId)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"correlationKey": correlationKey,
			"late_payload":   "delivered",
		})
	})
}

func createEventBasedGatewayInstanceForDefinition(t testing.TB, definitionKey int64, correlationKey string) zenclient.ProcessInstance {
	t.Helper()

	processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{
		"correlationKey": correlationKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	return processInstance
}

func deployDelayedEventBasedGatewayDefinition(t testing.TB) int64 {
	t.Helper()

	file, err := readE2ETestDataBPMN(eventBasedGatewayDelayedPath)
	require.NoError(t, err)

	processID := fmt.Sprintf("event-based-gateway-after-service-task-%d", time.Now().UnixNano())
	content := strings.NewReplacer(
		`bpmn:process id="event-based-gateway-after-service-task"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
	).Replace(string(file))

	response, err := deployDefinitionFromBytes(t, []byte(content), eventBasedGatewayDelayedPath)
	require.NoError(t, err)
	require.Equal(t, 201, response.StatusCode())
	require.NotNil(t, response.JSON201)

	return response.JSON201.ProcessDefinitionKey
}
