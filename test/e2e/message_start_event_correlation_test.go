package e2e

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageStartEventCorrelation(t *testing.T) {
	t.Run("wrong message name does not start a process instance", func(t *testing.T) {
		suffix := messageEventTestSuffix()
		processID := fmt.Sprintf("message-start-wrong-name-%d", suffix)
		messageName := fmt.Sprintf("message-start-ref-%d", suffix)
		deployMessageStartDefinitionVersion(t, processID, messageName, fmt.Sprintf("message-start-wrong-name-job-%d", suffix))

		response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			CorrelationKey: nil,
			MessageName:    messageName + "-wrong",
			Variables:      &map[string]any{"messagePayloadVar": "ignored"},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())

		waitForProcessInstancesByBPMNProcessID(t, processID, 0)
	})

	t.Run("correlation key is ignored for a definition-level message start subscription", func(t *testing.T) {
		definition := deployUniqueMessageStartEventDefinition(t, "correlation-key")
		correlationKey := "instance-only-correlation-key"

		response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			CorrelationKey: &correlationKey,
			MessageName:    definition.messageName,
			Variables:      &map[string]any{"messagePayloadVar": "delivered"},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		instance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})
		assertProcessInstanceVariables(t, instance.Key, map[string]any{
			"messagePayloadVar": "delivered",
		})
	})

	t.Run("message start uses the latest deployed process definition version", func(t *testing.T) {
		suffix := messageEventTestSuffix()
		processID := fmt.Sprintf("message-start-latest-version-%d", suffix)
		messageName := fmt.Sprintf("message-start-latest-version-ref-%d", suffix)
		firstDefinitionKey := deployMessageStartDefinitionVersion(t, processID, messageName, fmt.Sprintf("message-start-latest-version-job-%d-v1", suffix))
		secondDefinitionKey := deployMessageStartDefinitionVersion(t, processID, messageName, fmt.Sprintf("message-start-latest-version-job-%d-v2", suffix))
		require.NotEqual(t, firstDefinitionKey, secondDefinitionKey)

		response, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			CorrelationKey: nil,
			MessageName:    messageName,
			Variables:      &map[string]any{"messagePayloadVar": "latest"},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		instances := waitForProcessInstancesByBPMNProcessID(t, processID, 1)
		require.Equal(t, secondDefinitionKey, instances[0].ProcessDefinitionKey)
		require.Equal(t, zenclient.ProcessInstanceStateActive, instances[0].State)
		require.NotEqual(t, firstDefinitionKey, instances[0].ProcessDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instances[0].Key)
		})
	})
}

func deployMessageStartDefinitionVersion(t testing.TB, processID string, messageName string, jobType string) int64 {
	t.Helper()

	return deployMessageStartDefinition(t, processID, messageName, jobType)
}
