package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageCatchEventCorrelation(t *testing.T) {
	definitionKey, messageName := deployMessageCatchCorrelationDefinition(t)
	firstCorrelationKey := fmt.Sprintf("%s-first", messageName)
	secondCorrelationKey := fmt.Sprintf("%s-second", messageName)

	firstInstance := createMessageCatchCorrelationInstance(t, definitionKey, firstCorrelationKey)
	secondInstance := createMessageCatchCorrelationInstance(t, definitionKey, secondCorrelationKey)

	assertMessageCatchStillWaiting(t, firstInstance.Key)
	assertMessageCatchStillWaiting(t, secondInstance.Key)

	t.Run("wrong message name does not consume a matching correlation key", func(t *testing.T) {
		response, err := publishMessageWithResponse(t, messageName+"-wrong", firstCorrelationKey, &map[string]any{
			"winner": "wrong-name",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())

		assertMessageCatchStillWaiting(t, firstInstance.Key)
		assertMessageCatchStillWaiting(t, secondInstance.Key)
	})

	t.Run("wrong correlation key does not consume a matching message name", func(t *testing.T) {
		response, err := publishMessageWithResponse(t, messageName, secondCorrelationKey+"-wrong", &map[string]any{
			"winner": "wrong-key",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())

		assertMessageCatchStillWaiting(t, firstInstance.Key)
		assertMessageCatchStillWaiting(t, secondInstance.Key)
	})

	t.Run("message is delivered only to the instance with the matching key", func(t *testing.T) {
		err := publishMessage(t, messageName, secondCorrelationKey, &map[string]any{
			"winner": "second",
		})
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, secondInstance.Key, "Event_1jn3jqk")
		assertMessageSubscriptionStateCount(t, secondInstance.Key, "id-1", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, secondInstance.Key, "id-1", zenclient.EventSubscriptionStateCompleted, 1)
		assertProcessInstanceVariables(t, secondInstance.Key, map[string]any{
			"correlationKey": secondCorrelationKey,
			"winner":         "second",
		})

		assertMessageCatchStillWaiting(t, firstInstance.Key)
		assertProcessInstanceVariables(t, firstInstance.Key, map[string]any{
			"correlationKey": firstCorrelationKey,
		})
	})

	t.Run("duplicate message after subscription cleanup is rejected", func(t *testing.T) {
		response, err := publishMessageWithResponse(t, messageName, secondCorrelationKey, &map[string]any{
			"winner": "duplicate",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode())

		assertProcessInstanceIsCompleted(t, secondInstance.Key, "Event_1jn3jqk")
		assertMessageSubscriptionStateCount(t, secondInstance.Key, "id-1", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, secondInstance.Key, "id-1", zenclient.EventSubscriptionStateCompleted, 1)
		assertProcessInstanceVariables(t, secondInstance.Key, map[string]any{
			"correlationKey": secondCorrelationKey,
			"winner":         "second",
		})
	})

	t.Run("remaining instance can still be correlated afterwards", func(t *testing.T) {
		err := publishMessage(t, messageName, firstCorrelationKey, &map[string]any{
			"winner": "first",
		})
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, firstInstance.Key, "Event_1jn3jqk")
		assertProcessInstanceVariables(t, firstInstance.Key, map[string]any{
			"correlationKey": firstCorrelationKey,
			"winner":         "first",
		})
	})
}

func deployMessageCatchCorrelationDefinition(t testing.TB) (int64, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	processID := fmt.Sprintf("message-intermediate-catch-correlation-%d", suffix)
	messageName := fmt.Sprintf("message-intermediate-catch-correlation-ref-%d", suffix)
	bpmnData, err := readE2ETestDataBPMN("message_event/message-intermediate-catch-event.bpmn")
	require.NoError(t, err)
	content := strings.NewReplacer(
		`bpmn:process id="message-intermediate-catch-event"`,
		fmt.Sprintf(`bpmn:process id="%s"`, processID),
		`name="globalMsgRef"`,
		fmt.Sprintf(`name="%s"`, messageName),
		`correlationKey="=&#34;correlation-key-one&#34;"`,
		`correlationKey="=correlationKey"`,
	).Replace(string(bpmnData))

	return deployBPMNTestCaseContent(t, "message-intermediate-catch-event-correlation.bpmn", []byte(content)), messageName
}

func createMessageCatchCorrelationInstance(t testing.TB, definitionKey int64, correlationKey string) zenclient.ProcessInstance {
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

func assertMessageCatchStillWaiting(t testing.TB, processInstanceKey int64) {
	t.Helper()

	waitForProcessInstanceState(t, processInstanceKey, zenclient.ProcessInstanceStateActive)
	assertProcessInstanceTokenState(t, processInstanceKey, "id-1", runtime.TokenStateWaiting)
	assertMessageSubscriptionState(t, processInstanceKey, "id-1", zenclient.EventSubscriptionStateActive)
	assertProcessInstanceHasNoActiveJobByElementId(t, processInstanceKey, "Event_1jn3jqk")
}
