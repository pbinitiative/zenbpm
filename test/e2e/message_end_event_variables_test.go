package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageEndEventVariables(t *testing.T) {
	t.Run("Input mapping preserves required types, optional values, Unicode, identifiers and timestamps", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventPayloadPath)
		businessKey := uniqueMessageEndEventBusinessKey("payload")
		messageID := "db1ef6b5-af74-4df9-a364-e528c1af0d71"
		customerName := "Příliš žluťoučký kůň 🐎 – 東京"
		occurredAt := "2026-01-15T19:21:42.123+01:00"
		instance := createMessageEndEventProcessInstance(t, definitionKey, businessKey, messageEndEventPayloadVariables(
			businessKey,
			messageID,
			"order-žluťoučký-東京",
			customerName,
			occurredAt,
		))
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		completeJobForElementId(t, instance.Key, "prepare_publication", nil)
		publicationJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageEndEventElementID)

		require.Equal(t, "order-žluťoučký-東京", publicationJob.InputVariables["orderId"])
		require.Equal(t, float64(42.5), publicationJob.InputVariables["amount"])
		require.Equal(t, true, publicationJob.InputVariables["approved"])
		require.Equal(t, "order.completed", publicationJob.InputVariables["messageType"])
		require.Equal(t, businessKey, publicationJob.InputVariables["correlationId"])
		require.Equal(t, messageID, publicationJob.InputVariables["messageId"])
		require.Equal(t, customerName, publicationJob.InputVariables["customerName"])
		require.Equal(t, occurredAt, publicationJob.InputVariables["occurredAt"])
		require.Contains(t, publicationJob.InputVariables, "optionalNote")
		require.Nil(t, publicationJob.InputVariables["optionalNote"])
		require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, publicationJob.InputVariables["messageId"])

		emittedAt, ok := publicationJob.InputVariables["emittedAt"].(string)
		require.True(t, ok, "emittedAt must be serialized as a string")
		require.Equal(t, occurredAt, emittedAt)
		_, err := time.Parse(time.RFC3339Nano, emittedAt)
		require.NoError(t, err, "emittedAt must contain an RFC3339 timestamp with an offset")
		_, err = time.Parse(time.RFC3339Nano, publicationJob.InputVariables["occurredAt"].(string))
		require.NoError(t, err, "occurredAt must preserve its RFC3339 timestamp and offset")

		require.NoError(t, completeJob(t, publicationJob.Key, nil))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("Two instances keep input and correlation variables isolated", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventPayloadPath)
		firstBusinessKey := uniqueMessageEndEventBusinessKey("multi-first")
		secondBusinessKey := uniqueMessageEndEventBusinessKey("multi-second")
		firstMessageID := "773d4dc7-bcd6-4426-aa75-a4999a9ff982"
		secondMessageID := "b0b7fc9d-e40a-4822-b3ae-94f3ff925e70"

		first := createMessageEndEventProcessInstance(t, definitionKey, firstBusinessKey, messageEndEventPayloadVariables(
			firstBusinessKey,
			firstMessageID,
			"order-first",
			"První zákazník",
			"2026-07-16T07:00:00Z",
		))
		second := createMessageEndEventProcessInstance(t, definitionKey, secondBusinessKey, messageEndEventPayloadVariables(
			secondBusinessKey,
			secondMessageID,
			"order-second",
			"Second Customer",
			"2026-07-16T08:00:00+01:00",
		))
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, first.Key)
			cleanupOwnedProcessInstance(t, second.Key)
		})

		completeJobForElementId(t, first.Key, "prepare_publication", nil)
		completeJobForElementId(t, second.Key, "prepare_publication", nil)
		firstJob := waitForProcessInstanceActiveJobByElementId(t, first.Key, messageEndEventElementID)
		secondJob := waitForProcessInstanceActiveJobByElementId(t, second.Key, messageEndEventElementID)

		require.NotEqual(t, firstJob.Key, secondJob.Key)
		require.Equal(t, "order-first", firstJob.InputVariables["orderId"])
		require.Equal(t, firstBusinessKey, firstJob.InputVariables["correlationId"])
		require.Equal(t, firstMessageID, firstJob.InputVariables["messageId"])
		require.Equal(t, "order-second", secondJob.InputVariables["orderId"])
		require.Equal(t, secondBusinessKey, secondJob.InputVariables["correlationId"])
		require.Equal(t, secondMessageID, secondJob.InputVariables["messageId"])
		require.NotEqual(t, firstJob.InputVariables["messageId"], secondJob.InputVariables["messageId"])

		firstInstance, err := getProcessInstance(t, first.Key)
		require.NoError(t, err)
		secondInstance, err := getProcessInstance(t, second.Key)
		require.NoError(t, err)
		require.Equal(t, firstBusinessKey, *firstInstance.BusinessKey)
		require.Equal(t, secondBusinessKey, *secondInstance.BusinessKey)

		require.NoError(t, completeJob(t, firstJob.Key, nil))
		require.NoError(t, completeJob(t, secondJob.Key, nil))
		waitForTwoProcessInstanceStates(t, first.Key, zenclient.ProcessInstanceStateCompleted, second.Key, zenclient.ProcessInstanceStateCompleted)
	})
}
