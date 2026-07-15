package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageStartEventVariables(t *testing.T) {
	t.Run("Message start without output mapping propagates the complete payload", func(t *testing.T) {
		definition := deployUniqueMessageStartEventDefinition(t, "payload")
		messageID := "f12e89aa-dc71-4794-aa5f-5944ff6f74ce"
		occurredAt := "2026-07-17T10:42:31.123+02:00"
		variables := map[string]any{
			"messageId":    messageID,
			"orderId":      "objednávka-žluťoučký-東京",
			"amount":       42.5,
			"approved":     true,
			"customerName": "Příliš žluťoučký kůň 🐎 – 東京",
			"occurredAt":   occurredAt,
			"optionalNote": nil,
		}

		publishMessageStartEvent(t, definition.messageName, variables)
		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		instance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		assertProcessInstanceVariables(t, instance.Key, variables)
		assertFlowElementOutputVariables(t, instance.Key, messageStartEventElementID, variables)

		taskJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageStartEventTaskID)
		require.Equal(t, messageID, taskJob.InputVariables["messageId"])
		require.Equal(t, "objednávka-žluťoučký-東京", taskJob.InputVariables["orderId"])
		require.Equal(t, 42.5, taskJob.InputVariables["amount"])
		require.Equal(t, true, taskJob.InputVariables["approved"])
		require.Equal(t, "Příliš žluťoučký kůň 🐎 – 東京", taskJob.InputVariables["customerName"])
		require.Contains(t, taskJob.InputVariables, "optionalNote")
		require.Nil(t, taskJob.InputVariables["optionalNote"])

		jobOccurredAt, ok := taskJob.InputVariables["occurredAt"].(string)
		require.True(t, ok, "occurredAt must be serialized as a string")
		require.Equal(t, occurredAt, jobOccurredAt)
		_, err := time.Parse(time.RFC3339Nano, jobOccurredAt)
		require.NoError(t, err, "occurredAt must preserve a valid RFC3339 timestamp with an offset")

		require.NoError(t, completeJob(t, taskJob.Key, nil))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("Message start with output mapping propagates only mapped payload variables", func(t *testing.T) {

		outputMapping := `<bpmn:extensionElements>
			<zenbpm:ioMapping>
			  <zenbpm:output source="=payload" target="mappedPayload" />
			</zenbpm:ioMapping>
		  </bpmn:extensionElements>`

		definition := deployUniqueMessageStartEventDefinitionWithContent(t, "output-mapping", outputMapping)
		messageVariables := map[string]any{
			"payload":  "mapped-value",
			"unmapped": "ignored-value",
			"numeric":  float64(42),
		}

		publishMessageStartEvent(t, definition.messageName, messageVariables)
		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		instance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		expectedMappedVariables := map[string]any{
			"mappedPayload": "mapped-value",
		}
		assertProcessInstanceVariables(t, instance.Key, expectedMappedVariables)
		assertFlowElementOutputVariables(t, instance.Key, messageStartEventElementID, expectedMappedVariables)

		taskJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageStartEventTaskID)
		require.Equal(t, "mapped-value", taskJob.InputVariables["mappedPayload"])
		require.NotContains(t, taskJob.InputVariables, "payload")
		require.NotContains(t, taskJob.InputVariables, "unmapped")
		require.NotContains(t, taskJob.InputVariables, "numeric")

		require.NoError(t, completeJob(t, taskJob.Key, nil))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("Two messages keep their payload variables isolated", func(t *testing.T) {
		definition := deployUniqueMessageStartEventDefinition(t, "isolation")
		firstVariables := map[string]any{
			"messageId":    "f1d5d3bd-3a6f-49d3-a1c2-1fa1192d3328",
			"orderId":      "order-first",
			"customerName": "První zákazník",
		}
		secondVariables := map[string]any{
			"messageId":    "5033cc57-09bc-4f64-bdaf-d1f5542d2cca",
			"orderId":      "order-second",
			"customerName": "Second Customer",
		}

		publishMessageStartEvent(t, definition.messageName, firstVariables)
		firstInstances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		first := firstInstances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, first.Key)
		})

		publishMessageStartEvent(t, definition.messageName, secondVariables)
		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 2)

		var second zenclient.ProcessInstancesSimple
		for _, instance := range instances {
			if instance.Key != first.Key {
				second = instance
				break
			}
		}
		require.NotZero(t, second.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, second.Key)
		})

		assertProcessInstanceVariables(t, first.Key, firstVariables)
		assertProcessInstanceVariables(t, second.Key, secondVariables)

		firstJob := waitForProcessInstanceActiveJobByElementId(t, first.Key, messageStartEventTaskID)
		secondJob := waitForProcessInstanceActiveJobByElementId(t, second.Key, messageStartEventTaskID)
		require.Equal(t, firstVariables["messageId"], firstJob.InputVariables["messageId"])
		require.Equal(t, secondVariables["messageId"], secondJob.InputVariables["messageId"])
		require.NotEqual(t, firstJob.InputVariables["messageId"], secondJob.InputVariables["messageId"])

		require.NoError(t, completeJob(t, firstJob.Key, nil))
		require.NoError(t, completeJob(t, secondJob.Key, nil))
		waitForTwoProcessInstanceStates(
			t,
			first.Key, zenclient.ProcessInstanceStateCompleted,
			second.Key, zenclient.ProcessInstanceStateCompleted,
		)
	})
}
