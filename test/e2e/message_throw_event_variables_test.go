package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestMessageThrowEventVariables(t *testing.T) {
	t.Run("Without output mapping completion variables stay local", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/message_event/message-intermediate-throw-event-without-output-mapping.bpmn")
		processVariables := map[string]any{
			"payload":       "message-payload",
			"correlationId": "correlation-123",
			"unchanged":     "process-value",
		}
		processInstance, err := createProcessInstance(t, &definitionKey, processVariables)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		publicationJob := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "message_throw_event")
		expectedInputVariables := mergeMaps(processVariables, map[string]any{
			"messagePayload":       "message-payload",
			"messageCorrelationId": "correlation-123",
		})
		require.Equal(t, expectedInputVariables, publicationJob.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "message_throw_event", expectedInputVariables)

		completionVariables := map[string]any{
			"deliveryId": "delivery-123",
			"ignored":    "ignored-value",
		}
		require.NoError(t, completeJob(t, publicationJob.Key, completionVariables))

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertFlowElementOutputVariables(t, processInstance.Key, "message_throw_event", map[string]any{})
		assertProcessInstanceVariables(t, processInstance.Key, processVariables)
		completedJob := waitForProcessInstanceJobByElementId(t, processInstance.Key, "message_throw_event", public.JobStateCompleted)
		require.Equal(t, completionVariables, *completedJob.OutputVariables)
	})

	t.Run("With output mapping only mapped completion variables are propagated", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/message_event/message-intermediate-throw-event-with-output-mapping.bpmn")
		processVariables := map[string]any{
			"payload":       "message-payload",
			"correlationId": "correlation-456",
			"unchanged":     "process-value",
		}
		processInstance, err := createProcessInstance(t, &definitionKey, processVariables)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		publicationJob := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "message_throw_event")
		expectedInputVariables := mergeMaps(processVariables, map[string]any{
			"messagePayload":       "message-payload",
			"messageCorrelationId": "correlation-456",
		})
		require.Equal(t, expectedInputVariables, publicationJob.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "message_throw_event", expectedInputVariables)

		completionVariables := map[string]any{
			"deliveryId": "delivery-456",
			"ignored":    "ignored-value",
		}
		require.NoError(t, completeJob(t, publicationJob.Key, completionVariables))

		expectedOutputVariables := map[string]any{
			"publicationResult": "delivery-456",
		}
		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertFlowElementOutputVariables(t, processInstance.Key, "message_throw_event", expectedOutputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(processVariables, expectedOutputVariables))
		completedJob := waitForProcessInstanceJobByElementId(t, processInstance.Key, "message_throw_event", public.JobStateCompleted)
		require.Equal(t, completionVariables, *completedJob.OutputVariables)
	})
}
