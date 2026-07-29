package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageThrowEventFlow(t *testing.T) {
	t.Run("Process waits for message publication and continues after the job is completed", func(t *testing.T) {
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

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "message_throw_event", runtime.TokenStateWaiting)
		publicationJob := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "message_throw_event")
		require.Equal(t, "message-intermediate-throw", publicationJob.Type)
		require.Equal(t, mergeMaps(processVariables, map[string]any{
			"messagePayload":       "message-payload",
			"messageCorrelationId": "correlation-123",
		}), publicationJob.InputVariables)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_throw",
			"message_throw_event",
		})

		require.NoError(t, completeJob(t, publicationJob.Key, map[string]any{
			"deliveryId": "delivery-123",
		}))

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceTokenCount(t, processInstance.Key, "message_throw_event", 0)
		completedJob := waitForProcessInstanceJobByElementId(t, processInstance.Key, "message_throw_event", public.JobStateCompleted)
		require.Equal(t, publicationJob.Key, completedJob.Key)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_throw",
			"message_throw_event",
			"Flow_throw_to_end",
			"end_event",
		})
	})
}
