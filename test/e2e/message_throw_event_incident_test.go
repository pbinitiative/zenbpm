package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageThrowEventIncident(t *testing.T) {
	t.Run("Failing the publication job creates an incident and keeps the event waiting", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/message_event/message-intermediate-throw-event-without-output-mapping.bpmn")
		processVariables := map[string]any{
			"payload":       "message-payload",
			"correlationId": "correlation-incident",
		}
		processInstance, err := createProcessInstance(t, &definitionKey, processVariables)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "message_throw_event")
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		failJobForElementId(t, processInstance.Key, "message_throw_event", nil, map[string]any{
			"sendError": "transport-unavailable",
		})

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "message_throw_event", public.JobStateFailed)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "message_throw_event", runtime.TokenStateWaiting)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "message_throw_event", incidents[0].ElementId)
		assertProcessInstanceVariables(t, processInstance.Key, processVariables)
	})
}
