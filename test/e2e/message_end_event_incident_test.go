package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageEndEventIncident(t *testing.T) {
	t.Run("failing the message end event job creates an incident and keeps the token waiting", func(t *testing.T) {
		definitionKey := deployTestDataProcessDefinitionKey(t, messageEndEventBasicPath)

		processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{
			"instVar": "instVarValue",
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Event_11flm9z")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "Event_11flm9z", runtime.TokenStateWaiting)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		failJobForElementId(t, processInstance.Key, "Event_11flm9z", nil, map[string]any{
			"sendError": "transport-unavailable",
		})

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "Event_11flm9z", public.JobStateFailed)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "Event_11flm9z", runtime.TokenStateWaiting)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "Event_11flm9z", incidents[0].ElementId)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"instVar": "instVarValue",
		})
	})
}
