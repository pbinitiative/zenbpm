package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestStartEventIncident(t *testing.T) {
	t.Run("Failed output mappings are atomic across incident retries", func(t *testing.T) {
		definitionKey := deployStartEventDefinition(t, `<bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:output source="=attempt + &#34;-mapped&#34;" target="attempt" />
          <zenbpm:output source="=attempt." target="mapped_value" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>`)

		processInstance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{
			"attempt": "initial",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "start_event", runtime.TokenStateFailed)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"attempt": "initial",
		})

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "start_event", incidents[0].ElementId)
		require.Contains(t, incidents[0].Message, "failed to evaluate StartEvent output mappings")

		response, err := app.restClient.ResolveIncidentWithResponse(t.Context(), incidents[0].Key)
		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, response.StatusCode())
		require.Contains(t, string(response.Body), "failed to evaluate StartEvent output mappings")

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "start_event", runtime.TokenStateFailed)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "first_flow_node")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"attempt": "initial",
		})

		incidents, err = getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 2)

		resolvedCount := 0
		unresolvedCount := 0
		for _, incident := range incidents {
			require.Equal(t, "start_event", incident.ElementId)
			require.Contains(t, incident.Message, "failed to evaluate StartEvent output mappings")
			if incident.ResolvedAt == nil {
				unresolvedCount++
			} else {
				resolvedCount++
			}
		}
		require.Equal(t, 1, resolvedCount)
		require.Equal(t, 1, unresolvedCount)
	})
}
