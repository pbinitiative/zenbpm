package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const inclusiveGatewayNoMatchNoDefaultPath = "testdata/gateway_inclusive/inclusive_gateway_no_match_no_default.bpmn"

func TestInclusiveGatewayIncident(t *testing.T) {
	t.Run("No condition matched without a default creates an incident", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewayNoMatchNoDefaultPath, map[string]any{
			"routeA": false,
			"routeB": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"inclusive_gateway"}, []string{"end_event_a", "end_event_b"})
		assertProcessInstanceTokenState(t, processInstance.Key, "inclusive_gateway", runtime.TokenStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "inclusive_gateway", incidents[0].ElementId)
		require.Contains(t, incidents[0].Message, "No default flow, nor matching expressions found for gateway: inclusive_gateway")
	})

	t.Run("No condition matched incident can be resolved after fixing variables", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewayNoMatchNoDefaultPath, map[string]any{
			"routeA": false,
			"routeB": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "inclusive_gateway", runtime.TokenStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)

		err = updateProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"routeA": true,
		})
		require.NoError(t, err)

		resolveIncident(t, incidents[0].Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event_a"}, []string{"end_event_b"})

		resolvedIncidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, resolvedIncidents, 1)
		require.NotNil(t, resolvedIncidents[0].ResolvedAt)
	})
}
