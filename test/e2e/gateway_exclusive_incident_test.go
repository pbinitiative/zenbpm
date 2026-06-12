package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestExclusiveGatewayIncident(t *testing.T) {

	t.Run("Null condition value with a default creates an incident", func(t *testing.T) {

		assertExclusiveGatewayIncident(t,
			"testdata/gateway_exclusive/exclusive_gateway_with_default_flow_simple.bpmn",
			map[string]any{"isSuccess": nil},
			"did not evaluate to a boolean",
		)
	})

	t.Run("Missing condition variable with a default creates an incident", func(t *testing.T) {

		assertExclusiveGatewayIncident(t,
			"testdata/gateway_exclusive/exclusive_gateway_with_default_flow_simple.bpmn",
			map[string]any{},
			"did not evaluate to a boolean",
		)
	})

	t.Run("No condition matched without a default creates an incident", func(t *testing.T) {

		assertExclusiveGatewayIncident(t,
			"testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn",
			map[string]any{"amount": 1000},
			"No default flow, nor matching expressions found",
		)
	})

	t.Run("No condition matched without a default creates an incident and cannot be resolved", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn", map[string]any{"amount": 1000})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"exclusive_gateway"}, nil)
		assertProcessInstanceTokenState(t, processInstance.Key, "exclusive_gateway", runtime.TokenStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)

		r, err := app.restClient.ResolveIncidentWithResponse(t.Context(), incidents[0].Key)
		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, r.StatusCode())
		require.Contains(t, string(r.Body), "No default flow, nor matching expressions found")
	})
}

func assertExclusiveGatewayIncident(t *testing.T, filePath string, variables map[string]any, expectedMessage string) {
	t.Helper()

	processInstance := deployAndCreateUniqueProcessDefinition(t, filePath, variables)

	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"exclusive_gateway"}, nil)
	assertProcessInstanceTokenState(t, processInstance.Key, "exclusive_gateway", runtime.TokenStateFailed)

	incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	require.Equal(t, "exclusive_gateway", incidents[0].ElementId)
	require.Contains(t, incidents[0].Message, expectedMessage)
}
