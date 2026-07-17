package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetProcessInstancesIncidentCount verifies that the /process-instances
// endpoint reports the number of unresolved incidents on each instance.
func TestGetProcessInstancesIncidentCount(t *testing.T) {
	t.Run("active instance without incident reports zero", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, new(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		})

		var items []zenclient.ProcessInstancesSimple
		require.Eventually(t, func() bool {
			items = findProcessInstancesByKey(t, definition.Key, instance.Key)
			return len(items) > 0
		}, 5*time.Second, 100*time.Millisecond, "instance must be visible in /process-instances")

		require.Len(t, items, 1)
		require.NotNil(t, items[0].IncidentCount)
		assert.Equal(t, 0, *items[0].IncidentCount, "instance with no incident should report zero")
	})

	t.Run("active instance with unresolved incident reports the count", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, new(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		})

		// Fail the service task job to create an unresolved incident while the
		// instance is still active. The same precondition as in the issue
		// reproduction steps.
		failJobForElementId(t, instance.Key, "service-task-1", nil, nil)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)

		var items []zenclient.ProcessInstancesSimple
		require.Eventually(t, func() bool {
			items = findProcessInstancesByKey(t, definition.Key, instance.Key)
			return len(items) == 1 && items[0].IncidentCount != nil && *items[0].IncidentCount == 1
		}, 5*time.Second, 100*time.Millisecond,
			"active instance with one unresolved incident must report incidentCount=1")

		require.Len(t, items, 1)
		require.NotNil(t, items[0].IncidentCount)
		assert.Equal(t, 1, *items[0].IncidentCount)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, items[0].State,
			"instance must still be active after the job was failed (incident does not change instance state)")
	})

	t.Run("active instance reports zero after incidents are resolved", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, new(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		})

		failJobForElementId(t, instance.Key, "service-task-1", nil, nil)
		assertProcessInstanceIncidentsLength(t, instance.Key, 1)

		// resolve the incident via the existing helper.
		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, incidents)
		resolveIncident(t, incidents[0].Key)

		var items []zenclient.ProcessInstancesSimple
		require.Eventually(t, func() bool {
			items = findProcessInstancesByKey(t, definition.Key, instance.Key)
			return len(items) == 1 && items[0].IncidentCount != nil && *items[0].IncidentCount == 0
		}, 10*time.Second, 100*time.Millisecond,
			"instance with no open incidents should report incidentCount=0")
	})
}

func TestProcessInstancesSimpleOmitsIncidentCountWhenNotPopulated(t *testing.T) {
	body, err := json.Marshal(public.ProcessInstancesSimple{})
	require.NoError(t, err)
	require.NotContains(t, string(body), `"incidentCount"`)
}

// findProcessInstancesByKey returns items for the test-owned process definition
// that match the given instance key across all partitions.
func findProcessInstancesByKey(t *testing.T, definitionKey, instanceKey int64) []zenclient.ProcessInstancesSimple {
	t.Helper()
	resp, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
		ProcessDefinitionKey: new(definitionKey),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)

	var matched []zenclient.ProcessInstancesSimple
	for _, partition := range resp.JSON200.Partitions {
		for _, item := range partition.Items {
			if item.Key == instanceKey {
				matched = append(matched, item)
			}
		}
	}
	return matched
}
