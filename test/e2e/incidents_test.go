package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestIncidentStateFilter(t *testing.T) {
	// Deploy a process definition that will create incidents
	definition, err := deployGetUniqueDefinition(t, "exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)

	var unresolvedIncidentProcessInstanceKey int64
	var resolvedIncidentProcessInstanceKey int64

	// Create an instance with variables that will cause an incident
	instance, err := createProcessInstance(t, definition.Key, map[string]any{
		"price": 0, // This should cause an incident due to exclusive gateway condition
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)
	unresolvedIncidentProcessInstanceKey = instance.Key

	// Create another instance that will create an incident
	instance, err = createProcessInstance(t, definition.Key, map[string]any{
		"price": 0,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)
	resolvedIncidentProcessInstanceKey = instance.Key

	// Resolve the one of the incidents
	incidents, err := getProcessInstanceIncidents(t, resolvedIncidentProcessInstanceKey)
	assert.NoError(t, err)
	assert.NotEmpty(t, incidents)

	err = updateProcessInstanceVariables(t, resolvedIncidentProcessInstanceKey, map[string]any{
		"price": 50, // Fix the condition that caused the incident
	})
	assert.NoError(t, err)
	err = resolveIncident(t, incidents[0].Key)
	assert.NoError(t, err)

	resolvedIncidentKey := incidents[0].Key

	var unresolvedIncidents []zenclient.Incident

	t.Run("get unresolved incidents", func(t *testing.T) {
		// Test getting incidents without state filter (should return all)
		allIncidents, err := getProcessInstanceIncidents(t, unresolvedIncidentProcessInstanceKey)
		assert.NoError(t, err)
		assert.NotEmpty(t, allIncidents)

		// Test filtering for unresolved incidents
		unresolvedIncidents, err = getProcessInstanceIncidentsByState(t, unresolvedIncidentProcessInstanceKey, "unresolved")
		assert.NoError(t, err)
		assert.NotEmpty(t, unresolvedIncidents)

		// Verify all returned incidents are unresolved (ResolvedAt should be nil)
		for _, incident := range unresolvedIncidents {
			assert.Nil(t, incident.ResolvedAt, "Expected incident to be unresolved")
		}
	})

	t.Run("test resolved incidents filter", func(t *testing.T) {
		// Wait a bit for the incident resolution to be processed
		time.Sleep(100 * time.Millisecond)

		// Test filtering for resolved incidents
		resolvedIncidents, err := getProcessInstanceIncidentsByState(t, resolvedIncidentProcessInstanceKey, "resolved")
		assert.NoError(t, err)
		assert.NotEmpty(t, resolvedIncidents)

		// Verify all returned incidents are resolved (ResolvedAt should not be nil)
		for _, incident := range resolvedIncidents {
			assert.NotNil(t, incident.ResolvedAt, "Expected incident to be resolved")
		}

		// Verify the specific incident we resolved is in the list
		var foundResolvedIncident bool
		for _, incident := range resolvedIncidents {
			if incident.Key == resolvedIncidentKey {
				foundResolvedIncident = true
				break
			}
		}
		assert.True(t, foundResolvedIncident, "Expected to find the resolved incident in filtered results")
	})

	t.Run("test unresolved incidents filter after resolution", func(t *testing.T) {
		// Test filtering for unresolved incidents on the process instance where we resolved an incident
		unresolvedAfterResolution, err := getProcessInstanceIncidentsByState(t, resolvedIncidentProcessInstanceKey, "unresolved")
		assert.NoError(t, err)

		// Should not contain the resolved incident
		for _, incident := range unresolvedAfterResolution {
			assert.NotEqual(t, resolvedIncidentKey, incident.Key, "Resolved incident should not appear in unresolved filter")
			assert.Nil(t, incident.ResolvedAt, "All incidents in unresolved filter should have nil ResolvedAt")
		}
	})

	t.Run("test no filter returns all incidents", func(t *testing.T) {
		// Test getting all incidents without state filter
		allIncidents, err := getProcessInstanceIncidents(t, resolvedIncidentProcessInstanceKey)
		assert.NoError(t, err)
		assert.NotEmpty(t, allIncidents)

		// Should contain both resolved and unresolved incidents
		var hasResolved, hasUnresolved bool
		for _, incident := range allIncidents {
			if incident.ResolvedAt != nil {
				hasResolved = true
			} else {
				hasUnresolved = true
			}
		}

		// If we have both types, verify the counts
		if hasResolved && hasUnresolved {
			resolvedCount := 0
			unresolvedCount := 0
			for _, incident := range allIncidents {
				if incident.ResolvedAt != nil {
					resolvedCount++
				} else {
					unresolvedCount++
				}
			}
			assert.Greater(t, resolvedCount, 0, "Should have at least one resolved incident")
			assert.Greater(t, unresolvedCount, 0, "Should have at least one unresolved incident")
		}
	})
}

// Helper function to get incidents with state filter
func getProcessInstanceIncidentsByState(t testing.TB, processInstanceKey int64, state string) ([]zenclient.Incident, error) {

	r, err := app.restClient.GetIncidentsWithResponse(t.Context(), processInstanceKey, &zenclient.GetIncidentsParams{
		State: ptr.To(zenclient.GetIncidentsParamsState(state)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal incident page: %w", err)
	}

	if r.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to get incidents: %s data: %s", r.Status(), r.Body)
	}

	return r.JSON200.Items, nil
}

// Helper function to update process instance variables
func updateProcessInstanceVariables(t testing.TB, processInstanceKey int64, variables map[string]any) error {
	r, err := app.restClient.UpdateProcessInstanceVariablesWithResponse(t.Context(), processInstanceKey, zenclient.UpdateProcessInstanceVariablesJSONRequestBody{
		Variables: variables,
	})

	if err != nil {
		return fmt.Errorf("failed to update process instance variables: %w", err)
	}

	if r.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("failed to update process instance variables: %s data: %s", r.Status(), r.Body)
	}
	return nil
}

func resolveIncident(t testing.TB, key int64) error {
	r, err := app.restClient.ResolveIncidentWithResponse(t.Context(), key)

	if err != nil {
		return fmt.Errorf("failed to resolve incident: %w", err)
	}
	if r.StatusCode() != http.StatusCreated {
		return fmt.Errorf("failed to resolve incident: %s data: %s", r.Status(), r.Body)
	}

	return nil
}
