package e2e

import (
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

// allStatsItems collects all ProcessDefinitionStatistics items from all partitions.
func allStatsItems(page *zenclient.ProcessDefinitionStatisticsPage) []zenclient.ProcessDefinitionStatistics {
	var items []zenclient.ProcessDefinitionStatistics
	for _, p := range page.Partitions {
		items = append(items, p.Items...)
	}
	return items
}

func TestProcessDefinitionStatistics(t *testing.T) {
	// Deploy a unique definition so we have a known bpmnProcessId
	definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	assert.NotEmpty(t, definition.Key)

	t.Run("empty statistics for new definition", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.NotNil(t, resp.JSON200)

		// Find our definition in the results
		var found *zenclient.ProcessDefinitionStatistics
		for _, item := range allStatsItems(resp.JSON200) {
			if item.Key == definition.Key {
				found = &item
				break
			}
		}
		assert.NotNil(t, found, "definition should be in statistics")
		assert.Equal(t, 0, found.InstanceCounts.Total)
		assert.Equal(t, 0, found.InstanceCounts.Active)
	})

	// Create 2 active instances (service task will wait for completion)
	_, err = createProcessInstance(t, &definition.Key, map[string]any{"testVar": 1})
	assert.NoError(t, err)
	_, err = createProcessInstance(t, &definition.Key, map[string]any{"testVar": 2})
	assert.NoError(t, err)

	t.Run("statistics with active instances", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())

		var found *zenclient.ProcessDefinitionStatistics
		for _, item := range allStatsItems(resp.JSON200) {
			if item.Key == definition.Key {
				found = &item
				break
			}
		}
		assert.NotNil(t, found)
		assert.Equal(t, 2, found.InstanceCounts.Total)
		assert.Equal(t, 2, found.InstanceCounts.Active)
		assert.Equal(t, 0, found.InstanceCounts.Completed)
	})

	// Deploy a definition that produces incidents
	incidentDef, err := deployGetUniqueDefinition(t, "exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)

	// Create instance that causes an incident (price=0 causes no matching condition)
	incidentInstance, err := createProcessInstance(t, &incidentDef.Key, map[string]any{"price": 0})
	assert.NoError(t, err)
	assert.NotEmpty(t, incidentInstance.Key)

	t.Run("statistics with incidents", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessIdIn: &[]string{}, // testing with empty array to cover potential edge case
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())

		var found *zenclient.ProcessDefinitionStatistics
		for _, item := range allStatsItems(resp.JSON200) {
			if item.Key == incidentDef.Key {
				found = &item
				break
			}
		}
		assert.NotNil(t, found)
		assert.Equal(t, 1, found.InstanceCounts.Total)
		assert.Equal(t, 1, found.InstanceCounts.Failed)
	})

	t.Run("filter by bpmnProcessIdIn", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessIdIn: &[]string{definition.BpmnProcessId},
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.Equal(t, 1, resp.JSON200.TotalCount)
		items := allStatsItems(resp.JSON200)
		assert.Equal(t, definition.BpmnProcessId, items[0].BpmnProcessId)
	})

	t.Run("filter by bpmnProcessDefinitionKeyIn", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessDefinitionKeyIn: &[]int64{definition.Key},
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.Equal(t, 1, resp.JSON200.TotalCount)
		items := allStatsItems(resp.JSON200)
		assert.Equal(t, definition.Key, items[0].Key)
	})

	t.Run("filter by multiple bpmnProcessIdIn", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessIdIn: &[]string{definition.BpmnProcessId, incidentDef.BpmnProcessId},
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.Equal(t, 2, resp.JSON200.TotalCount)
	})

	t.Run("filter by non-existent bpmnProcessIdIn returns empty", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessIdIn: &[]string{"non-existent-process-id"},
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.Equal(t, 0, resp.JSON200.TotalCount)
		assert.Empty(t, allStatsItems(resp.JSON200))
	})

	t.Run("pagination", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				BpmnProcessDefinitionKeyIn: &[]int64{}, // testing with empty array to cover potential edge case,
				Page:                       ptr.To(int32(1)),
				Size:                       ptr.To(int32(1)),
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		assert.Equal(t, 1, len(allStatsItems(resp.JSON200)))
		assert.Greater(t, resp.JSON200.TotalCount, 1)
	})

	t.Run("name filter", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				Name: ptr.To("service-task"),
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		// All returned items should match the name filter
		for _, item := range allStatsItems(resp.JSON200) {
			assert.Contains(t, item.BpmnProcessId, "service-task")
		}
	})

	t.Run("onlyLatest filter", func(t *testing.T) {
		// Deploy a second version of our definition
		_, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)

		allVersions, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				OnlyLatest: ptr.To(false),
			})
		assert.NoError(t, err)

		latestOnly, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				OnlyLatest: ptr.To(true),
			})
		assert.NoError(t, err)
		assert.LessOrEqual(t, latestOnly.JSON200.TotalCount, allVersions.JSON200.TotalCount)
	})

	t.Run("sort by version desc", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				SortBy:    ptr.To(zenclient.GetProcessDefinitionStatisticsParamsSortByVersion),
				SortOrder: ptr.To(zenclient.GetProcessDefinitionStatisticsParamsSortOrderDesc),
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		items := allStatsItems(resp.JSON200)
		if len(items) >= 2 {
			assert.GreaterOrEqual(t, items[0].Version, items[1].Version)
		}
	})

	t.Run("sort by instanceCount desc", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				SortBy:    ptr.To(zenclient.GetProcessDefinitionStatisticsParamsSortByInstanceCount),
				SortOrder: ptr.To(zenclient.GetProcessDefinitionStatisticsParamsSortOrderDesc),
			})
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		items := allStatsItems(resp.JSON200)
		if len(items) >= 2 {
			assert.GreaterOrEqual(t, items[0].InstanceCounts.Total, items[1].InstanceCounts.Total)
		}
	})

	t.Run("bad request for page=0", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				Page: ptr.To(int32(0)),
				Size: ptr.To(int32(10)),
			})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})

	t.Run("bad request for size>100", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionStatisticsParams{
				Page: ptr.To(int32(1)),
				Size: ptr.To(int32(101)),
			})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})
}

func TestGetProcessDefinitionElementStatistics(t *testing.T) {
	definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	require.NoError(t, err)

	t.Run("returns empty statistics when no instances exist", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), definition.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		totalActive, totalIncidents := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 0, totalActive)
		assert.Equal(t, 0, totalIncidents)
	})

	// Create two instances that will block at the service task (waiting for job completion)
	instance1, err := createProcessInstance(t, &definition.Key, map[string]any{"testVar": 1})
	require.NoError(t, err)
	instance2, err := createProcessInstance(t, &definition.Key, map[string]any{"testVar": 2})
	require.NoError(t, err)

	t.Run("returns active counts for blocked instances", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), definition.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)
		assert.NotEmpty(t, resp.JSON200.Partitions)

		totalActive, _ := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 2, totalActive, "both instances should show active token at service-task-1")

		activeByElement := collectActiveByElement(resp.JSON200)
		assert.Equal(t, 2, activeByElement["service-task-1"], "both instances should be active at service-task-1")
	})

	t.Run("active count decreases after instance is cancelled", func(t *testing.T) {
		cancelResp, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance1.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, cancelResp.StatusCode())

		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), definition.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		activeByElement := collectActiveByElement(resp.JSON200)
		assert.Equal(t, 1, activeByElement["service-task-1"], "only one instance should remain active after cancel")
	})

	t.Run("incident count appears for process instance with incident", func(t *testing.T) {
		incidentDefinition, err := deployGetUniqueDefinition(t, "exclusive-gateway-with-condition.bpmn")
		require.NoError(t, err)

		_, err = createProcessInstance(t, &incidentDefinition.Key, map[string]any{"price": 0})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), incidentDefinition.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		_, totalIncidents := sumElementStatistics(resp.JSON200)
		assert.Greater(t, totalIncidents, 0, "should have at least one incident")
	})

	// cleanup: cancel remaining instance
	app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance2.Key) //nolint:errcheck
}

func TestGetProcessDefinitionElementStatisticsMultiInstance(t *testing.T) {
	cleanProcessInstances(t)
	definition, err := deployGetUniqueDefinition(t, "multi_instance_parallel_service_task.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{
		"testInputCollection": []string{"a", "b", "c"},
	})
	require.NoError(t, err)

	t.Run("definition element statistics excludes multi-instance scope token", func(t *testing.T) {
		assert.Eventually(t, func() bool {
			resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), definition.Key)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)

			activeByElement := collectActiveByElement(resp.JSON200)
			if 3 == activeByElement["Activity_0rae016"] {
				return true
			}

			return false
		}, 2*time.Second, 100*time.Millisecond, "activeByElement[\"Activity_0rae016\"] should be equal to 3")

	})

	t.Run("instance element statistics shows child body tokens", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		activeByElement := collectActiveByElement(resp.JSON200)
		assert.Equal(t, 3, activeByElement["Activity_0rae016"],
			"should count child body tokens, not the parent scope token")
	})

	app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key) //nolint:errcheck
}

func sumElementStatistics(stats *zenclient.ElementStatisticsPartitions) (totalActive, totalIncidents int) {
	for _, partition := range stats.Partitions {
		for _, counts := range partition.Items {
			totalActive += counts.ActiveCount
			totalIncidents += counts.IncidentCount
		}
	}
	return
}

func collectActiveByElement(stats *zenclient.ElementStatisticsPartitions) map[string]int {
	result := make(map[string]int)
	for _, partition := range stats.Partitions {
		for elementId, counts := range partition.Items {
			result[elementId] += counts.ActiveCount
		}
	}
	return result
}
