package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessDefinitionStatisticsPagination(t *testing.T) {
	var keys []int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-definitions/statistics",
		Setup: func(t *testing.T) (cleanup func()) {
			keys = createDefinitionKeysForStats(t, 11)
			return nil
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
				&zenclient.GetProcessDefinitionStatisticsParams{
					BpmnProcessDefinitionKeyIn: &keys,
					Page:                       ptr.To(int32(page)),
					Size:                       ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)
			return len(allStatsItems(resp.JSON200)), resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 5,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 5},
					{Page: 2, ExpectedCount: 5},
					{Page: 3, ExpectedCount: 1},
				},
				TotalCount:     11,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func createDefinitionKeysForStats(t *testing.T, count int) []int64 {
	t.Helper()

	var keys []int64
	for i := 0; i < count; i++ {
		def, err := deployGetUniqueDefinition(t, "pagination-test-service-task.bpmn")
		require.NoError(t, err)
		require.NotZero(t, def.Key)
		keys = append(keys, def.Key)
	}
	return keys
}

func TestProcessDefinitionStatisticsEmptyKeyFilter(t *testing.T) {
	def, err := deployGetUniqueDefinition(t, "pagination-test-service-task.bpmn")
	require.NoError(t, err)
	require.NotZero(t, def.Key)

	resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
		&zenclient.GetProcessDefinitionStatisticsParams{
			BpmnProcessDefinitionKeyIn: &[]int64{}, // empty array — must not filter out everything
			Page:                       ptr.To(int32(1)),
			Size:                       ptr.To(int32(1)),
		})
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	assert.Equal(t, 1, len(allStatsItems(resp.JSON200)), "page 1 with size 1 should return exactly 1 item")
	assert.Greater(t, resp.JSON200.TotalCount, 1, "totalCount should be at least 1 when empty filter is a no-op")
}
