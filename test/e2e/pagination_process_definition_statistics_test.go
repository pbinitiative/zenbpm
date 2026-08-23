package e2e

import (
	"net/http"
	"testing"

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
					Page:                       new(int32(page)),
					Size:                       new(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)
			require.NotEmpty(t, resp.JSON200.Partitions)

			partitionTotalCount := 0
			partitionTotalExceedsPageCount := false
			for _, partition := range resp.JSON200.Partitions {
				partitionTotalCount += partition.TotalCount
				partitionTotalExceedsPageCount = partitionTotalExceedsPageCount || partition.TotalCount > len(partition.Items)
			}
			assert.Equal(t, resp.JSON200.TotalCount, partitionTotalCount)
			assert.True(t, partitionTotalExceedsPageCount, "at least one partition totalCount must exceed its current page item count")

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

func TestProcessDefinitionStatisticsEmptyKeyFilter(t *testing.T) {
	def := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/pagination-test-service-task.bpmn")
	require.NotZero(t, def.Key)

	resp, err := app.restClient.GetProcessDefinitionStatisticsWithResponse(t.Context(),
		&zenclient.GetProcessDefinitionStatisticsParams{
			BpmnProcessDefinitionKeyIn: &[]int64{}, // empty array — must not filter out everything
			Page:                       new(int32(1)),
			Size:                       new(int32(1)),
		})
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())
	require.NotNil(t, resp.JSON200)
	assert.Equal(t, 1, len(allStatsItems(resp.JSON200)), "page 1 with size 1 should return exactly 1 item")
	assert.Greater(t, resp.JSON200.TotalCount, 1, "totalCount should be at least 1 when empty filter is a no-op")
}

func createDefinitionKeysForStats(t *testing.T, count int) []int64 {
	t.Helper()

	var keys []int64
	for range count {
		def := deployAndGetUniqueProcessDefinition(t, "../../pkg/bpmn/test-cases/pagination-test-service-task.bpmn")
		require.NotZero(t, def.Key)
		keys = append(keys, def.Key)
	}
	return keys
}
