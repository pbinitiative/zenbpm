package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestProcessInstancePagination(t *testing.T) {
	var bpmnProcessId string

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-instances",
		Setup: func(t *testing.T) (cleanup func()) {
			id, keys := deployAndCreateProcessInstances(t, 11)
			bpmnProcessId = id

			return func() {
				for _, key := range keys {
					app.restClient.CancelProcessInstanceWithResponse(t.Context(), key) //nolint:errcheck
				}
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetProcessInstancesWithResponse(t.Context(),
				&zenclient.GetProcessInstancesParams{
					BpmnProcessId: &bpmnProcessId,
					Page:          ptr.To(int32(page)),
					Size:          ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)

			count := 0
			for _, p := range resp.JSON200.Partitions {
				count += len(p.Items)
			}
			return count, resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
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

func deployAndCreateProcessInstances(t *testing.T, count int) (string, []int64) {
	t.Helper()

	def, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	require.NoError(t, err)
	require.NotZero(t, def.Key)
	id := def.BpmnProcessId

	var instanceKeys []int64
	for i := 0; i < count; i++ {
		instance, err := createProcessInstance(t, &def.Key, map[string]any{"testVar": i})
		require.NoError(t, err)
		instanceKeys = append(instanceKeys, instance.Key)
	}
	return id, instanceKeys
}
