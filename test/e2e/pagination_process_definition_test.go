package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestProcessDefinitionPagination(t *testing.T) {
	t.Parallel()
	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-definitions",
		Setup: func(t *testing.T) (cleanup func()) {
			createManyProcessDefinitions(t, 11)
			return nil
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(),
				&zenclient.GetProcessDefinitionsParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)
			return len(resp.JSON200.Items), resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 5,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 5},
					{Page: 2, ExpectedCount: 5},
				},
				TotalCount:     11,
				TotalCountMode: AtLeastCount,
			},
		},
	})
}

func createManyProcessDefinitions(t *testing.T, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		def, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		require.NoError(t, err)
		require.NotZero(t, def.Key)
	}
}
