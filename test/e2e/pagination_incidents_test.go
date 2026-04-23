package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestIncidentsPagination(t *testing.T) {
	t.Parallel()
	const totalIncidents = 5
	const incidentJobType = "pagination-incident-job"
	var instanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-instances/{key}/incidents",
		Setup: func(t *testing.T) (cleanup func()) {
			instanceKey = deployAndCreateIncidentInstance(t)
			waitForActiveJobs(t, instanceKey, incidentJobType, totalIncidents)
			failActiveJobs(t, instanceKey, incidentJobType)
			waitForIncidents(t, instanceKey, totalIncidents)

			return func() {
				app.restClient.CancelProcessInstanceWithResponse(t.Context(), instanceKey) //nolint:errcheck
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetIncidentsWithResponse(t.Context(), instanceKey,
				&zenclient.GetIncidentsParams{
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
				PageSize: 2,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 2},
					{Page: 2, ExpectedCount: 2},
					{Page: 3, ExpectedCount: 1},
				},
				TotalCount:     totalIncidents,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func deployAndCreateIncidentInstance(t *testing.T) int64 {
	t.Helper()

	def, err := deployGetUniqueDefinition(t, "pagination-test-incidents.bpmn")
	require.NoError(t, err)
	require.NotZero(t, def.Key)

	instance, err := createProcessInstance(t, &def.Key, map[string]any{})
	require.NoError(t, err)
	return instance.Key
}

func waitForActiveJobs(t *testing.T, instanceKey int64, jobType string, totalIncidents int) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := app.restClient.GetJobsWithResponse(t.Context(),
			&zenclient.GetJobsParams{
				ProcessInstanceKey: &instanceKey,
				JobType:            &jobType,
				Size:               ptr.To(int32(100)),
			})
		if err != nil || resp.JSON200 == nil {
			return false
		}
		activeCount := 0
		for _, p := range resp.JSON200.Partitions {
			for _, job := range p.Items {
				if string(job.State) == "active" {
					activeCount++
				}
			}
		}
		return activeCount >= totalIncidents
	}, 10*time.Second, 200*time.Millisecond, "all %d jobs should become active", totalIncidents)
}

func failActiveJobs(t *testing.T, instanceKey int64, jobType string) {
	t.Helper()

	resp, err := app.restClient.GetJobsWithResponse(t.Context(),
		&zenclient.GetJobsParams{
			ProcessInstanceKey: &instanceKey,
			JobType:            &jobType,
			Size:               ptr.To(int32(100)),
		})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)

	for _, p := range resp.JSON200.Partitions {
		for _, job := range p.Items {
			if string(job.State) != "active" {
				continue
			}
			failResp, err := app.restClient.FailJobWithResponse(t.Context(), job.Key,
				zenclient.FailJobJSONRequestBody{})
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, failResp.StatusCode(),
				"fail job %d: %s", job.Key, string(failResp.Body))
		}
	}
}

func waitForIncidents(t *testing.T, instanceKey int64, totalIncidents int) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := app.restClient.GetIncidentsWithResponse(t.Context(), instanceKey,
			&zenclient.GetIncidentsParams{})
		return err == nil && resp.JSON200 != nil && resp.JSON200.TotalCount >= totalIncidents
	}, 10*time.Second, 200*time.Millisecond,
		"all %d incidents should appear", totalIncidents)
}
