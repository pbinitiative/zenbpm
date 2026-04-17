package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestFlowElementHistoryPagination(t *testing.T) {
	t.Parallel()
	const jobsToComplete = 6
	// 2 entries per completed task (sequence flow + activation) + 3 fixed: StartEvent, flow to task 1, next task activation
	const totalHistoryEntries = 2*jobsToComplete + 3 // = 15
	var instanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-instances/{key}/history",
		Setup: func(t *testing.T) (cleanup func()) {
			instanceKey = deployAndCreateHistoryInstance(t)
			completeJobsSequentially(t, instanceKey, jobsToComplete, "TestType")
			waitForHistoryEntries(t, instanceKey, totalHistoryEntries)

			return func() {
				app.restClient.CancelProcessInstanceWithResponse(t.Context(), instanceKey) //nolint:errcheck
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetHistoryWithResponse(t.Context(), instanceKey,
				&zenclient.GetHistoryParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)

			count := 0
			if resp.JSON200.Items != nil {
				count = len(*resp.JSON200.Items)
			}
			return count, resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 5,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 5},
					{Page: 2, ExpectedCount: 5},
					{Page: 3, ExpectedCount: 5},
				},
				TotalCount:     totalHistoryEntries,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func deployAndCreateHistoryInstance(t *testing.T) int64 {
	t.Helper()

	def, err := deployGetUniqueDefinition(t, "long-task-chain.bpmn")
	require.NoError(t, err)
	require.NotZero(t, def.Key)

	instance, err := createProcessInstance(t, &def.Key, map[string]any{})
	require.NoError(t, err)
	return instance.Key
}

func completeJobsSequentially(t *testing.T, instanceKey int64, jobsToComplete int, jobType string) {
	t.Helper()

	for i := 0; i < jobsToComplete; i++ {
		var activeJob *zenclient.Job
		require.Eventually(t, func() bool {
			resp, err := app.restClient.GetJobsWithResponse(t.Context(),
				&zenclient.GetJobsParams{
					ProcessInstanceKey: &instanceKey,
					JobType:            &jobType,
				})
			if err != nil || resp.JSON200 == nil {
				return false
			}
			for _, p := range resp.JSON200.Partitions {
				for idx := range p.Items {
					job := p.Items[idx]
					if string(job.State) == "active" {
						activeJob = &job
						return true
					}
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond, "job %d should become active", i+1)

		err := completeJob(t, activeJob.Key, map[string]any{})
		require.NoError(t, err)
	}
}

func waitForHistoryEntries(t *testing.T, instanceKey int64, totalHistoryEntries int) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := app.restClient.GetHistoryWithResponse(t.Context(), instanceKey,
			&zenclient.GetHistoryParams{})
		return err == nil && resp.JSON200 != nil && resp.JSON200.TotalCount >= totalHistoryEntries
	}, 5*time.Second, 100*time.Millisecond,
		"history should have at least %d entries", totalHistoryEntries)
}
