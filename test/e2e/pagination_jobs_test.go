package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestGlobalJobsPagination(t *testing.T) {
	t.Parallel()
	const paginationTestJobType = "pagination-test-job"
	var instanceKeys []int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /jobs",
		Setup: func(t *testing.T) (cleanup func()) {
			defKey := deployJobDefinition(t, "pagination-test-service-task.bpmn")
			instanceKeys = createInstances(t, defKey, 11)
			waitForGlobalJobsCount(t, paginationTestJobType, 11)

			return func() {
				for _, key := range instanceKeys {
					app.restClient.CancelProcessInstanceWithResponse(t.Context(), key) //nolint:errcheck
				}
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			jobType := paginationTestJobType
			resp, err := app.restClient.GetJobsWithResponse(t.Context(),
				&zenclient.GetJobsParams{
					JobType: &jobType,
					Page:    ptr.To(int32(page)),
					Size:    ptr.To(int32(size)),
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
			{
				PageSize: 10,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 10},
					{Page: 2, ExpectedCount: 1},
				},
				TotalCount:     11,
				TotalCountMode: ExactCount,
			},
			{
				PageSize: 3,
				Pages: []PageExpectation{
					{Page: 4, ExpectedCount: 2},
				},
				TotalCount:     11,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func TestProcessInstanceJobsPagination(t *testing.T) {
	t.Parallel()
	const jobCount = 11
	var subInstanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-instances/{key}/jobs",
		Setup: func(t *testing.T) (cleanup func()) {
			defKey := deployJobDefinition(t, "pagination-test-parallel-service-task.bpmn")
			parentKey := createParentInstanceWithCollection(t, defKey, jobCount)
			waitForSubInstanceJobs(t, "pagination-parallel-job", jobCount, &subInstanceKey)

			require.NotZero(t, subInstanceKey, "sub-instance key must be set after jobs appear")

			return func() {
				app.restClient.CancelProcessInstanceWithResponse(t.Context(), parentKey) //nolint:errcheck
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetProcessInstanceJobsWithResponse(t.Context(),
				subInstanceKey,
				&zenclient.GetProcessInstanceJobsParams{
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
					{Page: 3, ExpectedCount: 1},
				},
				TotalCount:     jobCount,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func deployJobDefinition(t *testing.T, resource string) int64 {
	t.Helper()

	def, err := deployGetUniqueDefinition(t, resource)
	require.NoError(t, err)
	require.NotZero(t, def.Key)
	return def.Key
}

func createInstances(t *testing.T, defKey int64, count int) []int64 {
	t.Helper()

	var keys []int64
	for i := 0; i < count; i++ {
		instance, err := createProcessInstance(t, &defKey, map[string]any{})
		require.NoError(t, err)
		keys = append(keys, instance.Key)
	}
	return keys
}

func waitForGlobalJobsCount(t *testing.T, jobType string, required int) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := app.restClient.GetJobsWithResponse(t.Context(),
			&zenclient.GetJobsParams{
				JobType: &jobType,
				Size:    ptr.To(int32(100)),
			})
		if err != nil || resp.JSON200 == nil {
			return false
		}
		count := 0
		for _, p := range resp.JSON200.Partitions {
			count += len(p.Items)
		}
		return count >= required
	}, 10*time.Second, 200*time.Millisecond, "all %d jobs of type %q should appear", required, jobType)
}

func createParentInstanceWithCollection(t *testing.T, defKey int64, jobCount int) int64 {
	t.Helper()

	collection := make([]string, jobCount)
	for i := range collection {
		collection[i] = "item"
	}
	instance, err := createProcessInstance(t, &defKey, map[string]any{
		"testInputCollection": collection,
	})
	require.NoError(t, err)
	return instance.Key
}

func waitForSubInstanceJobs(t *testing.T, jobType string, jobCount int, subInstanceKey *int64) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := app.restClient.GetJobsWithResponse(t.Context(),
			&zenclient.GetJobsParams{
				JobType: &jobType,
				Size:    ptr.To(int32(100)),
			})
		if err != nil || resp.JSON200 == nil {
			return false
		}
		count := 0
		for _, p := range resp.JSON200.Partitions {
			for _, j := range p.Items {
				if *subInstanceKey == 0 {
					*subInstanceKey = j.ProcessInstanceKey
				}
				if j.ProcessInstanceKey == *subInstanceKey {
					count++
				}
			}
		}
		return count >= jobCount
	}, 10*time.Second, 200*time.Millisecond, "all %d jobs should appear", jobCount)
}
