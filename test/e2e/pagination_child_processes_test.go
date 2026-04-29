package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestChildProcessesPagination(t *testing.T) {
	t.Parallel()
	const childCount = 11
	var subInstanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /process-instances/{key}/child-processes",
		Setup: func(t *testing.T) (cleanup func()) {
			def := deployChildProcessDefinitions(t)
			parentKey := createParentInstanceWithCollectionForChildProcesses(t, def.Key, childCount)
			waitForAllChildInstances(t, parentKey, childCount, &subInstanceKey)

			require.NotZero(t, subInstanceKey, "multiInstance sub-instance key must be set after children appear")

			return func() {
				app.restClient.CancelProcessInstanceWithResponse(t.Context(), parentKey) //nolint:errcheck
			}
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(),
				subInstanceKey,
				&zenclient.GetChildProcessInstancesParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
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
				TotalCount:     childCount,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func deployChildProcessDefinitions(t *testing.T) zenclient.ProcessDefinitionSimple {
	t.Helper()

	_, err := deployGetDefinition(t, "multi_instance_call_activity_process.bpmn", "Multi_Instance_Call_Activity_Process")
	require.NoError(t, err)

	def, err := deployGetUniqueDefinition(t, "pagination-test-parallel-call-activity.bpmn")
	require.NoError(t, err)
	require.NotZero(t, def.Key)
	return def
}

func createParentInstanceWithCollectionForChildProcesses(t *testing.T, defKey int64, childCount int) int64 {
	t.Helper()

	collection := make([]string, childCount)
	for i := range collection {
		collection[i] = "item"
	}
	instance, err := createProcessInstance(t, &defKey, map[string]any{
		"testInputCollection": collection,
	})
	require.NoError(t, err)
	return instance.Key
}

func waitForAllChildInstances(t *testing.T, parentKey int64, childCount int, subInstanceKey *int64) {
	t.Helper()

	require.Eventually(t, func() bool {
		if *subInstanceKey == 0 {
			resp, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(),
				parentKey, &zenclient.GetChildProcessInstancesParams{
					Size: ptr.To(int32(10)),
				})
			if err != nil || resp.JSON200 == nil {
				return false
			}
			for _, p := range resp.JSON200.Partitions {
				for _, child := range p.Items {
					if child.ProcessType == zenclient.ProcessInstanceProcessTypeMultiInstance {
						*subInstanceKey = child.Key
						break
					}
				}
			}
			if *subInstanceKey == 0 {
				return false
			}
		}
		resp2, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(),
			*subInstanceKey, &zenclient.GetChildProcessInstancesParams{
				Size: ptr.To(int32(100)),
			})
		if err != nil || resp2.JSON200 == nil {
			return false
		}
		count := 0
		for _, p := range resp2.JSON200.Partitions {
			count += len(p.Items)
		}
		return count >= childCount
	}, 10*time.Second, 200*time.Millisecond, "all %d child instances should appear", childCount)
}
