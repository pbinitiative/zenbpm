package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getProcessInstanceJobs(t testing.TB, key int64) ([]public.Job, error) {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-instances/%d/jobs", key)).
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to read process instance jobs: %w", err)
	}
	jobPage := public.JobPage{}

	err = json.Unmarshal(resp, &jobPage)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job page: %w", err)
	}
	return jobPage.Items, nil
}

func waitForProcessInstanceActiveJobByElementId(t testing.TB, processInstanceKey int64, elementId string) public.Job {
	t.Helper()

	var foundJob public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.ElementId == elementId && job.State == public.JobStateActive {
				foundJob = job
				return true
			}
		}
		return false
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should expose active job for element %s", processInstanceKey, elementId)
	return foundJob
}

func waitForProcessInstanceJobByElementId(t testing.TB, processInstanceKey int64, elementId string, expectedState public.JobState) public.Job {
	t.Helper()

	var foundJob public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.ElementId == elementId && job.State == expectedState {
				foundJob = job
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "process instance %d should expose %s job for element %s", processInstanceKey, expectedState, elementId)

	return foundJob
}

func waitForProcessInstanceActiveJobsByElementId(t testing.TB, processInstanceKey int64, elementId string, expectedCount int) []public.Job {
	t.Helper()

	var foundJobs []public.Job
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		foundJobs = foundJobs[:0]
		for _, job := range jobs {
			if job.ElementId == elementId && job.State == public.JobStateActive {
				foundJobs = append(foundJobs, job)
			}
		}
		return len(foundJobs) == expectedCount
	}, 2*time.Second, 100*time.Millisecond,
		"process instance %d should expose %d active jobs for element %s", processInstanceKey, expectedCount, elementId)

	sort.Slice(foundJobs, func(i, j int) bool { return foundJobs[i].Key < foundJobs[j].Key })
	return foundJobs
}

func assertProcessInstanceHasNoActiveJobByElementId(t testing.TB, processInstanceKey int64, elementId string) {
	t.Helper()

	var requestErr error
	require.Never(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			requestErr = err
			return false
		}
		requestErr = nil
		for _, job := range jobs {
			if job.ElementId == elementId && job.State == public.JobStateActive {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 50*time.Millisecond,
		"process instance %d should not expose active job for element %s", processInstanceKey, elementId)
	require.NoError(t, requestErr)
}

func readWaitingJobs(t testing.TB, jobType string) (zenclient.JobPartitionPage, error) {
	return getJobs(t, zenclient.GetJobsParams{JobType: &jobType, State: ptr.To(zenclient.JobStateActive)})
}

func getJobs(t testing.TB, params zenclient.GetJobsParams) (zenclient.JobPartitionPage, error) {
	jobs, err := app.restClient.GetJobsWithResponse(t.Context(), &params)

	if err != nil {
		return zenclient.JobPartitionPage{}, fmt.Errorf("failed to get jobs: %w", err)
	}

	if jobs.StatusCode() != 200 {
		return zenclient.JobPartitionPage{}, fmt.Errorf("failed to get jobs: %s", jobs.Status())
	}

	return ptr.Deref(jobs.JSON200, zenclient.JobPartitionPage{}), nil
}

func completeJob(t testing.TB, jobKey int64, vars map[string]any) error {
	response, err := app.restClient.CompleteJobWithResponse(t.Context(), jobKey, zenclient.CompleteJobJSONRequestBody{
		Variables: &vars,
	})
	assert.NoError(t, err)
	if response.StatusCode() != 201 {
		return fmt.Errorf("status should be 201")
	}
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	return nil
}

func completeJobForElementId(t testing.TB, processInstanceKey int64, elementId string, vars map[string]any) {
	job := waitForProcessInstanceActiveJobByElementId(t, processInstanceKey, elementId)
	err := completeJob(t, job.Key, vars)
	require.NoError(t, err)
}

func completeJobsForElementIds(t testing.TB, processInstanceKey int64, elementIds ...string) {
	t.Helper()

	for _, elementId := range elementIds {
		completeJobForElementId(t, processInstanceKey, elementId, nil)
	}
}

func failJob(t testing.TB, jobKey int64, errorCode *string, vars map[string]any) {
	t.Helper()

	body := zenclient.FailJobJSONRequestBody{}
	if errorCode != nil {
		body.ErrorCode = errorCode
	}
	if vars != nil {
		body.Variables = &vars
	}

	response, err := app.restClient.FailJobWithResponse(t.Context(), jobKey, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
	require.Nil(t, response.JSON400)
	require.Nil(t, response.JSON502)
}

func failJobForElementId(t testing.TB, processInstanceKey int64, elementId string, errorCode *string, vars map[string]any) {
	job := waitForProcessInstanceActiveJobByElementId(t, processInstanceKey, elementId)
	failJob(t, job.Key, errorCode, vars)
}

func waitForExactlyOneActiveJobAmong(t testing.TB, processInstanceKey int64, elementIDs ...string) string {
	t.Helper()

	var activeElementIds []string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		activeElementIds = activeElementIds[:0]
		for _, job := range jobs {
			if job.State != public.JobStateActive {
				continue
			}
			for _, elementID := range elementIDs {
				if job.ElementId == elementID {
					activeElementIds = append(activeElementIds, elementID)
				}
			}
		}
		assert.Len(collect, activeElementIds, 1)
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should expose exactly one active job among %v", processInstanceKey, elementIDs)

	return activeElementIds[0]
}
