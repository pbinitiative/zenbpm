package e2e

import (
	"encoding/json"
	"fmt"
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

func waitForProcessInstanceJobByElementId(t testing.TB, processInstanceKey int64, elementId string) public.Job {
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

func waitForProcessInstanceJobState(t testing.TB, processInstanceKey int64, jobKey int64, expectedState public.JobState) public.Job {
	t.Helper()

	var foundJob public.Job

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		for _, job := range jobs {
			if job.Key == jobKey {
				assert.Equal(collect, expectedState, job.State, "process instance %d job %s has unexpected state", processInstanceKey, jobKey)

				if job.State == expectedState {
					foundJob = job
				}
				return
			}
		}

		assert.Fail(collect, "job not found", "process instance %d does not expose job for key %s", processInstanceKey, jobKey)
	}, 5*time.Second, 100*time.Millisecond)

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
	job := waitForProcessInstanceJobByElementId(t, processInstanceKey, elementId)
	err := completeJob(t, job.Key, vars)
	require.NoError(t, err)
}
