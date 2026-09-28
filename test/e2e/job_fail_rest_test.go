package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/require"
)

func TestRestJobFailErrorResponse(t *testing.T) {
	t.Run("job not found should return 404", func(t *testing.T) {
		partitionId := int64(1)
		nonExistingJobKey := partitionId << int64(zenflake.StepBits)

		body := zenclient.FailJobJSONRequestBody{}

		response, err := app.restClient.FailJobWithResponse(t.Context(), nonExistingJobKey, body)

		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON404)
		require.Equal(t, "NOT_FOUND", response.JSON404.Code)
		require.Contains(t, response.JSON404.Message, "not found")
		require.Nil(t, response.JSON400)
		require.Nil(t, response.JSON502)
	})

	t.Run("a job which no longer waits for a worker answers 409", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		// the model names no retries, so the first failure exhausts them
		failJob(t, job.Key, nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		response, err := app.restClient.FailJobWithResponse(t.Context(), job.Key, zenclient.FailJobJSONRequestBody{Message: new("repeated after a timeout")})

		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON409)
		require.Contains(t, response.JSON409.Message, "already failed")
	})
}

// TestRestJobFailOfAStreamDeliveredJobHonoursTheBackoff shows a worker which
// receives its jobs over the stream but fails them over REST gets a job it
// retries back once the backoff has passed, not once its lock has lapsed, when
// it names its stream's client id.
func TestRestJobFailOfAStreamDeliveredJobHonoursTheBackoff(t *testing.T) {
	jobType := fmt.Sprintf("rest-fail-stream-%d", rand.Int63())
	clientID := jobType + "-worker"
	zenClient := newLockTestGrpcClient(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var deliveries atomic.Int32
	failed := make(chan error, 1)
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), clientID,
		func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			if deliveries.Add(1) > 1 {
				return nil, nil
			}
			response, err := app.restClient.FailJobWithResponse(ctx, job.GetKey(), zenclient.FailJobJSONRequestBody{
				ClientId:     new(clientID),
				Message:      new("payment service unavailable"),
				RetryBackoff: new("PT0S"),
			})
			if err == nil && response.StatusCode() != http.StatusNoContent {
				err = fmt.Errorf("unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
			}
			failed <- err
			// the worker reported over REST, so this delivery sends nothing more
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(time.Minute)))
	require.NoError(t, err)
	definition, err := deployDefinitionWithJobType(t, "job_retries/service-task-retries.bpmn", jobType, map[string]string{"charge-card": jobType})
	require.NoError(t, err)
	instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, nil)
	require.NoError(t, err)

	select {
	case err := <-failed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the worker must receive the job")
	}

	require.Eventually(t, func() bool {
		return deliveries.Load() >= 2
	}, 10*time.Second, 50*time.Millisecond, "the job comes back at once, not when the lock of a minute lapses")
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateCompleted)
}
