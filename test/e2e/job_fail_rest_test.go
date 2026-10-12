package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
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

// TestARepeatedFailureOfAnEarlierDeliveryLeavesTheNextDeliveryWithItsWorker
// shows a worker which repeats the failure of a delivery the engine recorded
// already, as it does after an answer which did not confirm it, keeps the job
// it got back meanwhile: the repeat changes nothing, the worker still holds the
// lock of the next delivery, and the job is not handed out alongside.
func TestARepeatedFailureOfAnEarlierDeliveryLeavesTheNextDeliveryWithItsWorker(t *testing.T) {
	jobType := fmt.Sprintf("repeated-failure-%d", rand.Int63())
	clientID := jobType + "-worker"
	zenClient := newLockTestGrpcClient(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var deliveries atomic.Int32
	var firstDelivery atomic.Int64
	var secondDeliveryJobKey atomic.Int64
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), clientID,
		func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			if deliveries.Add(1) == 1 {
				firstDelivery.Store(job.GetDeliveryToken())
				return nil, &zenclient.WorkerError{Err: errors.New("payment service unavailable"), RetryBackoff: new(time.Duration(0))}
			}
			// the second delivery is still being worked on when the repeat arrives
			secondDeliveryJobKey.Store(job.GetKey())
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
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, instance.Key)
	})
	require.Eventually(t, func() bool {
		return secondDeliveryJobKey.Load() != 0
	}, 10*time.Second, 50*time.Millisecond, "the worker must receive the second delivery")
	jobKey := secondDeliveryJobKey.Load()

	failJobWithRetryRequest(t, jobKey, zenclient.FailJobJSONRequestBody{
		ClientId:      new(clientID),
		Message:       new("payment service unavailable"),
		DeliveryToken: new(firstDelivery.Load()),
	})

	require.Equal(t, new(int32(1)), getJob(t, jobKey).Attempts, "the repeat changes nothing")
	extended, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey, zenclient.ExtendJobLockJSONRequestBody{ClientId: clientID})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, extended.StatusCode(), "the worker still holds the lock of the second delivery, body: %s", string(extended.Body))
	require.Never(t, func() bool {
		return deliveries.Load() > 2
	}, 500*time.Millisecond, 20*time.Millisecond, "the second delivery is not handed out alongside")
}

// TestALateFailureOfALapsedDeliveryLeavesTheJobToTheNextDelivery shows the
// failure of a worker which worked past its lock, reported after the job was
// handed out again, is refused with 409 and changes nothing: although it asks
// for no retries left, it neither creates an incident nor spends an attempt
// while the next delivery runs, which then decides the outcome of the job. An
// extension naming the lapsed delivery is refused as well, although the next
// delivery went to the same client.
func TestALateFailureOfALapsedDeliveryLeavesTheJobToTheNextDelivery(t *testing.T) {
	jobType := fmt.Sprintf("lapsed-delivery-%d", rand.Int63())
	clientID := jobType + "-worker"
	zenClient := newLockTestGrpcClient(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var lapsedDelivery, nextDelivery, jobKey atomic.Int64
	secondDelivery := make(chan struct{})
	secondDelivered := sync.Once{}
	finish := make(chan struct{})
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), clientID,
		func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			if lapsedDelivery.CompareAndSwap(0, job.GetDeliveryToken()) {
				// works past its lock and never reports; the test reports late for it
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil, nil
			}
			secondDelivered.Do(func() {
				jobKey.Store(job.GetKey())
				nextDelivery.Store(job.GetDeliveryToken())
				close(secondDelivery)
			})
			select {
			case <-finish:
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(2*time.Second)))
	require.NoError(t, err)
	definition, err := deployDefinitionWithJobType(t, "job_retries/service-task-retries.bpmn", jobType, map[string]string{"charge-card": jobType})
	require.NoError(t, err)
	instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, instance.Key)
	})
	select {
	case <-secondDelivery:
	case <-time.After(15 * time.Second):
		require.FailNow(t, "the job must come back once the lock of the first delivery lapsed")
	}
	extended, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey.Load(), zenclient.ExtendJobLockJSONRequestBody{
		ClientId: clientID, LockDuration: new("PT1M"), DeliveryToken: new(nextDelivery.Load()),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, extended.StatusCode(), "body: %s", string(extended.Body))
	require.Greater(t, nextDelivery.Load(), lapsedDelivery.Load())
	lapsedExtension, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey.Load(), zenclient.ExtendJobLockJSONRequestBody{
		ClientId: clientID, DeliveryToken: new(lapsedDelivery.Load()),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, lapsedExtension.StatusCode(), "the lapsed delivery holds no lock, body: %s", string(lapsedExtension.Body))

	response, err := app.restClient.FailJobWithResponse(t.Context(), jobKey.Load(), zenclient.FailJobJSONRequestBody{
		ClientId:      new(clientID),
		Message:       new("timed out, much too late"),
		Retries:       new(int32(0)),
		DeliveryToken: new(lapsedDelivery.Load()),
	})

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode(), "body: %s", string(response.Body))
	require.NotNil(t, response.JSON409)
	require.Contains(t, response.JSON409.Message, "handed out again")
	running := getJob(t, jobKey.Load())
	require.Equal(t, zenclient.JobStateActive, running.State)
	require.Equal(t, new(int32(0)), running.Attempts)
	require.Equal(t, new(int32(3)), running.Retries)
	assertProcessInstanceIncidentsLength(t, instance.Key, 0)
	close(finish)
	waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateCompleted)
}
