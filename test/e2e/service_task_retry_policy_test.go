package e2e

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskRetryBackoffDelaysRedelivery(t *testing.T) {
	instance, jobType := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	deliveries := &retryDeliveries{}
	registerRetryWorker(t, jobType, deliveries, func(attempt int32) *zenclient.WorkerError {
		if attempt == 1 {
			return &zenclient.WorkerError{Err: errors.New("service down"), RetryBackoff: new(2 * time.Second)}
		}
		return nil
	})

	require.Never(t, func() bool {
		return deliveries.count() > 1
	}, 1500*time.Millisecond, 50*time.Millisecond, "the job must not be handed out again during its backoff")
	require.Eventually(t, func() bool {
		return deliveries.count() == 2
	}, 10*time.Second, 50*time.Millisecond, "the job must be handed out again once its backoff has passed")
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

	first, second := deliveries.get(0), deliveries.get(1)
	assert.GreaterOrEqual(t, second.at.Sub(first.at), 2*time.Second)
	assert.Equal(t, int32(1), first.attempt)
	assert.Equal(t, int32(3), first.retries)
	assert.Equal(t, int32(2), second.attempt)
	assert.Equal(t, int32(2), second.retries, "the delivery carries the retries the job has left")
}

func TestDefinitionBackoffPolicyDelaysRedelivery(t *testing.T) {
	instance, jobType := startRetryFixture(t, "testdata/service_task/service_task_retry_policy.bpmn", nil)
	deliveries := &retryDeliveries{}
	registerRetryWorker(t, jobType, deliveries, func(attempt int32) *zenclient.WorkerError {
		if attempt < 3 {
			// no backoff named: the task definition's PT1S,PT3S applies
			return &zenclient.WorkerError{Err: errors.New("service down")}
		}
		return nil
	})

	require.Eventually(t, func() bool {
		return deliveries.count() == 3
	}, 15*time.Second, 50*time.Millisecond, "the job must be handed out three times")
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

	firstGap := deliveries.get(1).at.Sub(deliveries.get(0).at)
	secondGap := deliveries.get(2).at.Sub(deliveries.get(1).at)
	assert.GreaterOrEqual(t, firstGap, time.Second, "the first failure waits the first entry")
	assert.Less(t, firstGap, 3*time.Second)
	assert.GreaterOrEqual(t, secondGap, 3*time.Second, "the second failure waits the second entry")
	assert.Less(t, secondGap, 6*time.Second)
	for i := range 3 {
		assert.Equal(t, int32(i+1), deliveries.get(i).attempt, "delivery %d", i)
	}
}

func TestStreamFailuresExhaustTheRetries(t *testing.T) {
	instance, jobType := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	deliveries := &retryDeliveries{}
	registerRetryWorker(t, jobType, deliveries, func(int32) *zenclient.WorkerError {
		return &zenclient.WorkerError{Err: errors.New("service down for good")}
	})

	failed := waitForProcessInstanceJobByElementId(t, instance.Key, "retried-task", public.JobStateFailed)

	assert.Equal(t, 3, deliveries.count(), "three attempts, then the incident")
	assert.Equal(t, new(int32(3)), failed.Attempts)
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, &failed.Key, incidents[0].JobKey)
	assert.Contains(t, incidents[0].Message, "3 attempts, retries exhausted")
	response, err := app.restClient.GetJobFailuresWithResponse(t.Context(), failed.Key, &zenclient.GetJobFailuresParams{})
	require.NoError(t, err)
	require.NotNil(t, response.JSON200, "body: %s", string(response.Body))
	require.Len(t, response.JSON200.Items, 3)
	assert.Equal(t, &incidents[0].Key, response.JSON200.Items[0].IncidentKey, "the last failure names the incident it created")
	assert.Nil(t, response.JSON200.Items[1].IncidentKey)
}

func TestUpdatedRetriesMakeAJobInBackoffDeliverable(t *testing.T) {
	instance, jobType := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	deliveries := &retryDeliveries{}
	registerRetryWorker(t, jobType, deliveries, func(attempt int32) *zenclient.WorkerError {
		if attempt == 1 {
			return &zenclient.WorkerError{Err: errors.New("service down"), RetryBackoff: new(time.Hour)}
		}
		return nil
	})
	require.Eventually(t, func() bool {
		return deliveries.count() == 1
	}, 10*time.Second, 50*time.Millisecond, "the worker must receive the job")
	jobKey := waitForJobInBackoff(t, instance.Key)
	require.Never(t, func() bool {
		return deliveries.count() > 1
	}, time.Second, 50*time.Millisecond, "the job waits out its hour of backoff")

	response, err := app.restClient.UpdateJobRetriesWithResponse(t.Context(), jobKey, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "body: %s", string(response.Body))

	require.Eventually(t, func() bool {
		return deliveries.count() == 2
	}, 10*time.Second, 50*time.Millisecond, "the update makes the job deliverable at once")
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	assert.Equal(t, int32(2), deliveries.get(1).attempt)
	assert.Equal(t, int32(2), deliveries.get(1).retries, "the delivery carries the retries the operator set")
}

func TestLapsedLockRedeliveryKeepsTheAttempt(t *testing.T) {
	instance, jobType := startRetryFixture(t, "testdata/service_task/service_task_retries.bpmn", nil)
	deliveries := &retryDeliveries{}
	client := newLockTestGrpcClient(t)
	for _, clientID := range []string{jobType + "-worker-a", jobType + "-worker-b"} {
		_, err := client.RegisterWorkerWithOptions(t.Context(), clientID,
			func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
				deliveries.add(job)
				<-ctx.Done()
				return nil, nil
			}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(time.Second)))
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return deliveries.count() == 2
	}, 10*time.Second, 50*time.Millisecond, "the job must be delivered again once the lock lapsed")

	assert.Equal(t, int32(1), deliveries.get(0).attempt)
	assert.Equal(t, int32(1), deliveries.get(1).attempt, "a lapsed lock is a redelivery of the same attempt, not a retry")
	assert.Equal(t, int32(3), deliveries.get(1).retries, "a lapsed lock spends no retry")
	job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, "retried-task")
	require.NoError(t, completeJob(t, job.Key, nil))
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

type retryDelivery struct {
	at      time.Time
	attempt int32
	retries int32
}

// retryDeliveries records every delivery of a job to a retry worker.
type retryDeliveries struct {
	mu    sync.Mutex
	items []retryDelivery
}

func (d *retryDeliveries) add(job *proto.WaitingJob) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items = append(d.items, retryDelivery{at: time.Now(), attempt: job.GetAttempt(), retries: job.GetRetries()})
}

func (d *retryDeliveries) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

func (d *retryDeliveries) get(index int) retryDelivery {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.items[index]
}

// registerRetryWorker subscribes a worker to jobType which records every
// delivery and answers it with what outcome returns for its attempt: a worker
// error, or nil to complete the job.
func registerRetryWorker(t *testing.T, jobType string, deliveries *retryDeliveries, outcome func(attempt int32) *zenclient.WorkerError) {
	t.Helper()
	client := newLockTestGrpcClient(t)
	_, err := client.RegisterWorkerWithOptions(t.Context(), jobType+"-worker",
		func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			deliveries.add(job)
			if workerErr := outcome(job.GetAttempt()); workerErr != nil {
				return nil, workerErr
			}
			return map[string]any{}, nil
		}, zenclient.WithJobType(jobType))
	require.NoError(t, err)
}

// waitForJobInBackoff waits until the instance's retried task has a job
// waiting out a backoff and returns its key.
func waitForJobInBackoff(t *testing.T, processInstanceKey int64) int64 {
	t.Helper()
	var jobKey int64
	require.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, job := range jobs {
			if job.ElementId == "retried-task" && job.RetryAt != nil {
				jobKey = job.Key
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "the job must wait out a backoff")
	return jobKey
}
