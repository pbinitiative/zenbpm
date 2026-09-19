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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestJobExtendLock extends the lock of a job delivered over the gRPC job
// stream through the REST API, with the stream's client id and with others.
func TestRestJobExtendLock(t *testing.T) {
	jobType := fmt.Sprintf("rest-extend-lock-%d", rand.Int63())
	clientID := jobType + "-worker"
	zenClient := newLockTestGrpcClient(t)
	release := make(chan struct{})
	var heldKey atomic.Int64
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), clientID,
		func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			heldKey.Store(job.GetKey())
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}, zenclient.WithJobType(jobType))
	require.NoError(t, err)
	instance := deployAndStartLockTestInstance(t, jobType)
	require.Eventually(t, func() bool {
		return heldKey.Load() != 0
	}, 10*time.Second, 50*time.Millisecond, "the worker must receive the job")
	jobKey := heldKey.Load()

	t.Run("the stream's client extends by an explicit duration", func(t *testing.T) {
		before := time.Now()
		response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey, zenclient.ExtendJobLockJSONRequestBody{
			ClientId:     clientID,
			LockDuration: new("PT5M"),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON200)
		assert.WithinRange(t, response.JSON200.LockUntil, before.Add(5*time.Minute).Add(-time.Second), time.Now().Add(5*time.Minute).Add(time.Second))
	})

	t.Run("the stream's client extends by the subscription's duration", func(t *testing.T) {
		before := time.Now()
		response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey, zenclient.ExtendJobLockJSONRequestBody{
			ClientId: clientID,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON200)
		assert.WithinRange(t, response.JSON200.LockUntil, before.Add(30*time.Second).Add(-time.Second), time.Now().Add(30*time.Second).Add(time.Second),
			"without a duration the engine default of thirty seconds applies")
	})

	t.Run("another client id is refused with 409", func(t *testing.T) {
		response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey, zenclient.ExtendJobLockJSONRequestBody{
			ClientId: clientID + "-other",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON409)
		assert.Equal(t, "CONFLICT", response.JSON409.Code)
		assert.Contains(t, response.JSON409.Message, "another client")
	})

	t.Run("a malformed duration is refused with 400", func(t *testing.T) {
		response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), jobKey, zenclient.ExtendJobLockJSONRequestBody{
			ClientId:     clientID,
			LockDuration: new("5 minutes"),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON400)
		assert.Contains(t, response.JSON400.Message, "ISO-8601")
	})

	t.Run("an unknown job is refused with 404", func(t *testing.T) {
		unknownKey := int64(1) << int64(zenflake.StepBits)
		response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), unknownKey, zenclient.ExtendJobLockJSONRequestBody{
			ClientId: clientID,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON404)
		assert.Equal(t, "NOT_FOUND", response.JSON404.Code)
	})

	release <- struct{}{}
	waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)
}

// TestRestJobExtendLockOfUnlockedJob shows a job nobody received over the
// stream holds no lock, so there is nothing to extend.
func TestRestJobExtendLockOfUnlockedJob(t *testing.T) {
	jobType := fmt.Sprintf("rest-extend-unlocked-%d", rand.Int63())
	instance := deployAndStartLockTestInstance(t, jobType)
	job := waitForActiveJobByType(t, jobType)

	response, err := app.restClient.ExtendJobLockWithResponse(t.Context(), job.Key, zenclient.ExtendJobLockJSONRequestBody{
		ClientId: jobType + "-rest-poller",
	})

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode(), "unexpected response: %s body: %s", response.Status(), string(response.Body))
	require.NotNil(t, response.JSON409)
	assert.Equal(t, "CONFLICT", response.JSON409.Code)
	assert.Contains(t, response.JSON409.Message, "not held")

	// nobody works the job, so the test completes it and leaves no active
	// instance behind in the shared node
	require.NoError(t, completeJob(t, job.Key, nil))
	waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)
}
