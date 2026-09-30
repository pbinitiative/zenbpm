package rest

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The requests below are refused before any node is asked, so the server needs none.

func TestFailJobRefusesNegativeRetries(t *testing.T) {
	server := &Server{}

	response, err := server.FailJob(t.Context(), public.FailJobRequestObject{
		JobKey: 1,
		Body:   &public.FailJobJSONRequestBody{Retries: new(int32(-1))},
	})

	require.NoError(t, err)
	refusal, ok := response.(public.FailJob400JSONResponse)
	require.True(t, ok, "expected 400, got %T", response)
	assert.Contains(t, refusal.Message, "retries must not be negative")
}

func TestFailJobRefusesABackoffWhichIsNoISODuration(t *testing.T) {
	server := &Server{}
	for _, backoff := range []string{"10s", "P1M", ""} {
		t.Run(backoff, func(t *testing.T) {
			response, err := server.FailJob(t.Context(), public.FailJobRequestObject{
				JobKey: 1,
				Body:   &public.FailJobJSONRequestBody{RetryBackoff: new(backoff)},
			})

			require.NoError(t, err)
			refusal, ok := response.(public.FailJob400JSONResponse)
			require.True(t, ok, "expected 400, got %T", response)
			assert.Contains(t, refusal.Message, "retryBackoff")
		})
	}
}

func TestFailJobValidatesTheBackoffNextToAnErrorCode(t *testing.T) {
	server := &Server{}

	response, err := server.FailJob(t.Context(), public.FailJobRequestObject{
		JobKey: 1,
		Body:   &public.FailJobJSONRequestBody{ErrorCode: new("PAYMENT_DECLINED"), RetryBackoff: new("10s")},
	})

	require.NoError(t, err)
	_, ok := response.(public.FailJob400JSONResponse)
	assert.True(t, ok, "an invalid field is refused even when the error code would ignore it, got %T", response)
}

func TestUpdateJobRetriesRefusesLessThanOne(t *testing.T) {
	server := &Server{}

	response, err := server.UpdateJobRetries(t.Context(), public.UpdateJobRetriesRequestObject{
		JobKey: 1,
		Body:   &public.UpdateJobRetriesJSONRequestBody{Retries: 0},
	})

	require.NoError(t, err)
	refusal, ok := response.(public.UpdateJobRetries400JSONResponse)
	require.True(t, ok, "expected 400, got %T", response)
	assert.Contains(t, refusal.Message, "at least 1")
}

func TestGetJobFailuresRefusesAPageBeyondTheLimit(t *testing.T) {
	server := &Server{}

	response, err := server.GetJobFailures(t.Context(), public.GetJobFailuresRequestObject{
		JobKey: 1,
		Params: public.GetJobFailuresParams{Size: new(int32(maxJobFailuresPageSize + 1))},
	})

	require.NoError(t, err)
	_, ok := response.(public.GetJobFailures400JSONResponse)
	assert.True(t, ok, "expected 400, got %T", response)
}

func TestJobCarriesItsRetryState(t *testing.T) {
	retryAt := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	job, err := (&Server{}).mapProtoJob(&proto.Job{
		Key:                new(int64(1)),
		State:              new(int64(runtime.ActivityStateActive)),
		Retries:            new(int32(2)),
		Attempts:           new(int32(1)),
		RetryAt:            new(retryAt.UnixMilli()),
		LastFailureMessage: new("payment service unavailable"),
		RetryBackoff:       new("PT10S,PT1M"),
		DeliveryToken:      new(int64(3)),
	})

	require.NoError(t, err)
	assert.Equal(t, new(int32(2)), job.Retries)
	assert.Equal(t, new(int32(1)), job.Attempts)
	require.NotNil(t, job.RetryAt)
	assert.True(t, retryAt.Equal(*job.RetryAt))
	assert.Equal(t, new("payment service unavailable"), job.LastFailureMessage)
	assert.Equal(t, new("PT10S,PT1M"), job.RetryBackoff)
	assert.Equal(t, new(int64(3)), job.DeliveryToken, "a REST client names it in its failure")

	deliverable, err := (&Server{}).mapProtoJob(&proto.Job{State: new(int64(runtime.ActivityStateActive))})
	require.NoError(t, err)
	assert.Nil(t, deliverable.RetryAt, "a job not waiting out a backoff has no retryAt")
}
