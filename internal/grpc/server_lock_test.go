package grpc

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecvClientRequestsPassesSubscriptionSettingsOn(t *testing.T) {
	manager := &jobStreamTestManager{}
	stream := newJobStreamTestServer(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				Type:           proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE.Enum(),
				JobType:        new("job-a"),
				LockDurationMs: new(int64(5000)),
				MaxActiveJobs:  new(int32(3)),
			},
		},
	})
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, manager.subscribedSettings, 1)
	assert.Equal(t, jobmanager.SubscriptionSettings{LockDuration: 5 * time.Second, MaxActiveJobs: 3}, manager.subscribedSettings[0])
}

func TestRecvClientRequestsSaturatesUnrepresentableDurations(t *testing.T) {
	manager := &jobStreamTestManager{}
	stream := newJobStreamTestServer(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				Type:           proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE.Enum(),
				JobType:        new("job-a"),
				LockDurationMs: new(int64(math.MaxInt64)),
			},
		},
	}, extendLockRequest(7, 0))
	stream.requests[1].GetExtendLock().LockDurationMs = new(int64(math.MaxInt64))
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, manager.subscribedSettings, 1)
	assert.Equal(t, time.Duration(math.MaxInt64), manager.subscribedSettings[0].LockDuration, "a huge request stays huge for the leader to cap, it does not wrap to a default request")
	require.Len(t, manager.extendedDurations, 1)
	assert.Equal(t, time.Duration(math.MaxInt64), manager.extendedDurations[0])
}

// TestRecvClientRequestsPassesTheDeliveryOfALockExtensionOn shows the delivery
// an extension names reaches the job manager, and stays absent when it names none.
func TestRecvClientRequestsPassesTheDeliveryOfALockExtensionOn(t *testing.T) {
	manager := &jobStreamTestManager{}
	named := &proto.JobStreamRequest{Request: &proto.JobStreamRequest_ExtendLock{ExtendLock: &proto.JobExtendLockRequest{
		Key:           new(int64(7)),
		DeliveryToken: new(int64(3)),
	}}}
	stream := newJobStreamTestServer(named, extendLockRequest(7, 0))
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, manager.extendedDeliveries, 2)
	assert.Equal(t, new(int64(3)), manager.extendedDeliveries[0])
	assert.Nil(t, manager.extendedDeliveries[1])
}

func TestRecvClientRequestsAnswersLockExtensionWithTheDeadline(t *testing.T) {
	lockUntil := time.Now().Add(42 * time.Second).Truncate(time.Millisecond)
	manager := &jobStreamTestManager{extendLockUntil: lockUntil}
	stream := newJobStreamTestServer(extendLockRequest(7, 2*time.Second))
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	assert.Equal(t, []int64{7}, manager.extendedKeys)
	assert.Equal(t, []time.Duration{2 * time.Second}, manager.extendedDurations)
	require.Len(t, stream.sent, 1)
	assert.Nil(t, stream.sent[0].Error)
	require.NotNil(t, stream.sent[0].LockExtended)
	assert.Equal(t, int64(7), stream.sent[0].LockExtended.GetKey())
	assert.Equal(t, lockUntil.UnixMilli(), stream.sent[0].LockExtended.GetLockUntil())
}

func TestRecvClientRequestsReportsLockExtensionRefusalByCode(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode proto.JobStreamErrorCode
	}{
		{"not held", jobmanager.ErrLockNotHeld, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD},
		{"held by other client", jobmanager.ErrLockHeldByOtherClient, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_HELD_BY_OTHER_CLIENT},
		{"leader unavailable", fmt.Errorf("%w: node-42 at 10.0.0.1 does not lead the partition", jobmanager.ErrLeaderUnavailable), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE},
		{"anything else", errors.New("node-42 at 10.0.0.1 refused the request"), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &jobStreamTestManager{extendErr: tt.err}
			stream := newJobStreamTestServer(extendLockRequest(7, 0))
			server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

			server.recvClientRequests(stream, "client-1", &sync.Mutex{})

			require.Len(t, stream.sent, 1)
			require.NotNil(t, stream.sent[0].Error)
			assert.Equal(t, uint32(tt.expectedCode), stream.sent[0].Error.GetCode())
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "10.0.0.1", "internal details never reach the client")
			require.NotNil(t, stream.sent[0].LockExtended, "the refusal names the job key so the client can match it")
			assert.Equal(t, int64(7), stream.sent[0].LockExtended.GetKey())
			assert.Zero(t, stream.sent[0].LockExtended.GetLockUntil())
		})
	}
}

func TestSendClientJobsCarriesLockUntil(t *testing.T) {
	stream := newJobStreamTestServer()
	server := &Server{ctx: t.Context(), logger: hclog.NewNullLogger()}
	clientCh := make(chan jobmanager.Job, 1)
	recvDone := make(chan struct{})
	clientCh <- jobmanager.Job{Key: 7, Type: "job-a", LockUntil: 1234567}
	close(clientCh)

	server.sendClientJobs(stream, clientCh, recvDone, &sync.Mutex{})

	require.Len(t, stream.sent, 1)
	require.NotNil(t, stream.sent[0].Job)
	assert.Equal(t, int64(1234567), stream.sent[0].Job.GetLockUntil())
}

func TestSendClientJobsCarriesRetriesAttemptAndDeliveryToken(t *testing.T) {
	stream := newJobStreamTestServer()
	server := &Server{ctx: t.Context(), logger: hclog.NewNullLogger()}
	clientCh := make(chan jobmanager.Job, 1)
	recvDone := make(chan struct{})
	clientCh <- jobmanager.Job{Key: 7, Type: "job-a", Retries: 2, Attempt: 3, DeliveryToken: 5}
	close(clientCh)

	server.sendClientJobs(stream, clientCh, recvDone, &sync.Mutex{})

	require.Len(t, stream.sent, 1)
	assert.Equal(t, int32(2), stream.sent[0].Job.GetRetries())
	assert.Equal(t, int32(3), stream.sent[0].Job.GetAttempt())
	assert.Equal(t, int64(5), stream.sent[0].Job.GetDeliveryToken())
}

// TestRecvClientRequestsPassesRetriesAndBackoffOn shows a failure's retries,
// backoff and delivery token reach the job manager as sent, and stay absent when a
// client, such as one built before retries existed, sends none.
func TestRecvClientRequestsPassesRetriesAndBackoffOn(t *testing.T) {
	key := int64(42)
	withRetries := &proto.JobStreamRequest{Request: &proto.JobStreamRequest_Fail{Fail: &proto.JobFailRequest{
		Key:            &key,
		Retries:        new(int32(3)),
		RetryBackoffMs: new(int64(2500)),
		DeliveryToken:  new(int64(2)),
	}}}
	manager := &jobStreamTestManager{}
	stream := newJobStreamTestServer(withRetries, failRequest(nil))
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, manager.failedRetries, 2)
	assert.Equal(t, new(int32(3)), manager.failedRetries[0])
	assert.Equal(t, new(2500*time.Millisecond), manager.failedBackoffs[0])
	assert.Equal(t, new(int64(2)), manager.failedDeliveries[0])
	assert.Nil(t, manager.failedRetries[1])
	assert.Nil(t, manager.failedBackoffs[1])
	assert.Nil(t, manager.failedDeliveries[1])
}

func TestRecvClientRequestsReportsWhyAFailureWasNotRecordedByCode(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode proto.JobStreamErrorCode
	}{
		{"invalid request", fmt.Errorf("%w: retries of job 42 must not be negative, got -1", jobmanager.ErrInvalidJobRequest), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_INVALID_REQUEST},
		{"unknown job", fmt.Errorf("%w: job 42 not found", jobmanager.ErrJobNotFound), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND},
		{"job no longer waits", fmt.Errorf("%w: job 42 is already completed", jobmanager.ErrJobInTerminalState), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_IN_TERMINAL_STATE},
		{"leader unavailable", fmt.Errorf("%w: node-42 at 10.0.0.1 does not lead the partition", jobmanager.ErrLeaderUnavailable), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE},
		{"anything else", errors.New("node-42 at 10.0.0.1 refused the request"), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &jobStreamTestManager{failErr: tt.err}
			stream := newJobStreamTestServer(failRequest(nil))
			server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

			server.recvClientRequests(stream, "client-1", &sync.Mutex{})

			require.Len(t, stream.sent, 1)
			require.NotNil(t, stream.sent[0].Error)
			assert.Equal(t, uint32(tt.expectedCode), stream.sent[0].Error.GetCode())
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "10.0.0.1", "internal details never reach the client")
			require.NotNil(t, stream.sent[0].Job, "the error names the job key so the client can match it")
			assert.Equal(t, int64(42), stream.sent[0].Job.GetKey())
		})
	}
}

// TestRecvClientRequestsReportsWhyACompletionWasNotRecordedByCode shows a
// completion is answered with the same codes as a failure.
func TestRecvClientRequestsReportsWhyACompletionWasNotRecordedByCode(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		expectedCode    proto.JobStreamErrorCode
		expectedMessage string
	}{
		{"job no longer waits", fmt.Errorf("%w: job 42 is already terminated", jobmanager.ErrJobInTerminalState), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_IN_TERMINAL_STATE, "The job no longer waits for a worker: it was terminated or failed"},
		{"unknown job", fmt.Errorf("%w: job 42 not found", jobmanager.ErrJobNotFound), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND, "Job not found"},
		{"leader unavailable", fmt.Errorf("%w: node-42 at 10.0.0.1 does not lead the partition", jobmanager.ErrLeaderUnavailable), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE, "The leader of the job's partition is unavailable at the moment; the completion may have been recorded, and repeating it is safe"},
		{"invalid request", &jobmanager.InvalidJobRequestError{Reason: "variables of job 42 exceed the limit"}, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_INVALID_REQUEST, "Invalid job completion request: variables of job 42 exceed the limit"},
		{"anything else", errors.New("node-42 at 10.0.0.1 refused the request"), proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_UNSPECIFIED, "Failed to complete job"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &jobStreamTestManager{completeErr: tt.err}
			stream := newJobStreamTestServer(completeRequest(nil))
			server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

			server.recvClientRequests(stream, "client-1", &sync.Mutex{})

			require.Len(t, stream.sent, 1)
			require.NotNil(t, stream.sent[0].Error)
			assert.Equal(t, uint32(tt.expectedCode), stream.sent[0].Error.GetCode())
			assert.Equal(t, tt.expectedMessage, stream.sent[0].Error.GetMessage())
			require.NotNil(t, stream.sent[0].Job, "the error names the job key so the client can match it")
		})
	}
}

// TestRecvClientRequestsCodesUndecodableCompletionVariablesAsAnInvalidRequest
// shows completion variables which do not decode carry the invalid-request
// code like failure variables do.
func TestRecvClientRequestsCodesUndecodableCompletionVariablesAsAnInvalidRequest(t *testing.T) {
	stream := newJobStreamTestServer(completeRequest([]byte(`{"broken":`)))
	server := &Server{jobManager: &jobStreamTestManager{}, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, stream.sent, 1)
	assert.Equal(t, uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_INVALID_REQUEST), stream.sent[0].Error.GetCode())
}

// TestRecvClientRequestsPassesTheEngineReasonOfAnInvalidRequestOn shows the
// worker is told what was wrong with its request, not a fixed sentence which
// fits only one of the refusals.
func TestRecvClientRequestsPassesTheEngineReasonOfAnInvalidRequestOn(t *testing.T) {
	// the layers on the way wrap the refusal, and the reason itself may read like a wrapping
	refusal := fmt.Errorf("failed to fail job 42: %w", &jobmanager.InvalidJobRequestError{
		Reason: "retry backoff of job 42 must not be negative: invalid job request: got -1s",
	})
	manager := &jobStreamTestManager{failErr: refusal}
	stream := newJobStreamTestServer(failRequest(nil))
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, stream.sent, 1)
	assert.Equal(t, "Invalid job failure request: retry backoff of job 42 must not be negative: invalid job request: got -1s", stream.sent[0].Error.GetMessage())
}

// TestRecvClientRequestsCodesUndecodableFailureVariablesAsAnInvalidRequest
// shows variables which do not decode are refused with the same code as any
// other wrong failure request.
func TestRecvClientRequestsCodesUndecodableFailureVariablesAsAnInvalidRequest(t *testing.T) {
	stream := newJobStreamTestServer(failRequest([]byte(`{"broken":`)))
	server := &Server{jobManager: &jobStreamTestManager{}, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	require.Len(t, stream.sent, 1)
	assert.Equal(t, uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_INVALID_REQUEST), stream.sent[0].Error.GetCode())
}

func extendLockRequest(key int64, duration time.Duration) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_ExtendLock{
			ExtendLock: &proto.JobExtendLockRequest{
				Key:            &key,
				LockDurationMs: new(duration.Milliseconds()),
			},
		},
	}
}
