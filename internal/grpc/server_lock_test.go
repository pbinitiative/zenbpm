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
