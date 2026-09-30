package jobmanager

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientFailJobNamesTheFailingClient shows a job failure reaches the
// leader with the client id of the worker, as a completion does, so the
// leader can tell the lock holder from another client.
func TestClientFailJobNamesTheFailingClient(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	_, completer, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))

	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job", SubscriptionSettings{}))
	completer.loader.addJobs(generateJobs(1)...)
	job := <-clientJobs

	require.NoError(t, clientManager.FailJobReq(t.Context(), "client-1", job.Key, "boom", nil, nil, nil, nil, nil))

	requests := leaderGRPC.receivedFailRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "client-1", requests[0].GetClientId(), "the failure must name the worker which failed the job")
	assert.Equal(t, job.Key, requests[0].GetKey())
	assert.Contains(t, completer.failedJobs, job.Key)
}

// TestClientFailJobCarriesRetriesAndBackoff shows what a worker says about the
// next attempt, the retries left and the backoff, and the delivery its failure
// belongs to reach the engine unchanged, and that a failure which says nothing
// leaves them absent, so that the engine decrements, applies the task
// definition's policy and counts every failure.
func TestClientFailJobCarriesRetriesAndBackoff(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	_, completer, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))

	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job", SubscriptionSettings{}))
	completer.loader.addJobs(generateJobs(2)...)
	retried := <-clientJobs
	defaulted := <-clientJobs

	require.NoError(t, clientManager.FailJobReq(t.Context(), "client-1", retried.Key, "down", nil, nil, new(int32(4)), new(1500*time.Millisecond), &retried.DeliveryToken))
	require.NoError(t, clientManager.FailJobReq(t.Context(), "client-1", defaulted.Key, "down", nil, nil, nil, nil, nil))

	requests := leaderGRPC.receivedFailRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, int32(4), requests[0].GetRetries())
	assert.Equal(t, int64(1500), requests[0].GetRetryBackoffMs())
	assert.Equal(t, retried.DeliveryToken, requests[0].GetDeliveryToken())
	assert.Nil(t, requests[1].DeliveryToken, "no delivery named, every failure counts")
	assert.Nil(t, requests[1].Retries, "no retries named, the engine decrements")
	assert.Nil(t, requests[1].RetryBackoffMs, "no backoff named, the task definition's policy applies")
	require.Len(t, completer.failures, 2)
	assert.Equal(t, new(int32(4)), completer.failures[0].retries)
	assert.Equal(t, new(1500*time.Millisecond), completer.failures[0].retryBackoff)
	assert.Equal(t, &retried.DeliveryToken, completer.failures[0].deliveryToken)
	assert.Nil(t, completer.failures[1].retries)
	assert.Nil(t, completer.failures[1].retryBackoff)
}

// TestClientFailJobTellsARefusalFromAnUnavailableLeader shows the classification
// of the leader's answer survives the hop to the node holding the worker's
// stream, so the stream can tell the worker by code whether its request was
// wrong or the cluster was changing.
func TestClientFailJobTellsARefusalFromAnUnavailableLeader(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)
	serverStore := getTestStore(listener)
	_, _, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))
	jobKey := gen.Generate().Int64()

	for _, tt := range []struct {
		name     string
		refusal  *zenerr.ZenError
		expected error
	}{
		{"negative retries", zenerr.BadRequest(errors.New("retries of job must not be negative, got -1")), ErrInvalidJobRequest},
		{"unknown job", zenerr.NotFound(errors.New("job not found")), ErrJobNotFound},
		{"job no longer waits", zenerr.Conflict(errors.New("job is already completed")), ErrJobInTerminalState},
		{"node no longer leads the partition", zenerr.ClusterError(errors.New("this node does not lead its partition")), ErrLeaderUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leaderGRPC.failJobResponse = &proto.FailJobResponse{Error: tt.refusal.ToProtoError()}
			defer func() { leaderGRPC.failJobResponse = nil }()

			err := clientManager.FailJobReq(t.Context(), "client-1", jobKey, "down", nil, nil, new(int32(-1)), nil, nil)

			assert.ErrorIs(t, err, tt.expected)
		})
	}
	t.Run("a refused request keeps the leader's reason", func(t *testing.T) {
		leaderGRPC.failJobResponse = &proto.FailJobResponse{Error: zenerr.BadRequest(errors.New("retries of job must not be negative, got -1")).ToProtoError()}
		defer func() { leaderGRPC.failJobResponse = nil }()

		err := clientManager.FailJobReq(t.Context(), "client-1", jobKey, "down", nil, nil, new(int32(-1)), nil, nil)

		var invalid *InvalidJobRequestError
		require.ErrorAs(t, err, &invalid)
		assert.Equal(t, "retries of job must not be negative, got -1", invalid.Reason)
	})
	t.Run("any other answer is none of them", func(t *testing.T) {
		leaderGRPC.failJobResponse = &proto.FailJobResponse{Error: zenerr.TechnicalError(errors.New("disk full")).ToProtoError()}
		defer func() { leaderGRPC.failJobResponse = nil }()

		err := clientManager.FailJobReq(t.Context(), "client-1", jobKey, "down", nil, nil, nil, nil, nil)

		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrInvalidJobRequest)
		assert.NotErrorIs(t, err, ErrJobNotFound)
		assert.NotErrorIs(t, err, ErrJobInTerminalState)
		assert.NotErrorIs(t, err, ErrLeaderUnavailable)
	})
}

// TestClientCompleteJobTellsARefusalFromAnUnavailableLeader shows a
// completion the leader answered with an error is reported to the worker, by
// the same sentinels a failure uses, and not as a success.
func TestClientCompleteJobTellsARefusalFromAnUnavailableLeader(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)
	serverStore := getTestStore(listener)
	_, _, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))
	jobKey := gen.Generate().Int64()

	for _, tt := range []struct {
		name     string
		refusal  *zenerr.ZenError
		expected error
	}{
		{"job no longer waits", zenerr.Conflict(errors.New("job is already terminated")), ErrJobInTerminalState},
		{"unknown job", zenerr.NotFound(errors.New("job not found")), ErrJobNotFound},
		{"invalid request", zenerr.BadRequest(errors.New("variables exceed the limit")), ErrInvalidJobRequest},
		{"node no longer leads the partition", zenerr.ClusterError(errors.New("this node does not lead its partition")), ErrLeaderUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leaderGRPC.completeJobResponse = &proto.CompleteJobResponse{Error: tt.refusal.ToProtoError()}
			defer func() { leaderGRPC.completeJobResponse = nil }()

			err := clientManager.CompleteJobReq(t.Context(), "client-1", jobKey, nil)

			assert.ErrorIs(t, err, tt.expected)
		})
	}
	t.Run("any other answer is an error of its own", func(t *testing.T) {
		leaderGRPC.completeJobResponse = &proto.CompleteJobResponse{Error: zenerr.TechnicalError(errors.New("disk full")).ToProtoError()}
		defer func() { leaderGRPC.completeJobResponse = nil }()

		err := clientManager.CompleteJobReq(t.Context(), "client-1", jobKey, nil)

		require.ErrorContains(t, err, "disk full")
		assert.NotErrorIs(t, err, ErrInvalidJobRequest)
		assert.NotErrorIs(t, err, ErrJobNotFound)
		assert.NotErrorIs(t, err, ErrJobInTerminalState)
		assert.NotErrorIs(t, err, ErrLeaderUnavailable)
	})
}

// TestDeliveryCarriesRetriesAttemptAndToken shows a delivery tells the worker
// how many retries the job has left, which attempt it is, and the token of the
// delivery, one above the token of the job's delivery before.
func TestDeliveryCarriesRetriesAttemptAndToken(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	_, completer, _ := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))

	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job", SubscriptionSettings{}))
	job := generateJobs(1)[0]
	job.Retries = 2
	job.Attempts = 1
	job.DeliveryToken = 4
	completer.loader.addJobs(job)

	delivered := <-clientJobs
	assert.Equal(t, int32(2), delivered.Retries)
	assert.Equal(t, int32(2), delivered.Attempt, "one failure so far, so this is the second attempt")
	assert.Equal(t, int64(5), delivered.DeliveryToken)
}

func TestRetryBackoffToMillisKeepsANegativeBackoffNegative(t *testing.T) {
	assert.Nil(t, RetryBackoffToMillis(nil), "absent stays absent")
	for _, tt := range []struct {
		backoff time.Duration
		millis  int64
	}{
		{-time.Nanosecond, -1},
		{-999 * time.Microsecond, -1},
		{-time.Millisecond, -1},
		{-1500 * time.Millisecond, -1500},
		{0, 0},
		{500 * time.Microsecond, 0},
		{1500 * time.Millisecond, 1500},
	} {
		assert.Equal(t, tt.millis, *RetryBackoffToMillis(new(tt.backoff)), "backoff %s", tt.backoff)
	}
}

func TestRetryBackoffFromMillisKeepsAbsentAndNegativeApart(t *testing.T) {
	assert.Nil(t, RetryBackoffFromMillis(nil), "absent stays absent")
	assert.Equal(t, new(time.Duration(0)), RetryBackoffFromMillis(new(int64(0))), "zero asks for at once")
	assert.Equal(t, new(2*time.Second), RetryBackoffFromMillis(new(int64(2000))))
	assert.Less(t, *RetryBackoffFromMillis(new(int64(-5))), time.Duration(0), "a negative count must reach the engine as negative, to be refused")
	assert.Less(t, *RetryBackoffFromMillis(new(int64(math.MinInt64))), time.Duration(0), "and must not wrap to a positive one")
}
