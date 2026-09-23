package jobmanager

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestClientExtendLockReportsAnUnavailableLeader shows every way the leader
// of the job's partition can be out of reach comes back as
// ErrLeaderUnavailable, which a worker may retry, while a caller which went
// away is told about its own context instead.
func TestClientExtendLockReportsAnUnavailableLeader(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)
	serverStore := getTestStore(listener)
	_, _, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	jobKey := gen.Generate().Int64()

	t.Run("the node answers it does not lead the partition any more", func(t *testing.T) {
		clientManager := createClientNode(t, serverStore.forNode("node-2"))
		leaderGRPC.extendLockResponse = &proto.ExtendJobLockResponse{
			Error: zenerr.ClusterError(errors.New("this node does not lead its partition")).ToProtoError(),
		}
		defer func() { leaderGRPC.extendLockResponse = nil }()

		_, err := clientManager.ExtendJobLockReq(t.Context(), "client-1", jobKey, 0)

		assert.ErrorIs(t, err, ErrLeaderUnavailable)
	})

	t.Run("no leader is known for the partition", func(t *testing.T) {
		clientStore := serverStore.forNode("node-2")
		clientStore.updateState(func(cluster *state.Cluster) {
			cluster.Partitions[partition] = state.Partition{Id: partition}
		})
		clientManager := createClientNode(t, clientStore)

		_, err := clientManager.ExtendJobLockReq(t.Context(), "client-1", jobKey, 0)

		assert.ErrorIs(t, err, ErrLeaderUnavailable)
	})

	t.Run("the leader cannot be reached", func(t *testing.T) {
		clientStore := serverStore.forNode("node-2")
		clientStore.updateState(func(cluster *state.Cluster) {
			leader := cluster.Nodes["node-1"]
			leader.Addr = "127.0.0.1:1"
			cluster.Nodes["node-1"] = leader
		})
		clientManager := createClientNode(t, clientStore)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		_, err := clientManager.ExtendJobLockReq(ctx, "client-1", jobKey, 0)

		assert.ErrorIs(t, err, ErrLeaderUnavailable)
	})

	t.Run("the caller went away before a leader was looked for", func(t *testing.T) {
		clientStore := serverStore.forNode("node-2")
		clientStore.updateState(func(cluster *state.Cluster) {
			cluster.Partitions[partition] = state.Partition{Id: partition}
		})
		clientManager := createClientNode(t, clientStore)
		gone, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := clientManager.ExtendJobLockReq(gone, "client-1", jobKey, 0)

		assert.ErrorIs(t, err, context.Canceled, "a caller which is gone is told so, whatever the cluster looks like")
		assert.NotErrorIs(t, err, ErrLeaderUnavailable)
	})

	t.Run("the caller went away", func(t *testing.T) {
		clientManager := createClientNode(t, serverStore.forNode("node-2"))
		gone, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := clientManager.ExtendJobLockReq(gone, "client-1", jobKey, 0)

		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrLeaderUnavailable, "the cluster is not to blame for a caller which is gone")
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestClientExtendLockDoesNotTakeAnUnknownCallForAnUnavailableLeader shows a
// node which answers the call but does not know it, as a node of an older
// release does, is not reported as a leader to retry against: retrying would
// never pass.
func TestClientExtendLockDoesNotTakeAnUnknownCallForAnUnavailableLeader(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)
	serveUnimplementedGRPC(t, listener)
	clientManager := createClientNode(t, getTestStore(listener).forNode("node-2"))

	_, err = clientManager.ExtendJobLockReq(t.Context(), "client-1", gen.Generate().Int64(), 0)

	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
	assert.NotErrorIs(t, err, ErrLeaderUnavailable, "a node without the call is no leader to retry against")
}

// serveUnimplementedGRPC serves a cluster endpoint which knows none of the calls.
func serveUnimplementedGRPC(t *testing.T, listener net.Listener) {
	t.Helper()
	srv := grpc.NewServer()
	proto.RegisterZenServiceServer(srv, proto.UnimplementedZenServiceServer{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	t.Cleanup(func() {
		srv.Stop()
		require.True(t, isExpectedGRPCServerStopError(<-serveErr))
	})
}
