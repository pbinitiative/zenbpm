package server

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendJobLockOnANonLeaderIsAClusterError(t *testing.T) {
	// a job manager which never started its leader-side server answers
	// NodeIsNotALeader, as a node does after losing the partition
	server := &Server{jobManager: jobmanager.New(t.Context(), extendLockTestStore{}, nil, nil, nil)}

	resp, err := server.ExtendJobLock(t.Context(), &proto.ExtendJobLockRequest{
		Key:      new(int64(7)),
		ClientId: new("client-1"),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, uint32(zenerr.ClusterErrorCode), resp.Error.GetCode(), "a stale leader is a routing failure to retry, not an internal error")
	assert.Equal(t, proto.LockRefusal_LOCK_REFUSAL_NONE, resp.GetRefusal())
}

type extendLockTestStore struct{}

func (extendLockTestStore) ClusterState() state.Cluster { return state.Cluster{} }
func (extendLockTestStore) NodeID() string              { return "node-1" }
