package controller

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineStartsOnRegainedPartitionLeadership(t *testing.T) {
	tStore, clientMgr, mux := setupControllerTestCluster(t)

	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id,
		Addr:   tStore.addr,
		Adv:    tStore.addr,
		Raft: config.ClusterRaft{
			Dir:                    t.TempDir(),
			JoinAttempts:           2,
			JoinInterval:           100 * time.Millisecond,
			JoinAddresses:          []string{tStore.addr},
			BootstrapExpect:        1,
			BootstrapExpectTimeout: time.Second,
		},
	})
	require.NoError(t, err)
	require.NoError(t, controller.Start(tStore, clientMgr))
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})

	tStore.setNode(state.Node{
		Id:         tStore.id,
		Addr:       tStore.addr,
		Suffrage:   raft.Voter,
		State:      state.NodeStateStarted,
		Role:       state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	})
	controller.ClusterStateChangeNotification(t.Context())

	testPoll(t, func() bool {
		pn := controller.GetPartition(t.Context(), 1)
		return pn != nil && pn.Engine != nil
	}, 100*time.Millisecond, 10*time.Second, "partition engine never started initially")

	pn := controller.GetPartition(t.Context(), 1)
	pn.Engine.Stop()
	pn.Engine = nil

	testPoll(t, func() bool {
		controller.ClusterStateChangeNotification(t.Context())
		pn := controller.GetPartition(t.Context(), 1)
		return pn != nil && pn.Engine != nil
	}, 100*time.Millisecond, 10*time.Second, "engine was not re-created after ClusterStateChangeNotification")
}
