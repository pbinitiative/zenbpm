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

func TestJoiningReentryDoesNotRestartRunningPartition(t *testing.T) {
	testStore, clientManager, mux := setupControllerTestCluster(t)

	controller, err := NewController(mux, config.Cluster{
		NodeId: testStore.id,
		Addr:   testStore.addr,
		Adv:    testStore.addr,
		Raft: config.ClusterRaft{
			Dir:                    t.TempDir(),
			JoinAttempts:           2,
			JoinInterval:           100 * time.Millisecond,
			JoinAddresses:          []string{testStore.addr},
			BootstrapExpect:        1,
			BootstrapExpectTimeout: time.Second,
		},
	})
	require.NoError(t, err)

	require.NoError(t, controller.Start(testStore, clientManager))
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})

	testStore.setNode(state.Node{
		Id:         testStore.id,
		Addr:       testStore.addr,
		Suffrage:   raft.Voter,
		State:      state.NodeStateStarted,
		Role:       state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	})

	controller.ClusterStateChangeNotification(t.Context())
	testPoll(t, func() bool {
		clusterState := controller.store.ClusterState()
		return clusterState.Nodes[testStore.id].Partitions[1].State == state.NodePartitionStateInitialized
	}, 100*time.Millisecond, 10*time.Second, "partition 1 never reached INITIALIZED")

	controller.partitionsMu.RLock()
	original := controller.partitions[1]
	controller.partitionsMu.RUnlock()
	require.NotNil(t, original)

	// Simulate the stale-local-state window where INITIALIZING has not yet
	// replicated back to this node while its partition is already running.
	node := testStore.ClusterState().Nodes[testStore.id]
	partitionState := node.Partitions[1]
	partitionState.State = state.NodePartitionStateJoining
	node.Partitions[1] = partitionState
	testStore.setNode(node)

	controller.ClusterStateChangeNotification(t.Context())

	controller.partitionsMu.RLock()
	current := controller.partitions[1]
	controller.partitionsMu.RUnlock()
	assert.Same(t, original, current,
		"re-entering JOINING with a running partition node must not start a second instance")
}
