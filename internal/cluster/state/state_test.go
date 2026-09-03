package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPartitionFollowerSelectsOnlyInitializedNodes(t *testing.T) {
	clusterState := Cluster{
		Partitions: map[uint32]Partition{1: {Id: 1, LeaderId: "leader"}},
		Nodes: map[string]Node{
			"leader": {Id: "leader", Addr: "leader:1", Partitions: map[uint32]NodePartition{
				1: {Id: 1, Role: RoleLeader, State: NodePartitionStateInitialized},
			}},
			"initializing-follower": {Id: "initializing-follower", Addr: "follower:1", Partitions: map[uint32]NodePartition{
				1: {Id: 1, Role: RoleFollower, State: NodePartitionStateInitializing},
			}},
		},
	}

	node, err := clusterState.GetPartitionFollower(1)
	require.NoError(t, err)
	assert.Equal(t, "leader", node.Id)

	leader := clusterState.Nodes["leader"]
	leaderPartition := leader.Partitions[1]
	leaderPartition.State = NodePartitionStateInitializing
	leader.Partitions[1] = leaderPartition
	clusterState.Nodes["leader"] = leader
	_, err = clusterState.GetPartitionFollower(1)
	require.Error(t, err)
}

func TestPartitionLeaderInitializedRequiresMatchingLeaderRole(t *testing.T) {
	clusterState := Cluster{
		Partitions: map[uint32]Partition{1: {Id: 1, LeaderId: "node-1"}},
		Nodes: map[string]Node{"node-1": {Id: "node-1", Partitions: map[uint32]NodePartition{
			1: {Id: 1, Role: RoleFollower, State: NodePartitionStateInitialized},
		}}},
	}

	assert.False(t, clusterState.PartitionLeaderInitialized(1))
	partition := clusterState.Nodes["node-1"].Partitions[1]
	partition.Role = RoleLeader
	node := clusterState.Nodes["node-1"]
	node.Partitions[1] = partition
	clusterState.Nodes["node-1"] = node
	assert.True(t, clusterState.PartitionLeaderInitialized(1))
}

func TestGetPartitionIdFromString(t *testing.T) {
	partitions := make(map[uint32]Partition)
	partitions[1] = Partition{
		Id:       1,
		LeaderId: "node-1",
	}
	partitions[2] = Partition{
		Id:       2,
		LeaderId: "node-2",
	}
	partitions[3] = Partition{
		Id:       3,
		LeaderId: "node-3",
	}
	state := Cluster{
		Config:     ClusterConfig{},
		Partitions: partitions,
		Nodes:      nil,
	}
	assert.Equal(t, uint32(1), state.GetPartitionIdFromString("0"))
	assert.Equal(t, uint32(2), state.GetPartitionIdFromString("1"))
	assert.Equal(t, uint32(3), state.GetPartitionIdFromString("2"))
}

// TestGetPartitionFollowerSkipsShutdownNodes verifies that a node in
// NodeStateShutdown is never returned as a read target, even if it still
// carries a stale NodePartition.Role=Follower entry in the cluster state.
//
// Regression: when the partition leader was killed, the server-side
// PartitionNodeLeaderChange handler demoted the old leader's partition role
// to Follower — which then made the dead node a valid candidate for
// follower-routed reads. GetPartitionFollower must filter out SHUTDOWN
// nodes regardless of their partition role.
func TestGetPartitionFollowerSkipsShutdownNodes(t *testing.T) {
	cs := Cluster{
		Partitions: map[uint32]Partition{
			1: {Id: 1, LeaderId: "node-2"},
		},
		Nodes: map[string]Node{
			"node-1": {
				Id:    "node-1",
				Addr:  "127.0.0.1:8091",
				State: NodeStateStarted,
				Partitions: map[uint32]NodePartition{
					1: {Id: 1, State: NodePartitionStateInitialized, Role: RoleFollower},
				},
			},
			"node-2": {
				Id:    "node-2",
				Addr:  "127.0.0.1:8092",
				State: NodeStateStarted,
				Partitions: map[uint32]NodePartition{
					1: {Id: 1, State: NodePartitionStateInitialized, Role: RoleLeader},
				},
			},
			"node-3": {
				Id:    "node-3",
				Addr:  "127.0.0.1:8093",
				State: NodeStateShutdown, // killed
				Partitions: map[uint32]NodePartition{
					// stale Follower role left over from PartitionNodeLeaderChange demotion.
					1: {Id: 1, State: NodePartitionStateInitialized, Role: RoleFollower},
				},
			},
		},
	}

	// Map iteration order is randomized per call, so exercise the selection
	// enough times that a regression would show up as a flaky pick of node-3.
	for i := 0; i < 50; i++ {
		got, err := cs.GetPartitionFollower(1)
		require.NoError(t, err)
		assert.Equal(t, "node-1", got.Id,
			"iteration %d: shutdown node must never be returned as read target", i)
	}
}

// TestGetPartitionFollowerSkipsShutdownNodesFallback covers the case where
// the only remaining follower is shutdown and the stale LeaderId in state
// points to a dead node too — the selector must not happily return a dead
// address either way.
func TestGetPartitionFollowerSkipsShutdownNodesFallback(t *testing.T) {
	cs := Cluster{
		Partitions: map[uint32]Partition{
			1: {Id: 1, LeaderId: "node-3"}, // stale: dead node still listed as leader
		},
		Nodes: map[string]Node{
			"node-1": {
				Id:    "node-1",
				Addr:  "127.0.0.1:8091",
				State: NodeStateStarted,
				Partitions: map[uint32]NodePartition{
					1: {Id: 1, State: NodePartitionStateInitialized, Role: RoleLeader},
				},
			},
			"node-3": {
				Id:    "node-3",
				Addr:  "127.0.0.1:8093",
				State: NodeStateShutdown,
				Partitions: map[uint32]NodePartition{
					1: {Id: 1, State: NodePartitionStateInitialized, Role: RoleFollower},
				},
			},
		},
	}

	// No healthy follower exists. Whatever the selector falls back to, it
	// must not be the dead node-3.
	for i := 0; i < 50; i++ {
		got, err := cs.GetPartitionFollower(1)
		require.NoError(t, err)
		assert.NotEqual(t, "node-3", got.Id,
			"iteration %d: shutdown node must never be returned even as fallback", i)
	}
}
