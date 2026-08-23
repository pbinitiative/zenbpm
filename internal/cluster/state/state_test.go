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
