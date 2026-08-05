package cluster

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
)

func TestNodeReadinessIgnoresClusterWidePartitionDegradation(t *testing.T) {
	cs := state.Cluster{
		Config: state.ClusterConfig{DesiredPartitions: 3},
		Partitions: map[uint32]state.Partition{
			1: {Id: 1, LeaderId: ""},
		},
		Nodes: map[string]state.Node{
			"node-1": {
				Id: "node-1",
				Partitions: map[uint32]state.NodePartition{
					2: {Id: 2, State: state.NodePartitionStateInitialized},
				},
			},
		},
	}

	assert.Empty(t, nodeReadinessReasons(cs, "node-1"))
}

func TestNodeReadinessReportsLocalPartitionState(t *testing.T) {
	cs := state.Cluster{Nodes: map[string]state.Node{
		"node-1": {
			Id: "node-1",
			Partitions: map[uint32]state.NodePartition{
				2: {Id: 2, State: state.NodePartitionStateInitializing},
			},
		},
	}}

	assert.Equal(t, []string{"partition 2 on this node is in state NodePartitionStateInitializing"}, nodeReadinessReasons(cs, "node-1"))
}

func TestNodeReadinessReportsUnregisteredNode(t *testing.T) {
	cs := state.Cluster{Nodes: map[string]state.Node{}}

	assert.Equal(t, []string{"node is not registered in the cluster state"}, nodeReadinessReasons(cs, "node-1"))
}
