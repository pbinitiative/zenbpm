package controller

import (
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
)

func TestPartitionLeaderChange(t *testing.T) {
	t.Run("empty server ID is a no-op", func(t *testing.T) {
		tStore := &ControllerTestStore{
			id:           "test-node-1",
			clusterState: state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}},
			leader:       true,
		}
		controller := &Controller{store: tStore, logger: hclog.NewNullLogger()}

		err := controller.partitionLeaderChange(raft.ServerID(""), 1)
		assert.NoError(t, err, "empty ServerID should not return an error")
		assert.Empty(t, controller.store.ClusterState().Partitions, "no state change should happen")
	})

	t.Run("invalid server ID returns an error", func(t *testing.T) {
		tStore := &ControllerTestStore{
			id:           "test-node-1",
			clusterState: state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}},
			leader:       true,
		}
		controller := &Controller{store: tStore, logger: hclog.NewNullLogger()}

		err := controller.partitionLeaderChange(raft.ServerID("garbage"), 1)
		assert.Error(t, err, "unresolvable ServerID should return an error")
	})
}
