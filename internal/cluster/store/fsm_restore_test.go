package store

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFsmApplyPartitionChangePromotesAndDemotesAtomically verifies that a
// single LEADER partition change records the new leader and demotes the
// previous one in the same apply, so routing never observes two leaders or a
// gap between demotion and promotion.
func TestFsmApplyPartitionChangePromotesAndDemotesAtomically(t *testing.T) {
	store := fsmTestStore{clusterState: state.Cluster{
		Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "old"}},
		Nodes: map[string]state.Node{
			"old": {Id: "old", Addr: "old:1", State: state.NodeStateStarted, Partitions: map[uint32]state.NodePartition{
				1: {Id: 1, Role: state.RoleLeader, State: state.NodePartitionStateInitialized},
			}},
			"new": {Id: "new", Addr: "new:1", State: state.NodeStateStarted, Partitions: map[uint32]state.NodePartition{
				1: {Id: 1, Role: state.RoleFollower, State: state.NodePartitionStateInitialized},
			}},
		},
	}}

	next := FsmApplyPartitionChange(store, &proto.NodePartitionChange{
		NodeId:      new("new"),
		PartitionId: new(uint32(1)),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED.Enum(),
		Role:        proto.Role_ROLE_TYPE_LEADER.Enum(),
	})

	assert.Equal(t, "new", next.Partitions[1].LeaderId)
	assert.Equal(t, state.RoleLeader, next.Nodes["new"].Partitions[1].Role)
	assert.Equal(t, state.RoleFollower, next.Nodes["old"].Partitions[1].Role)
	leaders := 0
	for _, n := range next.Nodes {
		if n.Partitions[1].Role == state.RoleLeader {
			leaders++
		}
	}
	assert.Equal(t, 1, leaders)
	addr, id := next.ActivePartitionLeader(1)
	assert.Equal(t, "new:1", addr)
	assert.Equal(t, "new", id)
}

// TestFsmResumedNodeKeepsLeaderRoleForPartitionItStillLeads verifies that when
// a node's heartbeat resumes, its partition roles come back consistent with
// the partition records: Leader where it is still the recorded leader, Follower
// elsewhere. Routing (ActivePartitionLeader) requires that agreement.
func TestFsmResumedNodeKeepsLeaderRoleForPartitionItStillLeads(t *testing.T) {
	store := fsmTestStore{leaderID: "cluster-leader", clusterState: state.Cluster{
		Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "n1"}, 2: {Id: 2, LeaderId: "n2"}},
		Nodes: map[string]state.Node{
			"n1": {Id: "n1", Addr: "n1:1", State: state.NodeStateShutdown, Partitions: map[uint32]state.NodePartition{
				1: {Id: 1, State: state.NodePartitionStateInitialized}, // role cleared by shutdownNode
				2: {Id: 2, State: state.NodePartitionStateInitialized},
			}},
			"n2": {Id: "n2", Addr: "n2:1", State: state.NodeStateStarted, Partitions: map[uint32]state.NodePartition{
				2: {Id: 2, Role: state.RoleLeader, State: state.NodePartitionStateInitialized},
			}},
		},
	}}

	require.Empty(t, mustAddr(store.clusterState.ActivePartitionLeader(1)), "a shut down leader is not routable")

	next := FsmApplyNodeChange(store, &proto.NodeChange{
		NodeId: new("n1"),
		State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	})

	assert.Equal(t, state.NodeStateStarted, next.Nodes["n1"].State)
	assert.Equal(t, state.RoleLeader, next.Nodes["n1"].Partitions[1].Role, "still the recorded leader of partition 1")
	assert.Equal(t, state.RoleFollower, next.Nodes["n1"].Partitions[2].Role, "follower of partition 2")
	assert.Equal(t, "n1:1", mustAddr(next.ActivePartitionLeader(1)), "routable again after the resume")
	assert.Equal(t, "n2:1", mustAddr(next.ActivePartitionLeader(2)))
}

func mustAddr(addr string, _ string) string {
	return addr
}

type fsmTestStore struct {
	clusterState state.Cluster
	leaderID     string
}

func (s fsmTestStore) ClusterState() state.Cluster {
	return *s.clusterState.DeepCopy()
}

func (s fsmTestStore) LeaderID() (string, error) {
	return s.leaderID, nil
}
