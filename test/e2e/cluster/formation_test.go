//go:build cluster_e2e

package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 1: Cluster Formation & Membership
// These tests validate that nodes can form clusters, join, leave, and maintain
// consistent membership state.

func TestSingleNodeBootstrap(t *testing.T) {
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 30*time.Second)

	// Single node should be the leader
	leader := tc.Leader()
	require.NotNil(t, leader, "single node should be leader")
	assert.Equal(t, "test-node-1", leader.ID)

	// Should have 1 partition
	WaitForPartitions(t, tc, 1, 10*time.Second)
}

func TestThreeNodeBootstrapExpect(t *testing.T) {
	tc := NewTestCluster(t, 3, WithBootstrapExpect(3))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)

	// Exactly one leader
	leader := tc.Leader()
	require.NotNil(t, leader)

	// All 3 nodes visible
	WaitForNodeCount(t, tc, 3, 10*time.Second)

	// State converged across all nodes
	AssertStateConverged(t, tc, 10*time.Second)
}

func TestThreeNodeWithPartition(t *testing.T) {
	// NOTE: DesiredPartitions is currently hardcoded to 1 in the store.
	// When ConfigurationUpdate is implemented, this test should be updated
	// to use WithPartitions(3) and verify multi-partition distribution.
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// The single partition should have a leader
	AssertPartitionHasLeader(t, tc, 1)

	// All nodes should have the partition assigned
	AssertStateConverged(t, tc, 10*time.Second)
}

func TestGracefulLeave(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)
	WaitForNodeCount(t, tc, 3, 10*time.Second)

	// Find a follower and stop it gracefully
	followers := tc.Followers()
	require.NotEmpty(t, followers, "should have followers")
	leavingNode := followers[0]
	tc.StopNode(t, leavingNode.ID)

	// Remaining 2 nodes should still be healthy
	WaitForHealthy(t, tc, 30*time.Second)

	// Verify the cluster still has a leader
	WaitForLeader(t, tc, 10*time.Second)
}

func TestClusterStatusEndpoint(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Every node should return a consistent /system/status
	for _, n := range tc.RunningNodes() {
		s, err := getStatus(n)
		require.NoError(t, err, "node %s status failed", n.ID)

		// Should see all 3 nodes
		assert.Len(t, s.Nodes, 3, "node %s should see 3 nodes", n.ID)

		// Should have at least 1 partition
		assert.NotEmpty(t, s.Partitions, "node %s should see partitions", n.ID)
	}
}

func TestNodeHeartbeatTimeout(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)

	// Hard-kill a follower (no graceful leave)
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	killedNode := followers[0]
	tc.KillNode(t, killedNode.ID)

	// The remaining cluster should still function and have a leader.
	// After a hard kill the cluster needs time to detect the failure.
	assert.Eventually(t, func() bool {
		leader := tc.Leader()
		if leader == nil {
			return false
		}
		s, err := getStatus(leader)
		if err != nil {
			return false
		}
		// Cluster should still have partitions and a leader
		return len(s.Partitions) > 0
	}, 60*time.Second, 500*time.Millisecond, "cluster should remain functional after node kill")
}
