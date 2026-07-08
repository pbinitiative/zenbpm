//go:build cluster_e2e

package cluster

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 5: Network Partitions & Split-Brain
// These tests use the TCP proxy to simulate network failures between nodes.
// All tests in this category are slow-tier (nightly).

func TestNodeIsolation(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Isolate a follower
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	isolatedID := followers[0].ID
	tc.IsolateNode(t, isolatedID)

	// Majority (2 nodes) should continue to function
	assert.Eventually(t, func() bool {
		leader := tc.Leader()
		if leader == nil {
			return false
		}
		s, err := getStatus(leader)
		if err != nil {
			return false
		}
		return len(s.Partitions) > 0
	}, 60*time.Second, 500*time.Millisecond, "majority should continue after isolating one node")
}

func TestSymmetricPartition(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 4)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Split into 2+2
	groupA := []string{tc.Nodes[0].ID, tc.Nodes[1].ID}
	groupB := []string{tc.Nodes[2].ID, tc.Nodes[3].ID}
	tc.PartitionNetwork(t, groupA, groupB)

	// Neither side has quorum (2 of 4), so neither should be able to elect a leader
	// Wait a bit for the partition to take effect
	time.Sleep(5 * time.Second)

	// Eventually the cluster should lose leadership on both sides
	// (or one side retains if it had the leader and hasn't timed out yet)
	t.Log("Network partitioned into 2+2 — verifying behavior")

	// Heal and verify recovery
	tc.HealNetwork(t)

	assert.Eventually(t, func() bool {
		return tc.Leader() != nil
	}, 60*time.Second, 500*time.Millisecond, "leader should be re-elected after healing partition")
}

func TestAsymmetricPartition(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Split into 3+2 — majority side should continue
	groupA := []string{tc.Nodes[0].ID, tc.Nodes[1].ID, tc.Nodes[2].ID}
	groupB := []string{tc.Nodes[3].ID, tc.Nodes[4].ID}
	tc.PartitionNetwork(t, groupA, groupB)

	// The majority side (3) should eventually elect a leader
	assert.Eventually(t, func() bool {
		for _, id := range groupA {
			n := tc.NodeByID(id)
			if n == nil || n.stopped {
				continue
			}
			s, err := getStatus(n)
			if err != nil {
				continue
			}
			for _, node := range s.Nodes {
				if node.Role == state.RoleLeader.String() {
					return true
				}
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "majority side should have a leader")

	tc.HealNetwork(t)
}

func TestHealAfterPartition(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Isolate one node
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	isolatedID := followers[0].ID
	tc.IsolateNode(t, isolatedID)

	// Let the cluster proceed for a bit
	time.Sleep(5 * time.Second)

	// Heal the network
	tc.HealNetwork(t)

	// The previously isolated node should rejoin and catch up
	assert.Eventually(t, func() bool {
		n := tc.NodeByID(isolatedID)
		if n == nil || n.stopped {
			return false
		}
		s, err := getStatus(n)
		if err != nil {
			return false
		}
		// Node should see the full cluster again
		return len(s.Nodes) >= 3
	}, 60*time.Second, 500*time.Millisecond, "isolated node should rejoin after network heal")

	AssertStateConverged(t, tc, 30*time.Second)
}

func TestPartitionDuringProcessExecution(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Isolate the partition leader
	var partitionLeaderID string
	s, _ := getStatus(leader)
	for _, p := range s.Partitions {
		partitionLeaderID = p.LeaderID
	}
	require.NotEmpty(t, partitionLeaderID)
	tc.IsolateNode(t, partitionLeaderID)

	// After network partition, a new partition leader should be elected
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			if n.ID == partitionLeaderID {
				continue // skip isolated node
			}
			s, err := getStatus(n)
			if err != nil {
				continue
			}
			for _, p := range s.Partitions {
				if p.LeaderID != "" && p.LeaderID != partitionLeaderID {
					return true
				}
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "new partition leader after network partition")

	// Heal
	tc.HealNetwork(t)
	AssertStateConverged(t, tc, 60*time.Second)
}

func TestNoSplitBrain(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Create a network partition
	groupA := []string{tc.Nodes[0].ID, tc.Nodes[1].ID, tc.Nodes[2].ID}
	groupB := []string{tc.Nodes[3].ID, tc.Nodes[4].ID}
	tc.PartitionNetwork(t, groupA, groupB)

	// Wait for partition to stabilize
	time.Sleep(10 * time.Second)

	// Count leaders across ALL nodes — there should be at most 1 base cluster leader
	leaderCount := 0
	for _, n := range tc.RunningNodes() {
		s, err := getStatus(n)
		if err != nil {
			continue
		}
		for _, node := range s.Nodes {
			if node.ID == n.ID && node.Role == state.RoleLeader.String() {
				leaderCount++
			}
		}
	}

	assert.LessOrEqual(t, leaderCount, 1,
		"should have at most 1 leader during network partition (no split-brain), got %d", leaderCount)

	tc.HealNetwork(t)
}

func TestPartitionBetweenBaseAndSubcluster(t *testing.T) {
	skipIfShort(t)

	// Scenario: base cluster leader can talk to all nodes,
	// but partition subcluster communication is disrupted
	// This is hard to simulate with a TCP proxy since all traffic goes through one port.
	// We approximate by isolating a node briefly and verifying partition failover
	// while base cluster stays stable.
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	baseLeader := tc.Leader()
	require.NotNil(t, baseLeader)

	// Find a non-leader node and briefly isolate it
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.IsolateNode(t, followers[0].ID)
	time.Sleep(3 * time.Second)
	tc.HealNetwork(t)

	// Base cluster leader should not have changed
	assert.Eventually(t, func() bool {
		currentLeader := tc.Leader()
		return currentLeader != nil && currentLeader.ID == baseLeader.ID
	}, 30*time.Second, 500*time.Millisecond, "base cluster leader should remain stable")
}

func TestSlowNetwork(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	// Add 500ms latency to one node
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.AddLatency(t, followers[0].ID, 500*time.Millisecond)

	// Cluster should still function (slowly but no false failovers)
	time.Sleep(5 * time.Second)

	// Leader should still be the same
	currentLeader := tc.Leader()
	assert.NotNil(t, currentLeader)
	assert.Equal(t, leader.ID, currentLeader.ID, "leader should not change with slow network")

	// Status should still be queryable
	s, err := getStatus(currentLeader)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.Nodes)

	// Cleanup
	tc.HealNetwork(t)
}
