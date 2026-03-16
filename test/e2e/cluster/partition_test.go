//go:build cluster_e2e

package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 2: Partition Lifecycle
// These tests validate partition creation, assignment, state transitions,
// leader election within partitions, and engine lifecycle.

func TestPartitionCreation(t *testing.T) {
	tc := NewTestCluster(t, 3, WithPartitions(3))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 3, 60*time.Second)

	// All 3 partitions should reach INITIALIZED state on at least one node
	for pid := uint32(1); pid <= 3; pid++ {
		AssertPartitionHasLeader(t, tc, pid)
	}
}

func TestPartitionAssignment(t *testing.T) {
	tc := NewTestCluster(t, 3, WithPartitions(3))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 3, 60*time.Second)

	// With 3 partitions and 3 nodes, each node should get at least 1 partition
	s, err := getStatus(tc.RunningNodes()[0])
	require.NoError(t, err)

	for nodeID, node := range s.Nodes {
		assert.NotEmpty(t, node.Partitions, "node %s should have at least 1 partition", nodeID)
	}
}

func TestPartitionStateTransitions(t *testing.T) {
	// Observe that partitions go through JOINING → INITIALIZING → INITIALIZED
	// We verify the end state since transitions happen fast
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	s, err := getStatus(tc.RunningNodes()[0])
	require.NoError(t, err)

	for _, node := range s.Nodes {
		for pID, p := range node.Partitions {
			assert.Equal(t, int64(state.NodePartitionStateInitialized), p.State,
				"node partition %s should be INITIALIZED", pID)
		}
	}
}

func TestPartitionLeaderElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// The partition should have exactly one leader
	AssertPartitionHasLeader(t, tc, 1)
}

func TestPartitionReassignmentOnLeave(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// Remove a follower node
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.StopNode(t, followers[0].ID)

	// Partition should still have a leader after node removal
	assert.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) == 0 {
			return false
		}
		s, err := getStatus(running[0])
		if err != nil {
			return false
		}
		for _, p := range s.Partitions {
			if p.LeaderID == "" {
				return false
			}
		}
		return len(s.Partitions) > 0
	}, 60*time.Second, 500*time.Millisecond, "partition should still have leader after node leave")
}

func TestIncreasePartitionCount(t *testing.T) {
	// Start with 1 partition, increase to 3 dynamically
	// NOTE: This requires ConfigurationUpdate endpoint to be implemented
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// TODO: Call ConfigurationUpdate to increase partition count to 3
	// For now this test documents the desired behavior
	t.Skip("ConfigurationUpdate endpoint is not yet implemented (server.go:177)")
}

func TestPartitionEngineLifecycle(t *testing.T) {
	// Only the partition leader should run a BPMN engine
	// Verify by deploying a process definition — it should succeed
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Deploy a BPMN definition via the leader — should succeed
	leader := tc.Leader()
	require.NotNil(t, leader)

	resp := DeployDefinitionOnNode(t, leader, "simple_task.bpmn")
	require.NotNil(t, resp)
}

func TestMultiplePartitionsPerNode(t *testing.T) {
	// 4 partitions on 2 nodes — each node hosts 2 partitions
	tc := NewTestCluster(t, 2, WithPartitions(4))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 4, 60*time.Second)

	s, err := getStatus(tc.RunningNodes()[0])
	require.NoError(t, err)

	for nodeID, node := range s.Nodes {
		assert.GreaterOrEqual(t, len(node.Partitions), 2,
			"node %s should have at least 2 partitions", nodeID)
	}
}

func TestMaxPartitions(t *testing.T) {
	skipIfShort(t)
	// Attempt to create partitions near the 122 limit (network mux byte constraint)
	// This is a stress test for the partition numbering scheme
	tc := NewTestCluster(t, 1, WithPartitions(122))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 120*time.Second)

	s, err := getStatus(tc.RunningNodes()[0])
	require.NoError(t, err)

	// Should have created partitions up to the limit
	assert.Equal(t, 122, len(s.Partitions),
		"should support up to 122 partitions (mux byte limit)")

	// Verify each partition has a leader
	for pid := uint32(1); pid <= 122; pid++ {
		pKey := fmt.Sprintf("%d", pid)
		p, ok := s.Partitions[pKey]
		assert.True(t, ok, "partition %d should exist", pid)
		assert.NotEmpty(t, p.LeaderID, "partition %d should have a leader", pid)
	}
}
