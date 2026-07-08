//go:build cluster_e2e

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 7: Crash Recovery & Snapshots
// These tests validate that nodes can recover from crashes,
// rejoin the cluster, and restore state from disk.
// All tests in this category are slow-tier (nightly).

func TestNodeRecoveryFromDisk(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Kill a follower
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	killedID := followers[0].ID
	tc.KillNode(t, killedID)

	time.Sleep(3 * time.Second)

	// Restart with same data dir — should rejoin from Raft log
	tc.RestartNode(t, killedID)

	WaitForHealthy(t, tc, 150*time.Second)
	WaitForNodeCount(t, tc, 3, 60*time.Second)
	AssertStateConverged(t, tc, 30*time.Second)
}

func TestNodeRecoveryAfterLongOutage(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Kill a node
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	killedID := followers[0].ID
	tc.KillNode(t, killedID)

	// Deploy data while node is down
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Let some time pass
	time.Sleep(5 * time.Second)

	// Restart — node should catch up via snapshot + log replay
	tc.RestartNode(t, killedID)

	WaitForHealthy(t, tc, 150*time.Second)

	// Restarted node should eventually see the deployed definition
	n := tc.NodeByID(killedID)
	assert.Eventually(t, func() bool {
		resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
		return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
	}, 60*time.Second, 1*time.Second, "restarted node should see data deployed during outage")
}

func TestAllNodesRestart(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Deploy some data
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Stop all nodes gracefully
	for _, n := range tc.Nodes {
		tc.StopNode(t, n.ID)
	}

	// Restart all nodes
	for _, n := range tc.Nodes {
		tc.RestartNode(t, n.ID)
	}

	// Cluster should reform from persisted state
	WaitForHealthy(t, tc, 120*time.Second)

	// Data should still be there
	for _, n := range tc.RunningNodes() {
		assert.Eventually(t, func() bool {
			resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
			return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
		}, 60*time.Second, 1*time.Second,
			"definition should survive full cluster restart on node %s", n.ID)
	}
}

func TestColdBootFromPersistence(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Deploy data
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Stop all nodes
	for _, n := range tc.Nodes {
		tc.StopNode(t, n.ID)
	}

	// Restart in REVERSE order (different from original start order)
	for i := len(tc.Nodes) - 1; i >= 0; i-- {
		tc.RestartNode(t, tc.Nodes[i].ID)
	}

	// Should re-form and have all data
	WaitForHealthy(t, tc, 120*time.Second)
	AssertStateConverged(t, tc, 30*time.Second)
}

func TestPartitionRecoveryPreservesProcessState(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.NotEmpty(t, resp.JSON200.Items)
	defKey := resp.JSON200.Items[0].Key

	// Create an instance (will be active at a task)
	instanceKey := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey)

	// Kill partition leader
	var partitionLeaderID string
	s, _ := getStatus(leader)
	for _, p := range s.Partitions {
		partitionLeaderID = p.LeaderID
	}
	require.NotEmpty(t, partitionLeaderID)
	tc.KillNode(t, partitionLeaderID)

	// Restart
	tc.RestartNode(t, partitionLeaderID)
	WaitForHealthy(t, tc, 150*time.Second)

	// Process instance should still be accessible and in the same state
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			r, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
			if err == nil && r != nil && r.JSON200 != nil {
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "process instance should survive partition leader restart")
}

func TestRecoveryAfterDiskCorruption(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Kill a follower
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	killedID := followers[0].ID
	killedNode := tc.NodeByID(killedID)
	tc.KillNode(t, killedID)

	// Corrupt its data directory
	raftDir := killedNode.TempDir
	os.RemoveAll(filepath.Join(raftDir, "raft.db"))
	os.RemoveAll(filepath.Join(raftDir, "partition-1"))

	// Restart with corrupted/missing data — should rejoin as a fresh node
	// This may fail depending on how the node handles missing Raft state
	tc.RestartNode(t, killedID)

	// Give it time to recover
	WaitForHealthy(t, tc, 120*time.Second)
}

func TestConcurrentNodeRecovery(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)

	// Kill 2 followers simultaneously
	followers := tc.Followers()
	require.Len(t, followers, 4, "5-node cluster should have 4 followers")
	tc.KillNode(t, followers[0].ID)
	tc.KillNode(t, followers[1].ID)

	time.Sleep(3 * time.Second)

	// Restart both
	tc.RestartNode(t, followers[0].ID)
	tc.RestartNode(t, followers[1].ID)

	// Both should rejoin and cluster should converge
	WaitForHealthy(t, tc, 120*time.Second)
	WaitForNodeCount(t, tc, 5, 60*time.Second)
	AssertStateConverged(t, tc, 30*time.Second)
}
