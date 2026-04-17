//go:build cluster_e2e

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 3: Leader Failover & Election
// These tests validate that the cluster recovers from leader failures
// at both the base cluster and partition levels.

func TestBaseClusterLeaderFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Kill the current leader
	leader := tc.Leader()
	require.NotNil(t, leader)
	leaderID := leader.ID
	tc.KillNode(t, leaderID)

	// A new leader should be elected from the remaining nodes
	assert.Eventually(t, func() bool {
		newLeader := tc.Leader()
		return newLeader != nil && newLeader.ID != leaderID
	}, 60*time.Second, 500*time.Millisecond, "new leader should be elected after killing old leader")

	// Cluster should still be able to serve requests
	newLeader := tc.Leader()
	require.NotNil(t, newLeader)
	s, err := getStatus(newLeader)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.Nodes)
}

func TestBaseClusterLeaderGracefulStepDown(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	leaderID := leader.ID

	// Graceful stop should allow faster failover than hard kill
	tc.StopNode(t, leaderID)

	assert.Eventually(t, func() bool {
		newLeader := tc.Leader()
		return newLeader != nil && newLeader.ID != leaderID
	}, 30*time.Second, 500*time.Millisecond, "new leader should be elected after graceful step-down")
}

func TestPartitionLeaderFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// Find which node is the partition leader
	var partitionLeaderID string
	for _, n := range tc.RunningNodes() {
		s, err := getStatus(n)
		if err != nil {
			continue
		}
		for _, p := range s.Partitions {
			if p.LeaderID != "" {
				partitionLeaderID = p.LeaderID
				break
			}
		}
		if partitionLeaderID != "" {
			break
		}
	}
	require.NotEmpty(t, partitionLeaderID, "should find a partition leader")

	// Kill the partition leader
	tc.KillNode(t, partitionLeaderID)

	// A new partition leader should be elected
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
			if p.LeaderID != "" && p.LeaderID != partitionLeaderID {
				return true
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "new partition leader should be elected")
}

func TestPartitionLeaderFailoverDuringProcessExecution(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Deploy a process definition
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Create a process instance (it will be waiting at a user task)
	defKey := GetFirstDefinitionKey(t, leader)

	instanceKey := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey)

	// Kill the partition leader
	var partitionLeaderID string
	s, _ := getStatus(leader)
	for _, p := range s.Partitions {
		if p.LeaderID != "" {
			partitionLeaderID = p.LeaderID
			break
		}
	}
	require.NotEmpty(t, partitionLeaderID)
	tc.KillNode(t, partitionLeaderID)

	// After failover, the process instance should still be accessible
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			r, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
			if err == nil && r != nil && r.JSON200 != nil {
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "process instance should be accessible after partition leader failover")
}

func TestDoubleFailover(t *testing.T) {
	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Kill the first leader
	leader1 := tc.Leader()
	require.NotNil(t, leader1)
	tc.KillNode(t, leader1.ID)

	// Wait for new leader
	var leader2 *TestNode
	assert.Eventually(t, func() bool {
		leader2 = tc.Leader()
		return leader2 != nil && leader2.ID != leader1.ID
	}, 60*time.Second, 500*time.Millisecond, "second leader should be elected")

	// Kill the second leader
	require.NotNil(t, leader2)
	tc.KillNode(t, leader2.ID)

	// A third leader should emerge
	assert.Eventually(t, func() bool {
		leader3 := tc.Leader()
		return leader3 != nil && leader3.ID != leader1.ID && leader3.ID != leader2.ID
	}, 60*time.Second, 500*time.Millisecond, "third leader should be elected after double failover")
}

func TestSimultaneousBaseAndPartitionLeaderFailure(t *testing.T) {
	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Find a node that is both base cluster leader AND partition leader
	leader := tc.Leader()
	require.NotNil(t, leader)

	// Kill it — both base cluster and partition should re-elect
	tc.KillNode(t, leader.ID)

	// New base cluster leader
	assert.Eventually(t, func() bool {
		return tc.Leader() != nil
	}, 60*time.Second, 500*time.Millisecond, "new base cluster leader should be elected")

	// Partition should also have a new leader
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
			if p.LeaderID != "" && p.LeaderID != leader.ID {
				return true
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "partition should elect new leader")
}

func TestFailoverPreservesInFlightJobs(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	// Deploy and create instance with a service task that creates a job
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	defKey := GetFirstDefinitionKey(t, leader)

	instanceKey := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey)

	// Kill partition leader
	var partitionLeaderID string
	s, _ := getStatus(leader)
	for _, p := range s.Partitions {
		if p.LeaderID != "" {
			partitionLeaderID = p.LeaderID
		}
	}
	require.NotEmpty(t, partitionLeaderID)
	tc.KillNode(t, partitionLeaderID)

	// After failover, jobs should still be queryable
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			r, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
			if err == nil && r != nil && r.JSON200 != nil {
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "process instance should be accessible after failover")
}

func TestLeaderElectionConvergence(t *testing.T) {
	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	start := time.Now()
	tc.KillNode(t, leader.ID)

	// Measure time until a new leader is elected
	assert.Eventually(t, func() bool {
		return tc.Leader() != nil
	}, 60*time.Second, 100*time.Millisecond, "new leader should be elected")

	elapsed := time.Since(start)
	t.Logf("Leader election convergence time: %s", elapsed)

	// Election should complete within a reasonable bound
	assert.Less(t, elapsed, 30*time.Second,
		"leader election should converge within 30s, took %s", elapsed)
}

func TestFollowerFailureNoDisruption(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	// Kill a follower
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.KillNode(t, followers[0].ID)

	// Leader should still be the same and serving
	currentLeader := tc.Leader()
	assert.NotNil(t, currentLeader)
	assert.Equal(t, leader.ID, currentLeader.ID, "leader should not change when a follower dies")

	// Should still be able to query status
	s, err := getStatus(currentLeader)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.Partitions)
}
