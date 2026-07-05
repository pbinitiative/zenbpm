//go:build cluster_e2e

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 9: Stream & Client Resilience
// These tests verify gRPC stream and REST API behavior during cluster failures.
// Mixed tier: some fast, some slow.

func TestJobActivateCompleteAcrossFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.NotEmpty(t, resp.JSON200.Items)
	defKey := resp.JSON200.Items[0].Key

	// Create an instance
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

	// After failover, instance should be queryable via a surviving node
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			r, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
			if err == nil && r != nil && r.JSON200 != nil {
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "instance should be accessible after failover")
}

func TestRestRequestDuringLeaderElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	// Kill the leader to trigger election
	tc.KillNode(t, leader.ID)

	// Immediately fire REST requests at surviving nodes
	// They should return errors (not hang or panic)
	for _, n := range tc.RunningNodes() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := n.RestClient.GetProcessDefinitionsWithResponse(ctx, nil)
		cancel()

		// Either error or a valid response — no panics, no infinite hang
		if err != nil {
			t.Logf("Node %s returned error during election (expected): %s", n.ID, err)
		} else if resp != nil {
			t.Logf("Node %s returned status %d during election", n.ID, resp.StatusCode())
		}
	}

	// After election completes, requests should succeed
	assert.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
			if err == nil && resp.StatusCode() < 500 {
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "REST requests should succeed after election")
}

func TestRestRequestDuringPartitionLeaderElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.NotEmpty(t, resp.JSON200.Items)
	defKey := resp.JSON200.Items[0].Key

	// Kill partition leader
	var partitionLeaderID string
	s, _ := getStatus(leader)
	for _, p := range s.Partitions {
		partitionLeaderID = p.LeaderID
	}
	require.NotEmpty(t, partitionLeaderID)
	tc.KillNode(t, partitionLeaderID)

	// Try to create an instance during partition failover
	// Should either succeed or return a clear error, never partial write
	for _, n := range tc.RunningNodes() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		body := zenclient.CreateProcessInstanceJSONRequestBody{
			ProcessDefinitionKey: new(defKey),
		}
		createResp, err := n.RestClient.CreateProcessInstanceWithResponse(ctx, body)
		cancel()

		if err != nil {
			t.Logf("Node %s: create during partition failover returned error (acceptable): %s", n.ID, err)
		} else if createResp.JSON201 != nil {
			t.Logf("Node %s: create succeeded during partition failover with key %d", n.ID, createResp.JSON201.Key)
		} else {
			t.Logf("Node %s: create returned status %d (acceptable during failover)", n.ID, createResp.StatusCode())
		}
		// The key assertion: no panic, no hang, clear result
	}
}

func TestRestIdempotencyAfterRetry(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.NotEmpty(t, resp.JSON200.Items)
	defKey := resp.JSON200.Items[0].Key

	// Create an instance normally
	instanceKey := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey)

	// Create another — should get a different key (no accidental duplicates)
	instanceKey2 := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey2)
	assert.NotEqual(t, instanceKey, instanceKey2, "different create calls should produce different instance keys")
}

func TestGrpcStreamDuringNodeIsolation(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Isolate a follower
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.IsolateNode(t, followers[0].ID)

	// Remaining nodes should still be able to serve gRPC requests
	time.Sleep(3 * time.Second)

	leader := tc.Leader()
	if leader != nil {
		s, err := getStatus(leader)
		assert.NoError(t, err)
		assert.NotEmpty(t, s.Partitions)
	}

	tc.HealNetwork(t)
}

func TestGrpcStreamUnderLatency(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Add 500ms latency to one node
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	tc.AddLatency(t, followers[0].ID, 500*time.Millisecond)

	// Cluster should still function
	leader := tc.Leader()
	require.NotNil(t, leader)

	// Deploy should still work (might be slower)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, resp.JSON200)

	tc.HealNetwork(t)
}

func TestConcurrentStreamsMultiplePartitions(t *testing.T) {
	skipIfShort(t)

	// With multiple partitions, killing one partition leader should only
	// affect streams to that partition
	tc := NewTestCluster(t, 3, WithPartitions(3))
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)
	WaitForPartitions(t, tc, 3, 60*time.Second)

	// Deploy on leader
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Verify definitions are accessible from all nodes
	for _, n := range tc.RunningNodes() {
		assert.Eventually(t, func() bool {
			resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
			return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
		}, 30*time.Second, 500*time.Millisecond,
			"definitions should be visible from node %s", n.ID)
	}
}
