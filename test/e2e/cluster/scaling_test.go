//go:build cluster_e2e

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 6: Dynamic Scaling
// These tests validate that nodes can be added/removed from a running cluster.
// All tests in this category are slow-tier (nightly).

func TestScaleUpFromOneToThree(t *testing.T) {
	skipIfShort(t)

	// Start with a single node
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)

	// Deploy some data
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Add 2 more nodes
	tc.AddNode(t)
	tc.AddNode(t)

	WaitForNodeCount(t, tc, 3, 60*time.Second)
	WaitForHealthy(t, tc, 90*time.Second)

	// Data should be accessible from new nodes
	for _, n := range tc.RunningNodes() {
		assert.Eventually(t, func() bool {
			resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
			return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
		}, 30*time.Second, 500*time.Millisecond,
			"definition should be visible from node %s", n.ID)
	}
}

func TestScaleDownFromThreeToOne(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Remove 2 nodes gracefully
	followers := tc.Followers()
	require.Len(t, followers, 2)
	tc.StopNode(t, followers[0].ID)
	tc.StopNode(t, followers[1].ID)

	// Last node should eventually take over all partitions
	assert.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) != 1 {
			return false
		}
		s, err := getStatus(running[0])
		if err != nil {
			return false
		}
		return len(s.Partitions) > 0
	}, 60*time.Second, 500*time.Millisecond, "single remaining node should serve partitions")
}

func TestScaleUpDuringLoad(t *testing.T) {
	skipIfShort(t)

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

	// Start continuous writes in background
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	writeErrors := make(chan error, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				body := zenclient.CreateProcessInstanceJSONRequestBody{
					ProcessDefinitionKey: new(defKey),
				}
				_, err := leader.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
				if err != nil {
					writeErrors <- err
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Add 2 nodes while writes are happening
	tc.AddNode(t)
	tc.AddNode(t)

	// Let writes continue for a bit
	time.Sleep(5 * time.Second)
	close(stopCh)
	wg.Wait()
	close(writeErrors)

	// Count errors — some may be tolerable during scaling
	errorCount := 0
	for range writeErrors {
		errorCount++
	}
	t.Logf("Write errors during scale-up: %d", errorCount)

	WaitForNodeCount(t, tc, 5, 60*time.Second)
}

func TestRollingRestart(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Deploy some data
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Restart nodes one at a time
	for _, n := range tc.Nodes {
		nodeID := n.ID
		t.Logf("Rolling restart: stopping %s", nodeID)
		tc.StopNode(t, nodeID)

		// Cluster should remain healthy with remaining nodes
		assert.Eventually(t, func() bool {
			running := tc.RunningNodes()
			if len(running) < 2 {
				return false
			}
			leader := tc.Leader()
			return leader != nil
		}, 30*time.Second, 500*time.Millisecond, "cluster should stay up during rolling restart")

		t.Logf("Rolling restart: restarting %s", nodeID)
		tc.RestartNode(t, nodeID)

		WaitForHealthy(t, tc, 60*time.Second)
	}

	// After rolling restart, all data should still be accessible
	for _, n := range tc.RunningNodes() {
		assert.Eventually(t, func() bool {
			resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
			return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
		}, 30*time.Second, 500*time.Millisecond,
			"definition should be visible from node %s after rolling restart", n.ID)
	}
}

func TestRapidScaleUpDown(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Add a node
	tc.AddNode(t)
	time.Sleep(2 * time.Second)

	// Remove it
	tc.StopNode(t, tc.Nodes[3].ID)
	time.Sleep(2 * time.Second)

	// Add another
	tc.AddNode(t)
	time.Sleep(2 * time.Second)

	// Cluster should converge to stable state
	WaitForHealthy(t, tc, 90*time.Second)
	AssertStateConverged(t, tc, 30*time.Second)
}
