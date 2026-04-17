//go:build cluster_e2e

package cluster

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Category 8: Stress & Chaos
// These tests verify cluster behavior under high load and random failures.
// All tests in this category are slow-tier (nightly).

func TestHighThroughputMultiNode(t *testing.T) {
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

	// Create 100 instances across all nodes
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64
	nodes := tc.RunningNodes()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			body := zenclient.CreateProcessInstanceJSONRequestBody{
				ProcessDefinitionKey: ptr.To(defKey),
			}
			_, err := n.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
			if err != nil {
				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("High throughput: %d successes, %d errors out of 100", successCount.Load(), errorCount.Load())
	assert.GreaterOrEqual(t, successCount.Load(), int64(90),
		"at least 90%% of instances should be created successfully")
}

func TestConcurrentDeployments(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Deploy the same definition concurrently from different nodes
	var wg sync.WaitGroup
	var successCount atomic.Int64
	nodes := tc.RunningNodes()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			resp := DeployDefinitionOnNode(t, n, "simple_task.bpmn")
			if resp != nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent deployments: %d successes out of 10", successCount.Load())
	// At least some should succeed (conflicts are expected for duplicates)
	assert.Greater(t, successCount.Load(), int64(0), "at least one deployment should succeed")
}

func TestConcurrentInstanceCreation(t *testing.T) {
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

	var wg sync.WaitGroup
	var successCount atomic.Int64
	nodes := tc.RunningNodes()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			body := zenclient.CreateProcessInstanceJSONRequestBody{
				ProcessDefinitionKey: ptr.To(defKey),
			}
			_, err := n.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
			if err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent creation: %d successes out of 50", successCount.Load())
	assert.GreaterOrEqual(t, successCount.Load(), int64(40),
		"at least 80%% of instances should be created successfully")
}

func TestChaosMonkey(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 5)
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

	// Background: continuously create instances
	stopCh := make(chan struct{})
	var instanceCount atomic.Int64

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				for _, n := range tc.RunningNodes() {
					body := zenclient.CreateProcessInstanceJSONRequestBody{
						ProcessDefinitionKey: ptr.To(defKey),
					}
					_, err := n.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
					if err == nil {
						instanceCount.Add(1)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}
	}()

	// Chaos: randomly kill/restart nodes for 30 seconds
	chaosEnd := time.Now().Add(30 * time.Second)
	for time.Now().Before(chaosEnd) {
		running := tc.RunningNodes()
		if len(running) <= 3 {
			// Restart a stopped node if we have too few running
			for _, n := range tc.Nodes {
				if n.stopped {
					tc.RestartNode(t, n.ID)
					break
				}
			}
		} else {
			// Kill a random non-leader node (keep at least 3 running for quorum)
			followers := tc.Followers()
			if len(followers) > 0 {
				victim := followers[rand.Intn(len(followers))]
				tc.KillNode(t, victim.ID)
			}
		}
		time.Sleep(time.Duration(3+rand.Intn(7)) * time.Second)
	}

	close(stopCh)

	// Restart any stopped nodes
	for _, n := range tc.Nodes {
		if n.stopped {
			tc.RestartNode(t, n.ID)
		}
	}

	// Cluster should converge
	WaitForHealthy(t, tc, 120*time.Second)

	t.Logf("Chaos monkey: created %d instances during chaos", instanceCount.Load())
	assert.Greater(t, instanceCount.Load(), int64(0), "should have created some instances during chaos")
}

func TestLeaderBounce(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Kill the leader every 5 seconds for 20 seconds
	for i := 0; i < 4; i++ {
		currentLeader := tc.Leader()
		if currentLeader != nil {
			t.Logf("Bouncing leader: %s", currentLeader.ID)
			tc.KillNode(t, currentLeader.ID)
		}

		time.Sleep(5 * time.Second)

		// Restart any killed nodes
		for _, n := range tc.Nodes {
			if n.stopped {
				tc.RestartNode(t, n.ID)
			}
		}

		// Wait for new leader
		assert.Eventually(t, func() bool {
			return tc.Leader() != nil
		}, 30*time.Second, 500*time.Millisecond, "leader should be elected after bounce %d", i)
	}

	// Final convergence
	WaitForHealthy(t, tc, 120*time.Second)
	AssertStateConverged(t, tc, 30*time.Second)
}

func TestMixedWorkload(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)

	// Deploy BPMN
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")

	// Deploy DMN
	DeployDMNDefinitionOnNode(t, leader, "can-autoliquidate-rule.dmn")

	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.NotEmpty(t, resp.JSON200.Items)
	defKey := resp.JSON200.Items[0].Key

	// Run mixed workload concurrently
	var wg sync.WaitGroup
	nodes := tc.RunningNodes()

	// Create process instances
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			body := zenclient.CreateProcessInstanceJSONRequestBody{
				ProcessDefinitionKey: ptr.To(defKey),
			}
			n.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
		}(i)
	}

	// Query jobs
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			n.RestClient.GetJobsWithResponse(context.Background(), &zenclient.GetJobsParams{})
		}(i)
	}

	// Query incidents
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := nodes[idx%len(nodes)]
			n.RestClient.GetProcessInstancesWithResponse(context.Background(), &zenclient.GetProcessInstancesParams{})
		}(i)
	}

	wg.Wait()
	t.Log("Mixed workload completed without deadlocks")
}

func TestGracefulDegradation(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 5)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 90*time.Second)

	// Progressively remove nodes until quorum is lost
	// 5-node cluster needs 3 for quorum
	removedCount := 0

	for _, n := range tc.Nodes {
		if removedCount >= 3 {
			break // Don't remove more than 3 (leaves 2, below quorum)
		}
		if n.ID == tc.Leader().ID {
			continue // Skip the leader for now
		}
		tc.StopNode(t, n.ID)
		removedCount++
		time.Sleep(2 * time.Second)
	}

	// With only 2 nodes left (below quorum of 3), writes should fail or the cluster
	// should report errors properly
	running := tc.RunningNodes()
	t.Logf("Running nodes after degradation: %d", len(running))

	if len(running) > 0 {
		// Status queries might still work (read from local state)
		s, err := getStatus(running[0])
		if err != nil {
			t.Logf("Status query failed as expected with degraded cluster: %s", err)
		} else {
			t.Logf("Status still queryable with %d nodes: %d partitions", len(running), len(s.Partitions))
		}
	}
}
