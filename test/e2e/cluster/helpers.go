//go:build cluster_e2e

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusterStatus is used to unmarshal the /system/status JSON response.
type clusterStatus struct {
	ClusterConfig struct {
		DesiredPartitions int64 `json:"desiredPartitions"`
	} `json:"clusterConfig"`
	Nodes map[string]struct {
		Addr       string `json:"addr"`
		ID         string `json:"id"`
		Partitions map[string]struct {
			ID    int64 `json:"id"`
			Role  int64 `json:"role"`
			State int64 `json:"state"`
		} `json:"partitions"`
		Role     int64 `json:"role"`
		State    int64 `json:"state"`
		Suffrage int64 `json:"suffrage"`
	} `json:"nodes"`
	Partitions map[string]struct {
		ID       int64  `json:"id"`
		LeaderID string `json:"leaderId"`
	} `json:"partitions"`
}

// getStatus fetches the cluster status from a node's REST API.
func getStatus(n *TestNode) (*clusterStatus, error) {
	resp, err := http.Get("http://" + n.RestAddr + "/system/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s clusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// WaitForHealthy waits until all running nodes report a healthy cluster state:
// - All running nodes are visible in the cluster
// - All partitions have leaders
// - All node partitions are in INITIALIZED state
func WaitForHealthy(t *testing.T, tc *TestCluster, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) == 0 {
			return false
		}
		// Check from the perspective of any running node
		n := running[0]
		s, err := getStatus(n)
		if err != nil {
			return false
		}

		// All running nodes visible
		if len(s.Nodes) < len(running) {
			return false
		}

		// All partitions have leaders
		for _, p := range s.Partitions {
			if p.LeaderID == "" {
				return false
			}
		}

		// All node partitions are initialized
		for _, node := range s.Nodes {
			for _, p := range node.Partitions {
				if p.State != int64(state.NodePartitionStateInitialized) {
					return false
				}
			}
		}

		return true
	}, timeout, 100*time.Millisecond, "cluster did not become healthy within %s", timeout)
}

// WaitForPartitions waits until the expected number of partitions exist and have leaders.
func WaitForPartitions(t *testing.T, tc *TestCluster, count int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) == 0 {
			return false
		}
		s, err := getStatus(running[0])
		if err != nil {
			return false
		}
		if len(s.Partitions) != count {
			return false
		}
		for _, p := range s.Partitions {
			if p.LeaderID == "" {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "expected %d partitions with leaders within %s", count, timeout)
}

// WaitForLeader waits until a base cluster leader is elected.
func WaitForLeader(t *testing.T, tc *TestCluster, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		return tc.Leader() != nil
	}, timeout, 100*time.Millisecond, "no leader elected within %s", timeout)
}

// WaitForNodeCount waits until the cluster reports the expected number of nodes.
func WaitForNodeCount(t *testing.T, tc *TestCluster, count int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) == 0 {
			return false
		}
		s, err := getStatus(running[0])
		if err != nil {
			return false
		}
		return len(s.Nodes) >= count
	}, timeout, 100*time.Millisecond, "expected %d nodes within %s", count, timeout)
}

// AssertStateConverged verifies all running nodes report the same cluster state.
func AssertStateConverged(t *testing.T, tc *TestCluster, timeout time.Duration) {
	t.Helper()
	assert.Eventually(t, func() bool {
		running := tc.RunningNodes()
		if len(running) < 2 {
			return true // nothing to compare
		}

		first, err := getStatus(running[0])
		if err != nil {
			return false
		}

		for _, n := range running[1:] {
			other, err := getStatus(n)
			if err != nil {
				return false
			}
			// Compare partition leaders
			if len(first.Partitions) != len(other.Partitions) {
				return false
			}
			for k, fp := range first.Partitions {
				op, ok := other.Partitions[k]
				if !ok || fp.LeaderID != op.LeaderID {
					return false
				}
			}
			// Compare node count
			if len(first.Nodes) != len(other.Nodes) {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "cluster state did not converge within %s", timeout)
}

// AssertPartitionHasLeader verifies a specific partition has exactly one leader.
func AssertPartitionHasLeader(t *testing.T, tc *TestCluster, partitionID uint32) {
	t.Helper()
	running := tc.RunningNodes()
	require.NotEmpty(t, running, "no running nodes")

	s, err := getStatus(running[0])
	require.NoError(t, err)

	pKey := fmt.Sprintf("%d", partitionID)
	p, ok := s.Partitions[pKey]
	require.True(t, ok, "partition %d not found", partitionID)
	assert.NotEmpty(t, p.LeaderID, "partition %d has no leader", partitionID)

	// Verify only one node claims leadership for this partition
	leaderCount := 0
	for _, node := range s.Nodes {
		for _, np := range node.Partitions {
			if np.ID == int64(partitionID) && np.Role == int64(state.RoleLeader) {
				leaderCount++
			}
		}
	}
	assert.Equal(t, 1, leaderCount, "partition %d should have exactly 1 leader, got %d", partitionID, leaderCount)
}

// AssertPartitionsBalanced verifies partitions are reasonably distributed across nodes.
// tolerance is a fraction (e.g., 0.5 means no node has >50% more partitions than the average).
func AssertPartitionsBalanced(t *testing.T, tc *TestCluster, tolerance float64) {
	t.Helper()
	running := tc.RunningNodes()
	require.NotEmpty(t, running)

	s, err := getStatus(running[0])
	require.NoError(t, err)

	var counts []int
	for _, node := range s.Nodes {
		counts = append(counts, len(node.Partitions))
	}

	if len(counts) == 0 {
		return
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	avg := float64(total) / float64(len(counts))
	if avg == 0 {
		return
	}

	for nodeID, node := range s.Nodes {
		count := float64(len(node.Partitions))
		deviation := (count - avg) / avg
		assert.LessOrEqual(t, deviation, tolerance,
			"node %s has %d partitions, average is %.1f (deviation %.1f%% exceeds tolerance %.1f%%)",
			nodeID, len(node.Partitions), avg, deviation*100, tolerance*100)
	}
}

// AssertProcessReachableFromAllNodes verifies a process instance can be queried from every running node.
func AssertProcessReachableFromAllNodes(t *testing.T, tc *TestCluster, instanceKey int64) {
	t.Helper()
	for _, n := range tc.RunningNodes() {
		resp, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
		assert.NoError(t, err, "node %s: failed to get process instance %d", n.ID, instanceKey)
		if resp != nil {
			assert.Nil(t, resp.JSON404, "node %s: process instance %d not found", n.ID, instanceKey)
			if resp.JSON200 != nil {
				assert.Equal(t, instanceKey, resp.JSON200.Key, "node %s: wrong instance key", n.ID)
			}
		}
	}
}

// DeployDefinitionOnNode deploys a BPMN definition file via a specific node.
// filename is the BPMN file name relative to pkg/bpmn/test-cases/.
func DeployDefinitionOnNode(t *testing.T, n *TestNode, filename string) *zenclient.CreateProcessDefinitionResponse {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	// Navigate from test/e2e/cluster to project root
	root := strings.ReplaceAll(wd, "/test/e2e/cluster", "")
	loc := filepath.Join(root, "pkg", "bpmn", "test-cases", filename)
	fileContent, err := os.ReadFile(loc)
	require.NoError(t, err, "failed to read BPMN file: %s", loc)

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	require.NoError(t, err)
	_, err = part.Write(fileContent)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	resp, err := n.RestClient.CreateProcessDefinitionWithBodyWithResponse(
		context.Background(), writer.FormDataContentType(), &requestBody,
	)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode(), 400, "deploy failed: %s", string(resp.Body))
	return resp
}

// CreateInstanceOnNode creates a process instance via a specific node.
func CreateInstanceOnNode(t *testing.T, n *TestNode, definitionKey int64, variables map[string]interface{}) int64 {
	t.Helper()
	body := zenclient.CreateProcessInstanceJSONRequestBody{
		ProcessDefinitionKey: ptr.To(definitionKey),
		Variables:            &variables,
	}
	resp, err := n.RestClient.CreateProcessInstanceWithResponse(context.Background(), body)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON201, "create instance returned nil response")
	return resp.JSON201.Key
}

// GetFirstDefinitionKey waits for and returns the first process definition key visible on a node.
func GetFirstDefinitionKey(t *testing.T, n *TestNode) int64 {
	t.Helper()
	var defKey int64
	require.Eventually(t, func() bool {
		resp, err := n.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
		if err != nil || resp.JSON200 == nil || len(resp.JSON200.Items) == 0 {
			return false
		}
		defKey = resp.JSON200.Items[0].Key
		return true
	}, 5*time.Second, 100*time.Millisecond, "process definition should be visible on node %s", n.ID)
	return defKey
}

// DeployDMNDefinitionOnNode deploys a DMN definition file via a specific node.
// filename is the DMN file name relative to pkg/dmn/test-data/bulk-evaluation-test/.
func DeployDMNDefinitionOnNode(t *testing.T, n *TestNode, filename string) {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	root := strings.ReplaceAll(wd, "/test/e2e/cluster", "")
	loc := filepath.Join(root, "pkg", "dmn", "test-data", "bulk-evaluation-test", filename)
	fileContent, err := os.ReadFile(loc)
	require.NoError(t, err, "failed to read DMN file: %s", loc)

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	require.NoError(t, err)
	_, err = part.Write(fileContent)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	resp, err := n.RestClient.CreateDmnResourceDefinitionWithBodyWithResponse(
		context.Background(), writer.FormDataContentType(), &requestBody,
	)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode(), 400, "DMN deploy failed: %s", string(resp.Body))
}
