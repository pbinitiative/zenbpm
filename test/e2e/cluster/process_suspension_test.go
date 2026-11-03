//go:build cluster_e2e

package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const suspensionTestProcessID = "Simple_Task_Process"

// TestProcessSuspensionIsConsistentAcrossNodes suspends a process through a follower and checks that every
// node reports it suspended and refuses to start an instance of it, then resumes it through the leader and
// checks that every node starts instances again.
func TestProcessSuspensionIsConsistentAcrossNodes(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	definitionKey := deploySuspensionTestProcess(t, tc)
	follower := tc.Followers()[0]
	leader := tc.Leader()
	require.NotNil(t, leader)

	setSuspensionOnNode(t, follower, true)
	for _, n := range tc.RunningNodes() {
		assertNodeSuspension(t, n, definitionKey, true)
	}

	setSuspensionOnNode(t, leader, false)
	for _, n := range tc.RunningNodes() {
		assertNodeSuspension(t, n, definitionKey, false)
	}
}

// TestProcessSuspensionSurvivesNodeRestarts restarts every node of the cluster, one after the other, while a
// process is suspended: a restarted node must still report it suspended and refuse to start an instance of it.
func TestProcessSuspensionSurvivesNodeRestarts(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	definitionKey := deploySuspensionTestProcess(t, tc)
	setSuspensionOnNode(t, tc.Leader(), true)

	nodeIDs := make([]string, 0, 3)
	for _, n := range tc.RunningNodes() {
		nodeIDs = append(nodeIDs, n.ID)
	}
	for _, nodeID := range nodeIDs {
		tc.KillNode(t, nodeID)
		WaitForNodeObservedDown(t, tc, nodeID, 60*time.Second)
		tc.RestartNode(t, nodeID)
		WaitForHealthy(t, tc, 150*time.Second)
		WaitForNodeCount(t, tc, 3, 60*time.Second)

		assertNodeSuspension(t, tc.NodeByID(nodeID), definitionKey, true)
	}
	for _, n := range tc.RunningNodes() {
		assertNodeSuspension(t, n, definitionKey, true)
	}

	setSuspensionOnNode(t, tc.Leader(), false)
	for _, n := range tc.RunningNodes() {
		assertNodeSuspension(t, n, definitionKey, false)
	}
}

// deploySuspensionTestProcess deploys the process of the tests and waits until every node sees it.
func deploySuspensionTestProcess(t *testing.T, tc *TestCluster) int64 {
	t.Helper()
	nodes := tc.RunningNodes()
	response := DeployDefinitionOnNode(t, nodes[0], "simple_task.bpmn")
	var definitionKey int64
	switch {
	case response.JSON201 != nil:
		definitionKey = response.JSON201.ProcessDefinitionKey
	case response.JSON200 != nil:
		definitionKey = response.JSON200.ProcessDefinitionKey
	default:
		t.Fatalf("deploy answered neither 200 nor 201: %s", string(response.Body))
	}
	for _, n := range nodes {
		GetFirstDefinitionKey(t, n)
	}
	return definitionKey
}

func setSuspensionOnNode(t *testing.T, n *TestNode, suspended bool) {
	t.Helper()
	response, err := n.RestClient.SetProcessSuspensionWithResponse(context.Background(), suspensionTestProcessID, zenclient.SetProcessSuspensionJSONRequestBody{
		Suspended: suspended,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), "node %s: %s", n.ID, string(response.Body))
}

// assertNodeSuspension checks, through one node, what the process reports and whether it starts an instance.
func assertNodeSuspension(t *testing.T, n *TestNode, definitionKey int64, suspended bool) {
	t.Helper()

	assert.Eventually(t, func() bool {
		detail, err := n.RestClient.GetProcessDefinitionWithResponse(context.Background(), definitionKey)
		return err == nil && detail.JSON200 != nil && detail.JSON200.Suspended != nil && *detail.JSON200.Suspended == suspended
	}, 30*time.Second, 200*time.Millisecond, "node %s: process should report suspended=%t", n.ID, suspended)

	response, err := n.RestClient.CreateProcessInstanceWithResponse(context.Background(), zenclient.CreateProcessInstanceJSONRequestBody{
		ProcessDefinitionKey: &definitionKey,
		Variables:            &map[string]any{},
	})
	require.NoError(t, err)
	if suspended {
		assert.Equal(t, http.StatusConflict, response.StatusCode(), "node %s must refuse to start a suspended process: %s", n.ID, string(response.Body))
		return
	}
	assert.Equal(t, http.StatusCreated, response.StatusCode(), "node %s must start a resumed process: %s", n.ID, string(response.Body))
}
