//go:build cluster_e2e

package cluster

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is a content fingerprint for change detection, not a security primitive
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentDeploymentsFromDifferentNodesAgreeOnVersions deploys two
// revisions of one process id at the same time through two different nodes
// (so at least one allocation is forwarded to the cluster leader) and checks
// that every node reports the same (version → key, checksum) mapping and
// that no deployment failed. It then deploys a third revision concurrently
// through two nodes, which must share one definition, and retries it, which
// must be answered with the existing key.
//
// The cluster has three nodes but one partition: a cluster with more
// partitions does not form yet (partition membership is not implemented), so
// the cross-partition mapping is covered by the fake-partition regression in
// internal/cluster instead.
func TestConcurrentDeploymentsFromDifferentNodesAgreeOnVersions(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 60*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	nodes := tc.RunningNodes()
	require.Len(t, nodes, 3)
	const processID = "Simple_Task_Process"
	revisionA := clusterTestBPMN(t, "simple_task.bpmn", "revision A")
	revisionB := clusterTestBPMN(t, "simple_task.bpmn", "revision B")
	revisionC := clusterTestBPMN(t, "simple_task.bpmn", "revision C")

	// two different revisions at once
	keys := deployConcurrently(t, []concurrentDeployment{{node: nodes[0], data: revisionA}, {node: nodes[1], data: revisionB}})
	assert.NotEqual(t, keys[0], keys[1])
	mapping := assertNodesAgreeOnProcessDefinitionVersions(t, nodes, processID, 2)
	assert.ElementsMatch(t, keys, []int64{mapping[1].key, mapping[2].key})
	assert.ElementsMatch(t, []definitionChecksum{md5.Sum(revisionA), md5.Sum(revisionB)}, []definitionChecksum{mapping[1].checksum, mapping[2].checksum})

	// the same new revision at once through two nodes
	keys = deployConcurrently(t, []concurrentDeployment{{node: nodes[0], data: revisionC}, {node: nodes[2], data: revisionC}})
	assert.Equal(t, keys[0], keys[1], "concurrent identical deployments share one definition")
	mapping = assertNodesAgreeOnProcessDefinitionVersions(t, nodes, processID, 3)
	assert.Equal(t, definitionRef{key: keys[0], checksum: md5.Sum(revisionC)}, mapping[3])

	// retrying the latest revision through yet another node reuses its definition
	retry, err := nodes[1].RestClient.CreateProcessDefinitionWithBodyWithResponse(
		context.Background(), "application/octet-stream", bytes.NewReader(revisionC))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, retry.StatusCode(), string(retry.Body))
	assert.Equal(t, keys[0], retry.JSON200.ProcessDefinitionKey)
	assertNodesAgreeOnProcessDefinitionVersions(t, nodes, processID, 3)
}

type concurrentDeployment struct {
	node *TestNode
	data []byte
}

// deployConcurrently runs the deployments at the same time and returns the
// key each one was answered with; a failed deployment fails the test.
func deployConcurrently(t *testing.T, deployments []concurrentDeployment) []int64 {
	t.Helper()
	responses := make([]*zenclient.CreateProcessDefinitionResponse, len(deployments))
	errs := make([]error, len(deployments))
	var wg sync.WaitGroup
	for i, d := range deployments {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses[i], errs[i] = d.node.RestClient.CreateProcessDefinitionWithBodyWithResponse(
				context.Background(), "application/octet-stream", bytes.NewReader(d.data))
		}()
	}
	wg.Wait()

	keys := make([]int64, len(deployments))
	for i, resp := range responses {
		require.NoError(t, errs[i], "deployment %d", i)
		switch resp.StatusCode() {
		case http.StatusCreated:
			keys[i] = resp.JSON201.ProcessDefinitionKey
		case http.StatusOK:
			keys[i] = resp.JSON200.ProcessDefinitionKey
		default:
			t.Fatalf("deployment %d failed with %d: %s", i, resp.StatusCode(), string(resp.Body))
		}
	}
	return keys
}

type definitionChecksum = [md5.Size]byte

// definitionRef identifies the definition a node reports for a version.
type definitionRef struct {
	key      int64
	checksum definitionChecksum
}

// assertNodesAgreeOnProcessDefinitionVersions waits until every node lists
// the expected number of versions of the process and checks that all of them
// report the same (version → key, checksum) mapping, which it returns.
func assertNodesAgreeOnProcessDefinitionVersions(t *testing.T, nodes []*TestNode, processID string, versions int) map[int]definitionRef {
	t.Helper()
	var reference map[int]definitionRef
	for _, node := range nodes {
		var mapping map[int]definitionRef
		require.Eventually(t, func() bool {
			mapping = processDefinitionVersionsOnNode(t, node, processID)
			return len(mapping) == versions
		}, 10*time.Second, 200*time.Millisecond, "node %s lists %d versions", node.ID, versions)
		for v := 1; v <= versions; v++ {
			assert.Contains(t, mapping, v, "node %s", node.ID)
		}
		if reference == nil {
			reference = mapping
			continue
		}
		assert.Equal(t, reference, mapping, "node %s disagrees on the version mapping", node.ID)
	}
	return reference
}

// processDefinitionVersionsOnNode lists the (version → key, checksum)
// mapping of a process as the node reports it, the checksum being taken over
// the BPMN bytes the node serves for the definition.
func processDefinitionVersionsOnNode(t *testing.T, node *TestNode, processID string) map[int]definitionRef {
	t.Helper()
	resp, err := node.RestClient.GetProcessDefinitionsWithResponse(context.Background(), &zenclient.GetProcessDefinitionsParams{
		BpmnProcessId: &processID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200, string(resp.Body))
	mapping := map[int]definitionRef{}
	for _, item := range resp.JSON200.Items {
		detail, err := node.RestClient.GetProcessDefinitionWithResponse(context.Background(), item.Key)
		require.NoError(t, err)
		require.NotNil(t, detail.JSON200, string(detail.Body))
		require.NotNil(t, detail.JSON200.BpmnData)
		mapping[item.Version] = definitionRef{key: item.Key, checksum: md5.Sum([]byte(*detail.JSON200.BpmnData))}
	}
	return mapping
}

// clusterTestBPMN reads a fixture from pkg/bpmn/test-cases and turns it into a
// distinct revision by renaming the process.
func clusterTestBPMN(t *testing.T, filename string, processName string) []byte {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	root := strings.ReplaceAll(wd, "/test/e2e/cluster", "")
	content, err := os.ReadFile(filepath.Join(root, "pkg", "bpmn", "test-cases", filename))
	require.NoError(t, err)
	revised := strings.Replace(string(content), `name="aName"`, `name="`+processName+`"`, 1)
	require.NotEqual(t, string(content), revised, "the fixture must carry the process name the revision replaces")
	return []byte(revised)
}
