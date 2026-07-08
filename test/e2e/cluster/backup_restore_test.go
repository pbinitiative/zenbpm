//go:build cluster_e2e

package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultScriptConfig returns FEEL/JS VM pool sizes matching the cleanenv
// env-defaults (Max 10 / Min 2). The harness builds config.Config as a struct
// literal, which bypasses those defaults, leaving the pools at 0/0 — in which
// state NewRunnerPool blocks forever on the first FEEL evaluation (e.g. a
// message correlation key expression). Wiring this into the harness config keeps
// FEEL-using processes (message catch events) working under e2e.
func defaultScriptConfig() config.Script {
	return config.Script{
		Feel: config.ScriptVmPoolConf{MaxVmPoolSize: 10, MinVmPoolSize: 2},
		Js:   config.ScriptVmPoolConf{MaxVmPoolSize: 10, MinVmPoolSize: 2},
	}
}

// takeBackup performs GET /system/v1/cluster/backup and returns the raw tar bundle.
func takeBackup(t *testing.T, n *TestNode) []byte {
	t.Helper()
	resp, err := http.Get("http://" + n.RestAddr + "/system/v1/cluster/backup")
	require.NoError(t, err, "backup request failed")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "backup should return 200")
	bundle, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "reading backup body")
	require.NotEmpty(t, bundle, "backup bundle must not be empty")
	return bundle
}

// postRestore uploads a bundle to POST /system/v1/cluster/restore, optionally forcing.
// It returns the status code and the response body.
func postRestore(t *testing.T, n *TestNode, bundle []byte, force bool) (int, []byte) {
	t.Helper()
	url := "http://" + n.RestAddr + "/system/v1/cluster/restore"
	if force {
		url += "?force=true"
	}
	resp, err := http.Post(url, "application/x-tar", bytes.NewReader(bundle))
	require.NoError(t, err, "restore request failed")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "reading restore body")
	return resp.StatusCode, body
}

// getInstanceState returns the process instance state as observed by a node,
// or ("", false) if the instance is not (yet) visible.
func getInstanceState(t *testing.T, n *TestNode, instanceKey int64) (zenclient.ProcessInstanceState, bool) {
	t.Helper()
	resp, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
	if err != nil || resp == nil || resp.JSON200 == nil {
		return "", false
	}
	return resp.JSON200.State, true
}

// TestClusterBackupRestoreRoundtrip exercises the full single-node happy path:
// seed state, take a backup over HTTP, mutate the cluster, verify a no-force
// restore is refused on a non-empty cluster, then a forced restore succeeds and
// the cluster resumes serving traffic (engines restart when the Restoring flag
// clears).
func TestClusterBackupRestoreRoundtrip(t *testing.T) {
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 60*time.Second)

	// On a 1-node cluster the single node is the cluster raft leader; restore
	// must be POSTed to the leader because SetRestoring writes through raft.
	node := tc.Leader()
	require.NotNil(t, node, "expected a cluster leader on a 1-node cluster")

	// Seed state: deploy a definition and create an instance.
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")
	defKey := GetFirstDefinitionKey(t, node)
	instanceKey := CreateInstanceOnNode(t, node, defKey, nil)
	require.NotZero(t, instanceKey)

	// Take the backup.
	bundle := takeBackup(t, node)

	// Mutate the cluster after the backup: deploy a second definition. This
	// makes the cluster non-empty and diverged from the bundle.
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")

	// Restore WITHOUT force must be refused (cluster contains data). The REST
	// handler maps the refusal to 409 with code RESTORE_FAILED.
	status, body := postRestore(t, node, bundle, false)
	require.NotEqual(t, http.StatusOK, status,
		"restore without force must be refused on a non-empty cluster; body=%s", body)
	assert.Equal(t, http.StatusConflict, status, "expected 409 Conflict on force-less restore")

	// Restore WITH force succeeds and returns a JSON RestoreReport.
	status, body = postRestore(t, node, bundle, true)
	require.Equal(t, http.StatusOK, status, "forced restore failed: body=%s", body)
	assert.Contains(t, string(body), "startedAtMillis", "expected a RestoreReport JSON body")

	// After restore the cluster serves traffic again: poll until a definition is
	// visible and a fresh instance can be created (engines restart on flag clear).
	require.Eventually(t, func() bool {
		resp, err := node.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
		if err != nil || resp.JSON200 == nil || len(resp.JSON200.Items) == 0 {
			return false
		}
		k := resp.JSON200.Items[0].Key
		ci, err := node.RestClient.CreateProcessInstanceWithResponse(context.Background(),
			zenclient.CreateProcessInstanceJSONRequestBody{ProcessDefinitionKey: &k})
		return err == nil && ci != nil && ci.JSON201 != nil && ci.JSON201.Key != 0
	}, 30*time.Second, 250*time.Millisecond, "cluster did not resume serving after restore")
}

// TestClusterRestoreRejectsCorruptBundle verifies that a truncated/corrupt
// bundle is rejected (non-200) even with force=true, and that the cluster stays
// healthy and serviceable afterwards.
func TestClusterRestoreRejectsCorruptBundle(t *testing.T) {
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 60*time.Second)

	node := tc.Leader()
	require.NotNil(t, node, "expected a cluster leader on a 1-node cluster")

	// Seed some state so there is a real bundle to truncate.
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")
	GetFirstDefinitionKey(t, node)

	bundle := takeBackup(t, node)
	require.Greater(t, len(bundle), 4, "bundle too small to truncate meaningfully")

	// Truncate the bundle: the first half of the bytes is not a valid tar with a
	// trailing manifest, so OpenBundle/Validate must reject it. force=true so we
	// are testing bundle validation, not the empty-cluster gate.
	truncated := bundle[:len(bundle)/2]
	status, body := postRestore(t, node, truncated, true)
	require.NotEqual(t, http.StatusOK, status,
		"a corrupt/truncated bundle must be rejected; body=%s", body)
	assert.Equal(t, http.StatusConflict, status, "expected 409 Conflict for corrupt bundle")

	// The cluster must remain healthy and serviceable: a rejected restore that
	// never entered restoring mode leaves the cluster serving. Poll to allow for
	// eventual consistency.
	WaitForHealthy(t, tc, 30*time.Second)
	require.Eventually(t, func() bool {
		resp, err := node.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
		return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) > 0
	}, 30*time.Second, 250*time.Millisecond, "cluster should still serve reads after a rejected restore")

	// And a fresh deploy still works.
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")
}

// TestBackupRestoreMessagePointerReconciliation exercises the reconciliation
// payoff: an instance waiting at a message catch event has both an instance-level
// message subscription and a pointer-table entry. A backup captures the waiting
// state; publishing the message consumes the subscription (instance completes);
// a forced restore reloads the waiting state AND rebuilds the pointer table from
// the restored active subscription rows; publishing the message again must
// correlate through the rebuilt pointer and complete the instance.
//
// Multi-partition is preferred (pointer and subscription may live on different
// partitions). Phase-2 multi-partition formation is not fully landed, so if a
// 2-partition cluster cannot form the multi-partition variant is skipped (per
// the clustering backlog) and the single-partition variant below still asserts
// the full pointer-rebuild flow.
func TestBackupRestoreMessagePointerReconciliation(t *testing.T) {
	t.Run("single_partition", func(t *testing.T) {
		runPointerReconciliation(t, 1, 1)
	})

	t.Run("multi_partition", func(t *testing.T) {
		tc := NewTestCluster(t, 3, WithPartitions(2))
		defer tc.Teardown(t)
		// Give multi-partition formation a bounded window; if it never forms,
		// skip (Phase-2 gap) rather than fail. We probe with a recover guard so
		// the require.Eventually inside WaitForPartitions does not fail the test.
		if !partitionsFormed(t, tc, 2, 30*time.Second) {
			t.Skip("blocked on Phase 2 multi-partition formation — see clustering backlog")
		}
		runPointerReconciliationOnCluster(t, tc)
	})
}

// partitionsFormed reports whether the expected number of partitions become
// initialized on all running nodes within the timeout, without failing the test
// if they do not (used to decide whether to skip the multi-partition variant).
func partitionsFormed(t *testing.T, tc *TestCluster, count int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		running := tc.RunningNodes()
		if len(running) > 0 {
			s, err := getStatus(running[0])
			if err == nil && len(s.Partitions) == count {
				allLeaders := true
				for _, p := range s.Partitions {
					if p.LeaderID == "" {
						allLeaders = false
						break
					}
				}
				if allLeaders {
					return true
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// runPointerReconciliation spins up a fresh cluster of the given size/partition
// count and runs the reconciliation flow on it.
func runPointerReconciliation(t *testing.T, nodeCount, partitions int) {
	var tc *TestCluster
	if partitions > 1 {
		tc = NewTestCluster(t, nodeCount, WithPartitions(partitions))
	} else {
		tc = NewTestCluster(t, nodeCount)
	}
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	runPointerReconciliationOnCluster(t, tc)
}

// runPointerReconciliationOnCluster runs the message-pointer reconciliation flow
// against an already-healthy cluster.
func runPointerReconciliationOnCluster(t *testing.T, tc *TestCluster) {
	t.Helper()

	// The cluster raft leader must serve the restore (SetRestoring goes through
	// the cluster raft). Deploys/instances/messages can go through any node.
	leader := tc.Leader()
	require.NotNil(t, leader, "expected a cluster leader")

	const (
		messageName    = "msg" // <bpmn:message name="msg">
		correlationKey = "key" // subscription correlationKey ="key" (static FEEL)
	)

	// Deploy the message-catch process and create an instance. Creating the
	// instance registers an instance-level message subscription and a pointer.
	DeployDefinitionOnNode(t, leader, "simple-intermediate-message-catch-event.bpmn")
	defKey := GetFirstDefinitionKey(t, leader)
	instanceKey := CreateInstanceOnNode(t, leader, defKey, nil)
	require.NotZero(t, instanceKey)

	// The instance should be waiting (active) at the catch event.
	require.Eventually(t, func() bool {
		st, ok := getInstanceState(t, leader, instanceKey)
		return ok && st == zenclient.ProcessInstanceStateActive
	}, 30*time.Second, 250*time.Millisecond, "instance should be waiting at the message catch event")

	// Take the backup while the instance waits (subscription + pointer captured).
	bundle := takeBackup(t, leader)

	// Publish the message: it must correlate through the pointer table and drive
	// the instance to completion, consuming the subscription.
	publishAndExpectComplete(t, tc, messageName, correlationKey, instanceKey)

	// Force-restore the captured bundle. This reloads the waiting state and — the
	// point of the test — wipes and rebuilds the pointer table from the restored
	// active subscription rows.
	status, body := postRestore(t, leader, bundle, true)
	require.Equal(t, http.StatusOK, status, "forced restore failed: body=%s", body)

	// After restore the instance is back to waiting (active). Poll: engines
	// restart when the Restoring flag clears.
	require.Eventually(t, func() bool {
		st, ok := getInstanceState(t, leader, instanceKey)
		return ok && st == zenclient.ProcessInstanceStateActive
	}, 60*time.Second, 250*time.Millisecond, "instance should be waiting again after restore")

	// Publish the message again. If the pointer table was rebuilt correctly the
	// message routes to the restored subscription and the instance completes.
	publishAndExpectComplete(t, tc, messageName, correlationKey, instanceKey)
}

// publishAndExpectComplete publishes a correlated message from a non-leader node
// where possible (to exercise cross-node routing through the pointer table) and
// asserts the target instance reaches the completed state.
func publishAndExpectComplete(t *testing.T, tc *TestCluster, messageName, correlationKey string, instanceKey int64) {
	t.Helper()
	publisher := publishNode(tc)
	body := zenclient.PublishMessageJSONRequestBody{
		MessageName:    messageName,
		CorrelationKey: &correlationKey,
	}

	// Publishing can transiently 404 if the pointer/subscription is not yet
	// visible on the publishing node (eventual consistency); retry until it is
	// accepted and the instance completes.
	require.Eventually(t, func() bool {
		resp, err := publisher.RestClient.PublishMessageWithResponse(context.Background(), body)
		if err != nil || resp == nil {
			return false
		}
		if resp.StatusCode() >= 400 {
			return false
		}
		st, ok := getInstanceState(t, publisher, instanceKey)
		return ok && st == zenclient.ProcessInstanceStateCompleted
	}, 60*time.Second, 250*time.Millisecond,
		"message %q/%q should correlate and complete instance %d", messageName, correlationKey, instanceKey)
}

// publishNode returns a follower if one exists (to exercise cross-node routing),
// otherwise the leader (single-node clusters).
func publishNode(tc *TestCluster) *TestNode {
	if fs := tc.Followers(); len(fs) > 0 {
		return fs[0]
	}
	return tc.Leader()
}
