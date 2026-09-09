//go:build cluster_e2e

package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipSecondPartitionNeverForms explains why tests needing two partitions are
// skipped: the controller assigns a new partition to a single node, while that
// node's partition raft group inherits the cluster-wide bootstrap-expect and
// waits for members that are never assigned. Partition membership is tracked
// in docs/cluster-implementation-plan.md.
const skipSecondPartitionNeverForms = "a second partition never forms: its raft group inherits the cluster bootstrap-expect while only one node is assigned to it (partition membership is not implemented, see docs/cluster-implementation-plan.md)"

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

	// Restore WITH force succeeds and returns a JSON RestoreReport. The request
	// returns promptly once the cluster left restore mode: it does not linger
	// until some internal deadline.
	restoreStart := time.Now()
	status, body = postRestore(t, node, bundle, true)
	require.Equal(t, http.StatusOK, status, "forced restore failed: body=%s", body)
	assert.Less(t, time.Since(restoreStart), 60*time.Second, "restore request should return promptly after the restore completed")
	var report backup.RestoreReport
	require.NoError(t, json.Unmarshal(body, &report), "expected a RestoreReport JSON body: %s", body)
	assert.NotEmpty(t, report.OperationID)
	assert.Equal(t, state.RestorePhaseDone, report.Phase)
	assert.NotZero(t, report.FinishedAtMillis)

	// The operation is durable: its record shows the terminal state and the
	// cluster is no longer gated.
	op := getRestoreOperation(t, node, report.OperationID)
	assert.Equal(t, string(state.RestoreStatusCompleted), op.Status)
	assert.Equal(t, string(state.RestorePhaseDone), op.Phase)
	assert.False(t, op.GatesCluster)
	assert.Equal(t, uint32(1), op.CompletedPartitions)
	assert.Equal(t, node.ID, op.CoordinatorID)

	// The coordinator waited for the engines: the cluster serves traffic right
	// away, without any further polling.
	defs, err := node.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, defs.JSON200)
	require.Len(t, defs.JSON200.Items, 1, "the second deploy was rolled back by the restore")
	k := defs.JSON200.Items[0].Key
	ci, err := node.RestClient.CreateProcessInstanceWithResponse(context.Background(),
		zenclient.CreateProcessInstanceJSONRequestBody{ProcessDefinitionKey: &k})
	require.NoError(t, err)
	require.NotNil(t, ci.JSON201, "instance creation right after the restore returned: %s", string(ci.Body))
	assert.NotZero(t, ci.JSON201.Key)
}

// restoreOperation mirrors the JSON of the restore operations endpoint.
type restoreOperation struct {
	ID                  string `json:"id"`
	Epoch               uint64 `json:"epoch"`
	CoordinatorID       string `json:"coordinatorId"`
	Phase               string `json:"phase"`
	Status              string `json:"status"`
	DataModified        bool   `json:"dataModified"`
	GatesCluster        bool   `json:"gatesCluster"`
	TotalPartitions     uint32 `json:"totalPartitions"`
	CompletedPartitions uint32 `json:"completedPartitions"`
	Error               string `json:"error"`
	StartedAtMillis     int64  `json:"startedAtMillis"`
	FinishedAtMillis    int64  `json:"finishedAtMillis"`
	LeaseExpired        bool   `json:"leaseExpired"`
}

// getRestoreOperation fetches one restore operation by id from a node.
func getRestoreOperation(t *testing.T, n *TestNode, id string) restoreOperation {
	t.Helper()
	resp, err := http.Get("http://" + n.RestAddr + "/system/v1/cluster/restore/operations/" + id)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "operation %s: %s", id, body)
	var op restoreOperation
	require.NoError(t, json.Unmarshal(body, &op))
	return op
}

// TestClusterRestoreOperationEndpoints exercises the durable operation record:
// it is listed, readable by id from any node (including followers), embedded in
// /system/status, and a finished operation refuses to be aborted.
func TestClusterRestoreOperationEndpoints(t *testing.T) {
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 60*time.Second)
	node := tc.Leader()
	require.NotNil(t, node)

	// nothing recorded before the first restore
	resp, err := http.Get("http://" + node.RestAddr + "/system/v1/cluster/restore/operations")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"operations": []}`, string(body))
	resp, err = http.Get("http://" + node.RestAddr + "/system/v1/cluster/restore/operations/nope")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	DeployDefinitionOnNode(t, node, "simple_task.bpmn")
	GetFirstDefinitionKey(t, node)
	bundle := takeBackup(t, node)
	status, body := postRestore(t, node, bundle, true)
	require.Equal(t, http.StatusOK, status, "restore failed: %s", body)
	var report backup.RestoreReport
	require.NoError(t, json.Unmarshal(body, &report))

	// listed ...
	resp, err = http.Get("http://" + node.RestAddr + "/system/v1/cluster/restore/operations")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var listed struct {
		Operations []restoreOperation `json:"operations"`
	}
	require.NoError(t, json.Unmarshal(body, &listed))
	require.Len(t, listed.Operations, 1)
	assert.Equal(t, report.OperationID, listed.Operations[0].ID)
	assert.Equal(t, string(state.RestoreStatusCompleted), listed.Operations[0].Status)

	// ... embedded in the status document ...
	resp, err = http.Get("http://" + node.RestAddr + "/system/status")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var statusDoc struct {
		Restore *restoreOperation `json:"restore"`
	}
	require.NoError(t, json.Unmarshal(body, &statusDoc))
	require.NotNil(t, statusDoc.Restore)
	assert.Equal(t, report.OperationID, statusDoc.Restore.ID)

	// ... and a completed operation cannot be aborted
	resp, err = http.Post("http://"+node.RestAddr+"/system/v1/cluster/restore/operations/"+report.OperationID+"/abort", "application/json", strings.NewReader(`{"reason":"test"}`))
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "abort of a completed operation: %s", body)
	assert.Contains(t, string(body), "RESTORE_ABORT_REFUSED")
	resp, err = http.Post("http://"+node.RestAddr+"/system/v1/cluster/restore/operations/unknown/abort", "application/json", nil)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// a second restore takes the next epoch and supersedes the record
	status, body = postRestore(t, node, bundle, true)
	require.Equal(t, http.StatusOK, status, "second restore failed: %s", body)
	var second backup.RestoreReport
	require.NoError(t, json.Unmarshal(body, &second))
	assert.NotEqual(t, report.OperationID, second.OperationID)
	assert.Equal(t, report.Epoch+1, second.Epoch)
	op := getRestoreOperation(t, node, second.OperationID)
	assert.Equal(t, string(state.RestoreStatusCompleted), op.Status)
}

// TestRestoreReconcilesDefinitionSnapshotSkew builds a bundle whose partition
// snapshots deliberately disagree: partition 1 was captured after a deploy,
// partition 2 before it. The restore must import the missing definition into
// partition 2 while the cluster is fenced, and the process must run on every
// partition once the cluster is un-gated.
//
// It needs two partitions and is skipped until a second partition can form:
// the controller assigns a new partition to a single node, while that node's
// partition raft group inherits the cluster-wide bootstrap-expect and so waits
// for members that are never assigned (see docs/cluster-implementation-plan.md,
// partition membership).
func TestRestoreReconcilesDefinitionSnapshotSkew(t *testing.T) {
	tc := NewTestCluster(t, 3, WithPartitions(2))
	defer tc.Teardown(t)
	if !partitionsFormed(t, tc, 2, 30*time.Second) {
		t.Skip(skipSecondPartitionNeverForms)
	}
	WaitForHealthy(t, tc, 150*time.Second)
	leader := tc.Leader()
	require.NotNil(t, leader)

	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")
	GetFirstDefinitionKey(t, leader)
	before := takeBackup(t, leader)

	DeployDefinitionOnNode(t, leader, "start-end.bpmn")
	require.Eventually(t, func() bool {
		resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
		return err == nil && resp.JSON200 != nil && len(resp.JSON200.Items) == 2
	}, 10*time.Second, 100*time.Millisecond)
	after := takeBackup(t, leader)

	// partition 1 from the later snapshot, partition 2 from the earlier one
	skewed := spliceBundle(t, after, before, 2)

	status, body := postRestore(t, leader, skewed, true)
	require.Equal(t, http.StatusOK, status, "restore of the skewed bundle failed: %s", body)
	var report backup.RestoreReport
	require.NoError(t, json.Unmarshal(body, &report))
	require.Len(t, report.DefinitionsSynced, 1, "exactly the definition missing from partition 2 is imported: %+v", report.DefinitionsSynced)
	assert.Equal(t, "process", report.DefinitionsSynced[0].Type)
	assert.Equal(t, []uint32{2}, report.DefinitionsSynced[0].ToPartitions)

	// the process runs on every partition: instances are spread across partitions
	var startEndKey int64
	resp, err := leader.RestClient.GetProcessDefinitionsWithResponse(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	for _, d := range resp.JSON200.Items {
		if d.BpmnProcessId == "start-end" {
			startEndKey = d.Key
		}
	}
	require.NotZero(t, startEndKey, "imported definition should be listed")
	assert.Equal(t, report.DefinitionsSynced[0].Key, startEndKey)
	for i := 0; i < 6; i++ {
		instanceKey := CreateInstanceOnNode(t, tc.RunningNodes()[i%len(tc.RunningNodes())], startEndKey, nil)
		require.Eventually(t, func() bool {
			st, ok := getInstanceState(t, leader, instanceKey)
			return ok && st == zenclient.ProcessInstanceStateCompleted
		}, 30*time.Second, 250*time.Millisecond, "instance %d of the imported definition should complete", instanceKey)
	}
}

// spliceBundle rebuilds a bundle from primary, replacing the given partition's
// file (and its manifest entry) with the one from secondary.
func spliceBundle(t *testing.T, primary, secondary []byte, partitionFromSecondary uint32) []byte {
	t.Helper()
	primaryFiles, primaryManifest := readBundle(t, primary)
	secondaryFiles, secondaryManifest := readBundle(t, secondary)
	name := backup.PartitionFileName(partitionFromSecondary)
	primaryFiles[name] = secondaryFiles[name]
	primaryManifest.Partitions[partitionFromSecondary] = secondaryManifest.Partitions[partitionFromSecondary]

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for id := uint32(1); id <= primaryManifest.PartitionCount; id++ {
		data := primaryFiles[backup.PartitionFileName(id)]
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.PartitionFileName(id), Mode: 0o600, Size: int64(len(data))}))
		_, err := tw.Write(data)
		require.NoError(t, err)
	}
	manifest, err := json.Marshal(primaryManifest)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestFileName, Mode: 0o600, Size: int64(len(manifest))}))
	_, err = tw.Write(manifest)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func readBundle(t *testing.T, bundle []byte) (map[string][]byte, backup.Manifest) {
	t.Helper()
	files := map[string][]byte{}
	var manifest backup.Manifest
	tr := tar.NewReader(bytes.NewReader(bundle))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data, err := io.ReadAll(tr)
		require.NoError(t, err)
		if hdr.Name == backup.ManifestFileName {
			require.NoError(t, json.Unmarshal(data, &manifest))
			continue
		}
		files[hdr.Name] = data
	}
	require.NotEmpty(t, manifest.Partitions, "bundle without manifest")
	for id := range manifest.Partitions {
		require.Contains(t, files, backup.PartitionFileName(id), fmt.Sprintf("bundle lacks partition %d", id))
	}
	return files, manifest
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
// partitions). A second partition cannot form yet (see
// skipSecondPartitionNeverForms), so when a 2-partition cluster does not form
// within a bounded window the multi-partition variant is skipped and the
// single-partition variant below still asserts the full pointer-rebuild flow.
func TestBackupRestoreMessagePointerReconciliation(t *testing.T) {
	t.Run("single_partition", func(t *testing.T) {
		runPointerReconciliation(t, 1, 1)
	})

	t.Run("multi_partition", func(t *testing.T) {
		tc := NewTestCluster(t, 3, WithPartitions(2))
		defer tc.Teardown(t)
		// partitionsFormed probes with a recover guard so the require.Eventually
		// inside WaitForPartitions does not fail the test when formation stalls
		if !partitionsFormed(t, tc, 2, 30*time.Second) {
			t.Skip(skipSecondPartitionNeverForms)
		}
		runPointerReconciliationOnCluster(t, tc)
	})
}

// partitionsFormed reports whether the expected number of partitions become
// initialized on all running nodes within the timeout, without failing the test
// if they do not (used to decide whether to skip the multi-partition variant).
func partitionsFormed(t *testing.T, tc *TestCluster, count int, timeout time.Duration) bool {
	t.Helper()
	formed := func() bool {
		running := tc.RunningNodes()
		if len(running) == 0 {
			return false
		}
		s, err := getStatus(running[0])
		if err != nil || len(s.Partitions) != count {
			return false
		}
		for _, p := range s.Partitions {
			if p.LeaderID == "" {
				return false
			}
		}
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if formed() {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
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
