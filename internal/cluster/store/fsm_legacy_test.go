package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFsmMigratesLegacyRestoringFlagFromLog verifies that replaying the
// maintenance entries of a binary that predates restore operations keeps the
// cluster gated: an interrupted legacy restore becomes a FAILED operation that
// modified data, and its clear entry completes it.
func TestFsmMigratesLegacyRestoringFlagFromLog(t *testing.T) {
	s, _ := newMustTestStore(t, config.Cluster{NodeId: "legacy", Raft: config.ClusterRaft{Dir: t.TempDir()}})
	fsm := NewFSM(s)

	res := fsm.applyMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: new(true)})
	result, ok := res.(RestoreApplyResult)
	require.True(t, ok)
	assert.Nil(t, result.Rejected)
	cs := s.ClusterState()
	assert.True(t, cs.RestoreInProgress(), "an interrupted legacy restore keeps the cluster gated")
	assert.Equal(t, state.LegacyRestoreOperationID, cs.Restore.ID)
	assert.Equal(t, state.RestoreStatusFailed, cs.Restore.Status)
	assert.True(t, cs.Restore.DataModified)
	assert.Equal(t, uint64(1), cs.Restore.Epoch)

	// replaying the flag again is idempotent
	fsm.applyMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: new(true)})
	assert.Equal(t, uint64(1), s.ClusterState().Restore.Epoch)

	// a new-style operation can take over and is not disturbed by a later legacy flag
	fsm.applyMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: new(false)})
	cs = s.ClusterState()
	assert.False(t, cs.RestoreInProgress(), "the legacy clear completes the legacy restore")
	assert.Equal(t, state.RestoreStatusCompleted, cs.Restore.Status)
}

func TestFsmRestoreMigratesLegacySnapshot(t *testing.T) {
	s, _ := newMustTestStore(t, config.Cluster{NodeId: "legacy", Raft: config.ClusterRaft{Dir: t.TempDir()}})
	fsm := NewFSM(s)

	legacy := map[string]any{
		"clusterState": map[string]any{
			"clusterConfig": map[string]any{"desiredPartitions": 1},
			"partitions":    map[string]any{"1": map[string]any{"id": 1, "leaderId": "n1"}},
			"nodes":         map[string]any{},
			"restoring":     true,
		},
	}
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(raw))))
	cs := s.ClusterState()
	assert.True(t, cs.RestoreInProgress(), "a legacy snapshot taken mid-restore keeps the cluster gated")
	assert.Equal(t, state.LegacyRestoreOperationID, cs.Restore.ID)
	assert.Equal(t, "n1", cs.Partitions[1].LeaderId, "the rest of the snapshot is decoded as before")

	// a current snapshot round-trips unchanged
	current := fsmSnapshot{ClusterState: state.Cluster{
		Partitions: map[uint32]state.Partition{},
		Nodes:      map[string]state.Node{},
		Restore:    state.RestoreOperation{ID: "op-1", Epoch: 3, Status: state.RestoreStatusCompleted, Phase: state.RestorePhaseDone},
	}}
	raw, err = json.Marshal(current)
	require.NoError(t, err)
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(raw))))
	assert.Equal(t, current.ClusterState.Restore, s.ClusterState().Restore)
	assert.False(t, s.ClusterState().RestoreInProgress())
}

// TestFollowerRestoredFromAnOlderSnapshotRecordsTheFeaturesAnnouncedAgain
// verifies the follower's side of recovering from a snapshot that a leader
// rolled back to a binary without features took: the restored state records
// no features for anybody, and the entries the others send afterwards (the
// rolled-back node's announcement once it runs the newer binary, and the
// announcement a peer owes for that change, see the controller's
// announcementOwedToOthers) bring every feature back.
func TestFollowerRestoredFromAnOlderSnapshotRecordsTheFeaturesAnnouncedAgain(t *testing.T) {
	s, _ := newMustTestStore(t, config.Cluster{NodeId: "follower", Raft: config.ClusterRaft{Dir: t.TempDir()}})
	fsm := NewFSM(s)
	members := []string{"follower", "rolled-back", "peer"}
	olderNode := func(id string) map[string]any {
		// the shape an older binary writes: no features, no feature changes
		return map[string]any{"id": id, "state": int(state.NodeStateStarted), "partitions": map[string]any{}, "protocolVersion": state.CurrentProtocolVersion}
	}
	older := map[string]any{"clusterState": map[string]any{
		"clusterConfig": map[string]any{"desiredPartitions": 1},
		"partitions":    map[string]any{},
		"nodes":         map[string]any{"follower": olderNode("follower"), "rolled-back": olderNode("rolled-back"), "peer": olderNode("peer")},
	}}
	raw, err := json.Marshal(older)
	require.NoError(t, err)
	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(raw))))
	assert.Empty(t, s.ClusterState().CommonFeatures(members), "the restored state records no features for anybody")

	announce := func(id string) {
		fsm.applyNodeChange(&proto.NodeChange{NodeId: new(id), ProtocolVersion: new(state.CurrentProtocolVersion), Features: state.CurrentFeatures()})
	}
	announce("follower")
	announce("rolled-back")
	assert.Empty(t, s.ClusterState().CommonFeatures(members), "the peer's features are still missing")
	assert.Equal(t, uint64(1), s.ClusterState().Nodes["rolled-back"].FeatureChanges, "the change the peers answer")

	announce("peer")
	assert.Equal(t, state.CurrentFeatures(), s.ClusterState().CommonFeatures(members), "the peer's repeated announcement completes the follower's state")
}

func TestWriteRestoreChangeHonoursCancelledContext(t *testing.T) {
	s, _ := newMustTestStore(t, config.Cluster{NodeId: "ctx", Raft: config.ClusterRaft{Dir: t.TempDir()}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.WriteRestoreChange(ctx, &proto.RestoreOperationChange{
		Action:      proto.RestoreOperationChange_RESTORE_ACTION_ACQUIRE.Enum(),
		OperationId: new("op-1"),
	})
	require.ErrorIs(t, err, context.Canceled)
}
