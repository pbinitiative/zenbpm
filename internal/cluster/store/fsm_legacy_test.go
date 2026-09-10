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
