package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreErrorStatus(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode int
		wantErr  string
	}{
		{"not the raft leader", errors.Join(errors.New("apply failed"), zenerr.ErrNotLeader), http.StatusServiceUnavailable, restoreCodeNotLeader},
		{"another restore in progress", backup.ErrRestoreInProgress, http.StatusConflict, restoreCodeRefused},
		{"invalid bundle", errors.Join(errors.New("truncated"), backup.ErrInvalidBundle), http.StatusConflict, restoreCodeRefused},
		{"non-empty cluster", &backup.PhaseError{Phase: state.RestorePhaseValidating, Err: backup.ErrClusterNotEmpty}, http.StatusConflict, restoreCodeRefused},
		{"phase deadline", &backup.PhaseError{Phase: state.RestorePhaseLoading, Err: context.DeadlineExceeded}, http.StatusGatewayTimeout, restoreCodeFailed},
		{"phase failure", &backup.PhaseError{Phase: state.RestorePhaseReconciling, Err: errors.New("boom")}, http.StatusInternalServerError, restoreCodeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, code := restoreErrorStatus(tc.err)
			assert.Equal(t, tc.wantCode, status)
			assert.Equal(t, tc.wantErr, code)
		})
	}
}

func TestBuildRestoreOperationViewDerivesLeaseAndGate(t *testing.T) {
	op := state.RestoreOperation{
		ID: "op-1", Epoch: 3, CoordinatorID: "node-a", Phase: state.RestorePhaseLoading, Status: state.RestoreStatusActive,
		Force: true, DataModified: true, TotalPartitions: 4, CompletedPartitions: 1,
		StartedAtMillis: 1_000, UpdatedAtMillis: 2_000, LeaseExpiresAtMillis: 5_000, PreviousOperationID: "op-0",
	}
	view := buildRestoreOperationView(op, time.UnixMilli(4_000))
	assert.False(t, view.LeaseExpired)
	assert.True(t, view.GatesCluster)
	assert.Equal(t, "op-0", view.PreviousOperationID)

	view = buildRestoreOperationView(op, time.UnixMilli(6_000))
	assert.True(t, view.LeaseExpired, "the lease is judged against the wall clock at read time")

	op.Status = state.RestoreStatusCompleted
	op.Phase = state.RestorePhaseDone
	op.FinishedAtMillis = 7_000
	view = buildRestoreOperationView(op, time.UnixMilli(8_000))
	assert.False(t, view.GatesCluster)
	assert.False(t, view.LeaseExpired, "a finished operation has no lease")

	encoded, err := json.Marshal(view)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"id": "op-1", "epoch": 3, "coordinatorId": "node-a", "phase": "DONE", "status": "COMPLETED",
		"force": true, "dataModified": true, "gatesCluster": false, "totalPartitions": 4, "completedPartitions": 1,
		"startedAtMillis": 1000, "updatedAtMillis": 2000, "finishedAtMillis": 7000, "leaseExpiresAtMillis": 5000,
		"leaseExpired": false, "previousOperationId": "op-0"
	}`, string(encoded))
}

func TestClusterStatusViewIncludesRestoreOnlyWhenOneExists(t *testing.T) {
	c := state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}}
	b, err := json.Marshal(buildClusterStatusView(c))
	require.NoError(t, err)
	assert.NotContains(t, string(b), `"restore"`)

	c.Restore = state.RestoreOperation{ID: "op-1", Epoch: 1, Status: state.RestoreStatusActive, Phase: state.RestorePhaseQuiescing}
	b, err = json.Marshal(buildClusterStatusView(c))
	require.NoError(t, err)
	assert.Contains(t, string(b), `"restore":{"id":"op-1"`)
	assert.Contains(t, string(b), `"gatesCluster":true`)
}
