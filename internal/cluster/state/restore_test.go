package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreAcquireIsExclusive(t *testing.T) {
	c := Cluster{}
	require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
	op := c.Restore
	assert.Equal(t, "op-1", op.ID)
	assert.Equal(t, uint64(1), op.Epoch)
	assert.Equal(t, RestoreStatusActive, op.Status)
	assert.Equal(t, RestorePhasePending, op.Phase)
	assert.Equal(t, int64(31_000), op.LeaseExpiresAtMillis)
	assert.True(t, c.RestoreInProgress())

	// a second coordinator is refused while the lease is alive, state untouched
	err := c.ApplyRestoreChange(acquire("op-2", "node-b", 2_000, 30_000))
	var rejected *RestoreRejectedError
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, "op-1", rejected.Current.ID)
	assert.Contains(t, err.Error(), "op-1")
	assert.Equal(t, op, c.Restore)

	// once the lease expired the operation can be taken over; the epoch moves
	// on so the old owner is fenced out
	require.NoError(t, c.ApplyRestoreChange(acquire("op-2", "node-b", 31_000, 30_000)))
	assert.Equal(t, "op-2", c.Restore.ID)
	assert.Equal(t, uint64(2), c.Restore.Epoch)
	assert.Equal(t, "op-1", c.Restore.PreviousOperationID)
	assert.False(t, c.Restore.Owns("op-1", 1))
	assert.True(t, c.Restore.Owns("op-2", 2))
}

func TestRestoreAcquireRequiresOperationID(t *testing.T) {
	c := Cluster{}
	err := c.ApplyRestoreChange(acquire("", "node-a", 1, 1))
	var rejected *RestoreRejectedError
	require.ErrorAs(t, err, &rejected)
	assert.False(t, c.Restore.Exists())
}

func TestRestoreUpdateIsFencedByToken(t *testing.T) {
	c := Cluster{}
	require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))

	var rejected *RestoreRejectedError
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 2, Phase: RestorePhaseQuiescing, NowMillis: 2_000}), &rejected, "wrong epoch")
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "other", Epoch: 1, Phase: RestorePhaseQuiescing, NowMillis: 2_000}), &rejected, "wrong id")
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: "BOGUS", NowMillis: 2_000}), &rejected, "unknown phase")
	assert.Equal(t, RestorePhasePending, c.Restore.Phase)

	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseLoading, CompletedPartitions: 1, TotalPartitions: 3, NowMillis: 5_000, LeaseMillis: 10_000}))
	assert.Equal(t, RestorePhaseLoading, c.Restore.Phase)
	assert.Equal(t, uint32(1), c.Restore.CompletedPartitions)
	assert.Equal(t, uint32(3), c.Restore.TotalPartitions)
	assert.Equal(t, int64(15_000), c.Restore.LeaseExpiresAtMillis, "update renews the lease")
	assert.True(t, c.Restore.DataModified, "loading marks the data as modified")

	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseQuiescing, NowMillis: 6_000}), &rejected, "phase cannot move backwards")
	// a heartbeat re-sending the current phase is fine
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseLoading, CompletedPartitions: 1, NowMillis: 7_000, LeaseMillis: 10_000}))
	assert.Equal(t, int64(17_000), c.Restore.LeaseExpiresAtMillis)
}

func TestRestoreCompleteLiftsGateAndAllowsNextRestore(t *testing.T) {
	c := Cluster{}
	require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseResuming, NowMillis: 2_000}))
	assert.False(t, c.RestoreInProgress(), "the RESUMING phase lifts the gate while the operation is still owned")
	assert.True(t, c.Restore.Active())

	var rejected *RestoreRejectedError
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionComplete, OperationID: "op-1", Epoch: 9, NowMillis: 3_000}), &rejected)
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionComplete, OperationID: "op-1", Epoch: 1, NowMillis: 3_000}))
	assert.Equal(t, RestoreStatusCompleted, c.Restore.Status)
	assert.Equal(t, RestorePhaseDone, c.Restore.Phase)
	assert.Equal(t, int64(3_000), c.Restore.FinishedAtMillis)
	assert.False(t, c.RestoreInProgress())
	assert.True(t, c.Restore.Terminal())

	// nothing to update or complete on a finished operation
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseDone, NowMillis: 4_000}), &rejected)

	require.NoError(t, c.ApplyRestoreChange(acquire("op-2", "node-a", 5_000, 30_000)))
	assert.Equal(t, uint64(2), c.Restore.Epoch)
	assert.Empty(t, c.Restore.PreviousOperationID, "a completed restore is not resumed")
	assert.False(t, c.Restore.DataModified)
}

func TestRestoreFailureKeepsClusterGatedOnlyAfterDataWasModified(t *testing.T) {
	t.Run("failure before loading lifts the gate", func(t *testing.T) {
		c := Cluster{}
		require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseValidating, NowMillis: 2_000}))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionFail, OperationID: "op-1", Epoch: 1, Error: "cluster contains data", NowMillis: 3_000}))
		assert.Equal(t, RestoreStatusFailed, c.Restore.Status)
		assert.Equal(t, "cluster contains data", c.Restore.Error)
		assert.Equal(t, RestorePhaseValidating, c.Restore.Phase, "the failing phase is kept for diagnosis")
		assert.False(t, c.RestoreInProgress())
	})

	t.Run("failure after loading keeps the gate until a retry succeeds", func(t *testing.T) {
		c := Cluster{}
		require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseLoading, NowMillis: 2_000}))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionFail, OperationID: "op-1", Epoch: 1, Error: "partition 2 unreachable", NowMillis: 3_000}))
		assert.True(t, c.RestoreInProgress(), "partially restored data keeps the cluster gated")
		assert.False(t, c.Restore.Active(), "a failed operation has no owner")

		// the retry inherits the gate; failing it before loading must not lift it
		require.NoError(t, c.ApplyRestoreChange(acquire("op-2", "node-b", 4_000, 30_000)))
		assert.Equal(t, "op-1", c.Restore.PreviousOperationID)
		assert.True(t, c.Restore.DataModified)
		assert.True(t, c.RestoreInProgress())
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionFail, OperationID: "op-2", Epoch: 2, Error: "bundle rejected", NowMillis: 5_000}))
		assert.True(t, c.RestoreInProgress(), "inherited partial data keeps the cluster gated")

		// a successful retry lifts it
		require.NoError(t, c.ApplyRestoreChange(acquire("op-3", "node-b", 6_000, 30_000)))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-3", Epoch: 3, Phase: RestorePhaseResuming, NowMillis: 7_000}))
		assert.False(t, c.RestoreInProgress())
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionComplete, OperationID: "op-3", Epoch: 3, NowMillis: 8_000}))
		assert.False(t, c.RestoreInProgress())
	})

	t.Run("failure while resuming does not re-gate a fully loaded cluster", func(t *testing.T) {
		c := Cluster{}
		require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseResuming, NowMillis: 2_000}))
		require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionFail, OperationID: "op-1", Epoch: 1, Error: "engines did not come back", NowMillis: 3_000}))
		assert.False(t, c.RestoreInProgress())
	})
}

func TestRestoreAbort(t *testing.T) {
	c := Cluster{}
	var rejected *RestoreRejectedError
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionAbort, OperationID: "op-1", NowMillis: 1}), &rejected, "nothing to abort")

	require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 1_000, 30_000)))
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseLoading, NowMillis: 2_000}))
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionAbort, OperationID: "op-9", NowMillis: 3_000}), &rejected, "unknown id")

	// the operator aborts a dead coordinator's restore: the gate is lifted even
	// though data was modified, and the old owner is fenced out
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionAbort, OperationID: "op-1", Error: "coordinator crashed", NowMillis: 3_000}))
	assert.Equal(t, RestoreStatusAborted, c.Restore.Status)
	assert.Equal(t, "coordinator crashed", c.Restore.Error)
	assert.True(t, c.Restore.DataModified, "the record keeps what happened")
	assert.False(t, c.RestoreInProgress())
	assert.False(t, c.Restore.Owns("op-1", 1))
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUpdate, OperationID: "op-1", Epoch: 1, Phase: RestorePhaseReconciling, NowMillis: 4_000}), &rejected)
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionAbort, OperationID: "op-1", NowMillis: 5_000}), &rejected, "already aborted")

	// a failed operation can be aborted too
	require.NoError(t, c.ApplyRestoreChange(acquire("op-2", "node-a", 6_000, 30_000)))
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionFail, OperationID: "op-2", Epoch: 2, Error: "boom", NowMillis: 7_000}))
	require.NoError(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionAbort, OperationID: "op-2", NowMillis: 8_000}))
	assert.Equal(t, RestoreStatusAborted, c.Restore.Status)
	assert.Equal(t, "boom", c.Restore.Error, "an abort without a reason keeps the failure message")
}

func TestRestoreUnknownActionIsRejected(t *testing.T) {
	c := Cluster{}
	var rejected *RestoreRejectedError
	assert.ErrorAs(t, c.ApplyRestoreChange(RestoreChange{Action: RestoreActionUnknown}), &rejected)
}

func TestRestorePhaseOrdering(t *testing.T) {
	assert.Less(t, RestorePhasePending.Index(), RestorePhaseQuiescing.Index())
	assert.Less(t, RestorePhaseQuiescing.Index(), RestorePhaseValidating.Index())
	assert.Less(t, RestorePhaseValidating.Index(), RestorePhaseLoading.Index())
	assert.Less(t, RestorePhaseLoading.Index(), RestorePhaseReconciling.Index())
	assert.Less(t, RestorePhaseReconciling.Index(), RestorePhaseResuming.Index())
	assert.Less(t, RestorePhaseResuming.Index(), RestorePhaseDone.Index())
	assert.Equal(t, -1, RestorePhase("nope").Index())
	assert.False(t, RestorePhaseValidating.ModifiesData())
	assert.True(t, RestorePhaseLoading.ModifiesData())
	assert.True(t, RestorePhaseDone.ModifiesData())
}

func TestActivePartitionLeader(t *testing.T) {
	c := Cluster{
		Partitions: map[uint32]Partition{1: {Id: 1, LeaderId: "leader"}, 2: {Id: 2}},
		Nodes: map[string]Node{
			"leader": {Id: "leader", Addr: "leader:1", State: NodeStateStarted, Partitions: map[uint32]NodePartition{
				1: {Id: 1, Role: RoleLeader, State: NodePartitionStateInitialized},
			}},
		},
	}
	addr, id := c.ActivePartitionLeader(1)
	assert.Equal(t, "leader:1", addr)
	assert.Equal(t, "leader", id)

	addr, id = c.ActivePartitionLeader(2)
	assert.Empty(t, addr, "partition without leader")
	assert.Empty(t, id)
	addr, _ = c.ActivePartitionLeader(3)
	assert.Empty(t, addr, "unknown partition")

	// the recorded leader lost the leader role (its heartbeat failed and the
	// cluster leader cleared it): do not route to it
	node := c.Nodes["leader"]
	node.Partitions[1] = NodePartition{Id: 1, State: NodePartitionStateInitialized}
	c.Nodes["leader"] = node
	addr, _ = c.ActivePartitionLeader(1)
	assert.Empty(t, addr)

	node.Partitions[1] = NodePartition{Id: 1, Role: RoleLeader, State: NodePartitionStateInitialized}
	node.State = NodeStateShutdown
	c.Nodes["leader"] = node
	addr, _ = c.ActivePartitionLeader(1)
	assert.Empty(t, addr, "shut down node")

	delete(node.Partitions, 1)
	node.State = NodeStateStarted
	c.Nodes["leader"] = node
	addr, _ = c.ActivePartitionLeader(1)
	assert.Empty(t, addr, "node left the partition")
}

func TestDefinitionSubscriptionPartitionIsStable(t *testing.T) {
	c := Cluster{Partitions: map[uint32]Partition{1: {Id: 1}, 2: {Id: 2}, 3: {Id: 3}}}
	first := c.DefinitionSubscriptionPartition("order-process")
	assert.Contains(t, []uint32{1, 2, 3}, first)
	for i := 0; i < 10; i++ {
		assert.Equal(t, first, c.DefinitionSubscriptionPartition("order-process"))
	}
	assert.Equal(t, uint32(0), Cluster{}.DefinitionSubscriptionPartition("order-process"))
}

func acquire(id, coordinator string, now, lease int64) RestoreChange {
	return RestoreChange{
		Action:          RestoreActionAcquire,
		OperationID:     id,
		CoordinatorID:   coordinator,
		TotalPartitions: 2,
		NowMillis:       now,
		LeaseMillis:     lease,
	}
}

func TestApplyLegacyRestoringFlag(t *testing.T) {
	c := Cluster{}
	c.ApplyLegacyRestoringFlag(false, 1)
	assert.False(t, c.Restore.Exists(), "a clear without a legacy restore is a no-op")

	c.ApplyLegacyRestoringFlag(true, 2)
	assert.True(t, c.RestoreInProgress())
	assert.Equal(t, LegacyRestoreOperationID, c.Restore.ID)
	assert.Equal(t, uint64(1), c.Restore.Epoch)
	assert.Equal(t, RestoreStatusFailed, c.Restore.Status)
	assert.True(t, c.Restore.DataModified)
	assert.False(t, c.Restore.Active(), "nobody owns a legacy restore: it must be retried or aborted")

	// idempotent while gated
	c.ApplyLegacyRestoringFlag(true, 3)
	assert.Equal(t, uint64(1), c.Restore.Epoch)

	// a retry supersedes it and inherits the gate
	require.NoError(t, c.ApplyRestoreChange(acquire("op-1", "node-a", 4, 30_000)))
	assert.Equal(t, LegacyRestoreOperationID, c.Restore.PreviousOperationID)
	assert.True(t, c.Restore.DataModified)
	// a stray legacy flag cannot disturb a modern operation
	c.ApplyLegacyRestoringFlag(true, 5)
	assert.Equal(t, "op-1", c.Restore.ID)
	c.ApplyLegacyRestoringFlag(false, 6)
	assert.Equal(t, "op-1", c.Restore.ID)
	assert.Equal(t, RestoreStatusActive, c.Restore.Status)

	// the legacy clear completes a legacy restore
	c = Cluster{}
	c.ApplyLegacyRestoringFlag(true, 1)
	c.ApplyLegacyRestoringFlag(false, 2)
	assert.False(t, c.RestoreInProgress())
	assert.Equal(t, RestoreStatusCompleted, c.Restore.Status)
	assert.Equal(t, RestorePhaseDone, c.Restore.Phase)
}
