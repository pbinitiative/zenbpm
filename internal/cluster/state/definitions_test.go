package state

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenflake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocateProcessDefinitionAssignsConsecutiveVersions(t *testing.T) {
	var c Cluster

	first, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "aaa", Sequence: 100, NowMillis: 10,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(1), first.Version)
	assert.Equal(t, "aaa", first.Checksum)
	assert.Equal(t, int64(10), first.AllocatedAtMillis)
	assert.Equal(t, uint32(zenflake.GlobalResourceNode), zenflake.GetPartitionId(first.Key), "definition keys are global resources")

	second, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "bbb", Sequence: 200, VersionTag: "v2", NowMillis: 20,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(2), second.Version)
	assert.NotEqual(t, first.Key, second.Key)
	assert.Equal(t, "v2", second.VersionTag)

	// another process id has its own version sequence
	other, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "invoice", Checksum: "aaa", Sequence: 300, NowMillis: 30,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), other.Version)
	assert.NotEqual(t, second.Key, other.Key)

	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest)
	assert.Equal(t, map[string]ProcessDefinitionAllocation{"v2": second}, c.ProcessDefinitions["order"].VersionTags)
}

func TestAllocateProcessDefinitionReusesLatestForIdenticalContent(t *testing.T) {
	var c Cluster
	first, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, NowMillis: 10})
	require.NoError(t, err)

	// a retry (or a concurrent identical deployment) is a later log entry
	// but must end up with the allocation that already exists
	again, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 101, NowMillis: 20})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, first, again)

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, NowMillis: 30})
	require.NoError(t, err)

	// older content is deduplicated against the latest version only: deployed
	// after another revision it becomes a new version, the latest one again
	redeployed, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 300, NowMillis: 40})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(3), redeployed.Version)
	assert.NotEqual(t, first.Key, redeployed.Key)
	assert.Equal(t, redeployed, c.ProcessDefinitions["order"].Latest)
}

// TestAllocateProcessDefinitionRepeatsTaggedContentUnderItsTag verifies that
// a tagged deployment whose content is already allocated under that tag is
// answered with that allocation whatever was allocated in between (a retry
// after a partial failure), while different content under the tag is
// rejected.
func TestAllocateProcessDefinitionRepeatsTaggedContentUnderItsTag(t *testing.T) {
	var c Cluster
	first, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 100, NowMillis: 10})
	require.NoError(t, err)
	second, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, NowMillis: 20})
	require.NoError(t, err)
	require.Equal(t, int32(2), second.Version)

	retried, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 300, NowMillis: 30})
	require.NoError(t, err, "the retry must not be rejected for its own version tag")
	assert.True(t, existing)
	assert.Equal(t, first, retried)
	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest, "a retry never moves the latest version backwards")

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "ccc", VersionTag: "stable", Sequence: 400, NowMillis: 40})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "the tag is taken by other content")
	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest, "nothing is allocated on a rejection")

	// the same content without its tag is a new deployment
	untagged, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 500, NowMillis: 50})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(3), untagged.Version)
	assert.Empty(t, untagged.VersionTag)
}

func TestAllocateProcessDefinitionKeysAreUniqueWhateverTheWritersClocksSay(t *testing.T) {
	var c Cluster
	keys := map[int64]struct{}{}
	// writers with skewed clocks, some in the same millisecond, some behind
	// the allocation clock, and log indexes 4096 apart (equal low bits)
	for i, nowMillis := range []int64{1000, 1000, 999, 1000, 500, 5000, 4000} {
		allocation, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
			ProcessID: fmt.Sprintf("process-%d", i), Checksum: "aaa", Sequence: uint64(i * 4096), NowMillis: nowMillis,
		})
		require.NoError(t, err)
		_, taken := keys[allocation.Key]
		assert.False(t, taken, "key %d allocated twice", allocation.Key)
		keys[allocation.Key] = struct{}{}
		assert.Equal(t, uint32(zenflake.GlobalResourceNode), zenflake.GetPartitionId(allocation.Key))
	}
	assert.Equal(t, int64(5001), c.ProcessDefinitionKeyClockMillis, "the allocation clock never moves backwards")
}

func TestAllocateProcessDefinitionRejectsReusedVersionTag(t *testing.T) {
	var c Cluster
	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, VersionTag: "stable", NowMillis: 10})
	require.NoError(t, err)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, NowMillis: 20})
	require.NoError(t, err)

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "ccc", Sequence: 300, VersionTag: "stable", NowMillis: 30})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "the tag stays unique across the whole history, not only the latest version")
	assert.Equal(t, "order", rejected.ProcessID)
	assert.Equal(t, int32(2), c.ProcessDefinitions["order"].Latest.Version, "nothing is allocated on a rejection")

	// the same tag on another process is fine
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "invoice", Checksum: "ccc", Sequence: 400, VersionTag: "stable", NowMillis: 40})
	require.NoError(t, err)
}

func TestAllocateProcessDefinitionRejectsInvalidRequests(t *testing.T) {
	var c Cluster
	var rejected *ProcessDefinitionAllocationRejectedError

	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{Checksum: "aaa", Sequence: 100})
	require.ErrorAs(t, err, &rejected)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Sequence: 100})
	require.ErrorAs(t, err, &rejected)
	assert.Empty(t, c.ProcessDefinitions)
}

func TestAllocateProcessDefinitionRejectsNonPositiveTimestamps(t *testing.T) {
	for _, timestamp := range []int64{0, -1} {
		var c Cluster
		_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", NowMillis: timestamp})
		var rejected *ProcessDefinitionAllocationRejectedError
		require.ErrorAs(t, err, &rejected)
		assert.Equal(t, "timestamp must be positive", rejected.Reason)
		assert.Empty(t, c.ProcessDefinitions)
		assert.Zero(t, c.ProcessDefinitionKeyClockMillis)
	}
}

func TestAllocateProcessDefinitionCatchesUpWithObservedPartitionState(t *testing.T) {
	// definitions deployed before allocations were replicated exist on the
	// partitions only; the first allocation after the upgrade continues
	// their version sequence instead of restarting at 1 and reserves every
	// tag the partitions hold
	var c Cluster
	observed := []ProcessDefinitionAllocation{
		{Key: 7, Version: 3, Checksum: "ccc", VersionTag: "legacy"},
		{Key: 5, Version: 1, Checksum: "aaa", VersionTag: "first"},
	}

	next, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ddd", Sequence: 100, Observed: observed, NowMillis: 10,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(4), next.Version)
	assert.Equal(t, map[string]ProcessDefinitionAllocation{"legacy": observed[0], "first": observed[1]}, c.ProcessDefinitions["order"].VersionTags, "the observed tags are protected too")
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "eee", VersionTag: "first", Sequence: 150, NowMillis: 15,
	})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "a tag an older version holds on the partitions is taken")

	// an observed definition with the same content as the request is
	// returned as the existing one without allocating
	var fresh Cluster
	same, existing, err := fresh.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ccc", Sequence: 100, Observed: observed, NowMillis: 10,
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, observed[0], same)
	assert.Equal(t, observed[0], fresh.ProcessDefinitions["order"].Latest, "the observed definition is recorded as the latest")

	// the order of the observations does not matter to the result
	var shuffled Cluster
	_, _, err = shuffled.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ccc", Sequence: 100, Observed: []ProcessDefinitionAllocation{observed[1], observed[0]}, NowMillis: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, fresh, shuffled)

	// the replicated state is authoritative once it is ahead: a partition
	// lagging behind (a fan-out still in flight) never moves the sequence
	// backwards, but a tag it holds on an older version replaces the tag
	// recorded for an allocation that may never have reached it
	behind := []ProcessDefinitionAllocation{{Key: 1, Version: 1, Checksum: "aaa", VersionTag: "legacy"}}
	later, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "fff", Sequence: 200, Observed: behind, NowMillis: 20,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(5), later.Version)
	assert.Equal(t, behind[0], c.ProcessDefinitions["order"].VersionTags["legacy"], "the partitions are authoritative for the tags they hold")
}

// TestCatchUpReservesASharedTagForTheNewestObservedDefinition verifies that
// a tag the partitions hold on different definitions (a history that
// diverged before allocations were replicated) is reserved for the newest of
// them, the definition the latest resolution picks too, whatever the order
// of the observations, on both the allocation and the reset path.
func TestCatchUpReservesASharedTagForTheNewestObservedDefinition(t *testing.T) {
	// two partitions accepted different content under the tag at the same
	// version: the higher key wins, like the latest resolution
	sameVersion := []ProcessDefinitionAllocation{
		{Key: 5, Version: 2, Checksum: "aaa", VersionTag: "shared"},
		{Key: 9, Version: 2, Checksum: "bbb", VersionTag: "shared"},
	}
	// a partition that missed a deployment numbered the tagged one lower
	// than another partition did: the higher version wins
	differentVersions := []ProcessDefinitionAllocation{
		{Key: 9, Version: 1, Checksum: "aaa", VersionTag: "shared"},
		{Key: 4, Version: 2, Checksum: "bbb", VersionTag: "shared"},
	}
	for name, tc := range map[string]struct {
		observed []ProcessDefinitionAllocation
		want     ProcessDefinitionAllocation
	}{
		"same version":       {observed: sameVersion, want: sameVersion[1]},
		"different versions": {observed: differentVersions, want: differentVersions[1]},
	} {
		t.Run(name, func(t *testing.T) {
			reversed := []ProcessDefinitionAllocation{tc.observed[1], tc.observed[0]}
			for _, observed := range [][]ProcessDefinitionAllocation{tc.observed, reversed} {
				var c Cluster
				_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
					ProcessID: "order", Checksum: "ccc", Sequence: 100, Observed: observed, NowMillis: 10,
				})
				require.NoError(t, err)
				versions := c.ProcessDefinitions["order"]
				assert.Equal(t, tc.want, versions.VersionTags["shared"], "the tag is reserved for the newest definition holding it")
				assert.Equal(t, int32(3), versions.Latest.Version)

				restored := Cluster{Restore: RestoreOperation{ID: "op", Epoch: 1, Status: RestoreStatusActive, Phase: RestorePhaseReconciling}}
				definitions := make([]ObservedProcessDefinition, 0, len(observed))
				for _, definition := range observed {
					definitions = append(definitions, ObservedProcessDefinition{
						ProcessID: "order", Key: definition.Key, Version: definition.Version, Checksum: definition.Checksum, VersionTag: definition.VersionTag,
					})
				}
				require.NoError(t, restored.ResetProcessDefinitions(definitions, "op", 1))
				assert.Equal(t, ProcessDefinitionVersions{
					Latest:      tc.want,
					VersionTags: map[string]ProcessDefinitionAllocation{"shared": tc.want},
				}, restored.ProcessDefinitions["order"], "the reset resolves the tag the same way")
			}
		})
	}
}

func TestAllocateProcessDefinitionRejectsIncompleteObservations(t *testing.T) {
	for _, observed := range []ProcessDefinitionAllocation{
		{Key: 0, Version: 2, Checksum: "ccc"},
		{Key: 7, Version: 2, Checksum: ""},
		{Key: 7, Version: 0, Checksum: "ccc"},
	} {
		var c Cluster
		_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
			ProcessID: "order", Checksum: "ddd", Sequence: 100, Observed: []ProcessDefinitionAllocation{observed}, NowMillis: 10,
		})
		var rejected *ProcessDefinitionAllocationRejectedError
		require.ErrorAs(t, err, &rejected)
		assert.Empty(t, c.ProcessDefinitions, "nothing is recorded from an observation without identity")
	}
}

// TestResetProcessDefinitionsRebuildsTheRegistryFromThePartitions verifies
// that a reset (a cluster restore) replaces every recorded allocation with
// the definitions the partitions hold, and that allocations continue from
// them.
func TestResetProcessDefinitionsRebuildsTheRegistryFromThePartitions(t *testing.T) {
	var c Cluster
	for _, req := range []ProcessDefinitionAllocationRequest{
		{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 100, NowMillis: 10},
		{ProcessID: "order", Checksum: "bbb", Sequence: 200, NowMillis: 20},
		{ProcessID: "invoice", Checksum: "ccc", Sequence: 300, NowMillis: 30},
	} {
		_, _, err := c.AllocateProcessDefinition(req)
		require.NoError(t, err)
	}

	restored := []ObservedProcessDefinition{
		{ProcessID: "order", Key: 12, Version: 2, Checksum: "yyy"},
		{ProcessID: "payment", Key: 21, Version: 1, Checksum: "zzz"},
		{ProcessID: "order", Key: 11, Version: 1, Checksum: "xxx", VersionTag: "v1"},
	}
	// only the restore operation that owns the cluster, while it reconciles
	// the partitions, may reset the registry
	var restoreRejected *RestoreRejectedError
	require.ErrorAs(t, c.ResetProcessDefinitions(restored, "restore-1", 1), &restoreRejected, "no restore owns the cluster")
	c.Restore = RestoreOperation{ID: "restore-1", Epoch: 1, Status: RestoreStatusActive, Phase: RestorePhaseLoading}
	require.ErrorAs(t, c.ResetProcessDefinitions(restored, "restore-1", 1), &restoreRejected, "the restore is not reconciling yet")
	c.Restore.Phase = RestorePhaseReconciling
	require.ErrorAs(t, c.ResetProcessDefinitions(restored, "restore-1", 0), &restoreRejected, "a superseded coordinator")
	require.ErrorAs(t, c.ResetProcessDefinitions(restored, "restore-0", 1), &restoreRejected, "another operation")
	assert.Len(t, c.ProcessDefinitions["order"].VersionTags, 1, "a refused reset changes nothing")
	require.NoError(t, c.ResetProcessDefinitions(restored, "restore-1", 1))
	order := ProcessDefinitionAllocation{Key: 11, Version: 1, Checksum: "xxx", VersionTag: "v1"}
	assert.Equal(t, map[string]ProcessDefinitionVersions{
		"order":   {Latest: ProcessDefinitionAllocation{Key: 12, Version: 2, Checksum: "yyy"}, VersionTags: map[string]ProcessDefinitionAllocation{"v1": order}},
		"payment": {Latest: ProcessDefinitionAllocation{Key: 21, Version: 1, Checksum: "zzz"}},
	}, c.ProcessDefinitions, "a process the partitions do not hold is forgotten")

	// the order of the definitions does not matter to the result
	reversed := Cluster{Restore: c.Restore}
	require.NoError(t, reversed.ResetProcessDefinitions([]ObservedProcessDefinition{restored[2], restored[1], restored[0]}, "restore-1", 1))
	assert.Equal(t, c.ProcessDefinitions, reversed.ProcessDefinitions)

	// allocations continue from the restored definitions once the restore
	// is over; a deployment names the restore generation it observed
	c.Restore.Phase, c.Restore.Status = RestorePhaseDone, RestoreStatusCompleted
	next, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "www", VersionTag: "stable", Sequence: 400, RestoreID: "restore-1", RestoreEpoch: 1, NowMillis: 40})
	require.NoError(t, err, "the tag of an allocation the partitions do not hold is free again")
	assert.False(t, existing)
	assert.Equal(t, int32(3), next.Version)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "vvv", VersionTag: "v1", Sequence: 500, RestoreID: "restore-1", RestoreEpoch: 1, NowMillis: 50})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "a tag the partitions hold is taken")

	// a definition without identity is rejected and nothing changes
	c.Restore.Phase, c.Restore.Status = RestorePhaseReconciling, RestoreStatusActive
	before := *c.DeepCopy()
	err = c.ResetProcessDefinitions([]ObservedProcessDefinition{{ProcessID: "order", Key: 0, Version: 1, Checksum: "xxx"}}, "restore-1", 1)
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, before, c)

	require.NoError(t, c.ResetProcessDefinitions(nil, "restore-1", 1))
	assert.Empty(t, c.ProcessDefinitions, "partitions holding nothing empty the registry")
}

// TestAllocateProcessDefinitionIsFencedByRestores verifies that no allocation
// is made while a restore gates the cluster, and that an allocation whose
// observation of the partitions predates a restore is refused: the restored
// partitions no longer hold what it observed.
func TestAllocateProcessDefinitionIsFencedByRestores(t *testing.T) {
	var c Cluster
	var restoreRejected *RestoreRejectedError
	request := func(checksum string, restoreID string, epoch uint64) ProcessDefinitionAllocationRequest {
		return ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: checksum, Sequence: 100, RestoreID: restoreID, RestoreEpoch: epoch, NowMillis: 10}
	}
	_, _, err := c.AllocateProcessDefinition(request("aaa", "", 0))
	require.NoError(t, err, "a cluster that was never restored")

	for _, phase := range []RestorePhase{RestorePhasePending, RestorePhaseLoading, RestorePhaseReconciling} {
		c.Restore = RestoreOperation{ID: "restore-1", Epoch: 1, Status: RestoreStatusActive, Phase: phase, DataModified: true}
		_, _, err = c.AllocateProcessDefinition(request("bbb", "restore-1", 1))
		require.ErrorAs(t, err, &restoreRejected, "phase %s gates the cluster", phase)
	}
	c.Restore = RestoreOperation{ID: "restore-1", Epoch: 1, Status: RestoreStatusFailed, Phase: RestorePhaseLoading, DataModified: true}
	_, _, err = c.AllocateProcessDefinition(request("bbb", "restore-1", 1))
	require.ErrorAs(t, err, &restoreRejected, "a failed restore that modified the data gates the cluster")

	c.Restore = RestoreOperation{ID: "restore-1", Epoch: 1, Status: RestoreStatusCompleted, Phase: RestorePhaseDone}
	_, _, err = c.AllocateProcessDefinition(request("bbb", "", 0))
	require.ErrorAs(t, err, &restoreRejected, "observed before the restore")
	_, _, err = c.AllocateProcessDefinition(request("bbb", "restore-1", 0))
	require.ErrorAs(t, err, &restoreRejected, "observed under an earlier acquisition of the restore")
	assert.Equal(t, int32(1), c.ProcessDefinitions["order"].Latest.Version, "a fenced allocation changes nothing")

	next, _, err := c.AllocateProcessDefinition(request("bbb", "restore-1", 1))
	require.NoError(t, err, "observed after the restore")
	assert.Equal(t, int32(2), next.Version)
}

// TestAllocateProcessDefinitionRecoversFromAnAllocationNoPartitionHolds covers
// a registry whose latest version was allocated from a wrong observation:
// the partitions hold another definition at that version, so the recorded
// allocation can never be deployed. An observation showing that replaces
// it, while an observation that finds the recorded allocation on a partition
// keeps it.
func TestAllocateProcessDefinitionRecoversFromAnAllocationNoPartitionHolds(t *testing.T) {
	var c Cluster
	// allocated as version 2 from a stale observation; the partitions in
	// fact hold key 7 as version 2 ("ccc", tagged)
	wrong, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "bbb", VersionTag: "wrong", Sequence: 100, NowMillis: 10,
		Observed: []ProcessDefinitionAllocation{{Key: 5, Version: 1, Checksum: "aaa"}},
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), wrong.Version)
	held := ProcessDefinitionAllocation{Key: 7, Version: 2, Checksum: "ccc", VersionTag: "stable"}

	// a retry of the same content with the accurate observation: the wrong
	// allocation is not answered again, the content becomes version 3
	retried, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "bbb", VersionTag: "wrong", Sequence: 200, Observed: []ProcessDefinitionAllocation{held}, NowMillis: 20,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(3), retried.Version)
	assert.NotEqual(t, wrong.Key, retried.Key)
	assert.Equal(t, held, c.ProcessDefinitions["order"].VersionTags["stable"], "the tag the partitions hold is reserved")

	// the recorded latest is kept when a partition holds it, even next to a
	// partition holding another definition at the same version (a history
	// that diverged before allocations were replicated)
	var d Cluster
	first, _, err := d.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, NowMillis: 10})
	require.NoError(t, err)
	same, existing, err := d.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "aaa", Sequence: 200, NowMillis: 20,
		Observed: []ProcessDefinitionAllocation{{Key: 9, Version: 1, Checksum: "zzz"}, first},
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, first, same)
}

func TestProcessDefinitionVersionsRoundTripJSON(t *testing.T) {
	var current Cluster
	_, _, err := current.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: "v1", Sequence: 100, NowMillis: 10})
	require.NoError(t, err)
	encoded, err := json.Marshal(current)
	require.NoError(t, err)
	var decoded Cluster
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, current.ProcessDefinitions, decoded.ProcessDefinitions, "the allocations round-trip through a snapshot, tags included")
}

func TestAllocateProcessDefinitionIsDeterministic(t *testing.T) {
	requests := []ProcessDefinitionAllocationRequest{
		{ProcessID: "order", Checksum: "aaa", Sequence: 100, NowMillis: 1},
		{ProcessID: "order", Checksum: "bbb", Sequence: 200, VersionTag: "x", NowMillis: 2},
		{ProcessID: "order", Checksum: "bbb", Sequence: 201, NowMillis: 3},
		{ProcessID: "order", Checksum: "aaa", Sequence: 300, NowMillis: 4},
		{ProcessID: "order", Checksum: "ccc", Sequence: 400, VersionTag: "x", NowMillis: 5},
	}
	apply := func() Cluster {
		var c Cluster
		for _, req := range requests {
			_, _, _ = c.AllocateProcessDefinition(req)
		}
		return c
	}
	replicaA, replicaB := apply(), apply()
	assert.Equal(t, replicaA, replicaB)
	assert.Equal(t, int32(3), replicaA.ProcessDefinitions["order"].Latest.Version)
}

func TestClusterDeepCopyIsolatesProcessDefinitions(t *testing.T) {
	var c Cluster
	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, VersionTag: "t", NowMillis: 10})
	require.NoError(t, err)

	clone := c.DeepCopy()
	_, _, err = clone.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, VersionTag: "u", NowMillis: 20})
	require.NoError(t, err)

	assert.Equal(t, int32(1), c.ProcessDefinitions["order"].Latest.Version)
	assert.Equal(t, map[string]ProcessDefinitionAllocation{"t": c.ProcessDefinitions["order"].Latest}, c.ProcessDefinitions["order"].VersionTags)
	assert.Equal(t, int32(2), clone.ProcessDefinitions["order"].Latest.Version)
	assert.Len(t, clone.ProcessDefinitions["order"].VersionTags, 2)
}
