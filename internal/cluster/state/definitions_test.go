package state

import (
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
		ProcessID: "invoice", Checksum: "aaa", Sequence: 300,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), other.Version)
	assert.NotEqual(t, second.Key, other.Key)

	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest)
	assert.Equal(t, map[string]int32{"v2": 2}, c.ProcessDefinitions["order"].VersionTags)
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

	confirmed, wasIncomplete := c.ConfirmProcessDefinition("order", first.Key)
	assert.True(t, wasIncomplete)
	assert.Equal(t, first, confirmed)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200})
	require.NoError(t, err)

	// older content whose deployment completed is deduplicated against the
	// latest version only: it is deployed again as a new version
	redeployed, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 300})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(3), redeployed.Version)
	assert.NotEqual(t, first.Key, redeployed.Key)
}

func TestAllocateProcessDefinitionReturnsUnconfirmedAllocationAfterAnotherRevision(t *testing.T) {
	for _, tag := range []string{"", "stable"} {
		t.Run("tag="+tag, func(t *testing.T) {
			var c Cluster
			// revision A is allocated but its deployment does not reach every
			// partition, so it is never confirmed
			first, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: tag, Sequence: 100})
			require.NoError(t, err)
			second, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200})
			require.NoError(t, err)
			require.Equal(t, int32(2), second.Version)

			// the retry of A gets its allocation back instead of a new version
			// (or, with a tag, a rejection for reusing its own tag)
			retried, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: tag, Sequence: 300})
			require.NoError(t, err)
			assert.True(t, existing)
			assert.Equal(t, first, retried)
			assert.Equal(t, second, c.ProcessDefinitions["order"].Latest, "a retry never moves the latest version backwards")

			// once confirmed, the same content is a new deployment again
			_, wasIncomplete := c.ConfirmProcessDefinition("order", first.Key)
			assert.True(t, wasIncomplete)
			_, wasIncomplete = c.ConfirmProcessDefinition("order", first.Key)
			assert.False(t, wasIncomplete, "confirming twice changes nothing")
			_, wasIncomplete = c.ConfirmProcessDefinition("invoice", first.Key)
			assert.False(t, wasIncomplete, "an unknown process changes nothing")
			redeployed, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: tag, Sequence: 400})
			if tag != "" {
				var rejected *ProcessDefinitionAllocationRejectedError
				require.ErrorAs(t, err, &rejected, "the tag of the confirmed version stays taken")
				return
			}
			require.NoError(t, err)
			assert.False(t, existing)
			assert.Equal(t, int32(3), redeployed.Version)
		})
	}
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
	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, VersionTag: "stable"})
	require.NoError(t, err)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200})
	require.NoError(t, err)

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "ccc", Sequence: 300, VersionTag: "stable"})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "the tag stays unique across the whole history, not only the latest version")
	assert.Equal(t, "order", rejected.ProcessID)
	assert.Equal(t, int32(2), c.ProcessDefinitions["order"].Latest.Version, "nothing is allocated on a rejection")

	// the same tag on another process is fine
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "invoice", Checksum: "ccc", Sequence: 400, VersionTag: "stable"})
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

func TestAllocateProcessDefinitionCatchesUpWithObservedPartitionState(t *testing.T) {
	// definitions deployed before allocations were replicated exist on the
	// partitions only; the first allocation after the upgrade continues
	// their version sequence instead of restarting at 1
	var c Cluster
	observed := &ProcessDefinitionAllocation{Key: 7, Version: 3, Checksum: "ccc", VersionTag: "legacy"}

	next, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ddd", Sequence: 100, ObservedLatest: observed,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(4), next.Version)
	assert.Equal(t, map[string]int32{"legacy": 3}, c.ProcessDefinitions["order"].VersionTags, "the observed tag is protected too")

	// an observed definition with the same content as the request is
	// returned as the existing one without allocating
	var fresh Cluster
	same, existing, err := fresh.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ccc", Sequence: 100, ObservedLatest: observed,
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, *observed, same)
	assert.Equal(t, *observed, fresh.ProcessDefinitions["order"].Latest, "the observed definition is recorded as the latest")

	// the replicated state is authoritative once it is ahead: a partition
	// lagging behind (a fan-out still in flight, a restore from an older
	// backup) never moves the sequence backwards
	behind := &ProcessDefinitionAllocation{Key: 1, Version: 1, Checksum: "aaa"}
	later, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "eee", Sequence: 200, ObservedLatest: behind,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(5), later.Version)
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
		for i, req := range requests {
			allocation, _, _ := c.AllocateProcessDefinition(req)
			if i%2 == 0 {
				c.ConfirmProcessDefinition(req.ProcessID, allocation.Key)
			}
		}
		return c
	}
	replicaA, replicaB := apply(), apply()
	assert.Equal(t, replicaA, replicaB)
	assert.Equal(t, int32(3), replicaA.ProcessDefinitions["order"].Latest.Version)
}

func TestClusterDeepCopyIsolatesProcessDefinitions(t *testing.T) {
	var c Cluster
	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, VersionTag: "t"})
	require.NoError(t, err)

	clone := c.DeepCopy()
	_, _, err = clone.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, VersionTag: "u"})
	require.NoError(t, err)

	assert.Equal(t, int32(1), c.ProcessDefinitions["order"].Latest.Version)
	assert.Equal(t, map[string]int32{"t": 1}, c.ProcessDefinitions["order"].VersionTags)
	assert.Len(t, c.ProcessDefinitions["order"].Incomplete, 1)
	assert.Equal(t, int32(2), clone.ProcessDefinitions["order"].Latest.Version)
	assert.Len(t, clone.ProcessDefinitions["order"].Incomplete, 2)
}
