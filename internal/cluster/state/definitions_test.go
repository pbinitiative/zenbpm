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

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200})
	require.NoError(t, err)

	// older content is deduplicated against the latest version only: deployed
	// after another revision it becomes a new version, the latest one again
	redeployed, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 300})
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
	first, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 100})
	require.NoError(t, err)
	second, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200})
	require.NoError(t, err)
	require.Equal(t, int32(2), second.Version)

	retried, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 300})
	require.NoError(t, err, "the retry must not be rejected for its own version tag")
	assert.True(t, existing)
	assert.Equal(t, first, retried)
	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest, "a retry never moves the latest version backwards")

	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "ccc", VersionTag: "stable", Sequence: 400})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "the tag is taken by other content")
	assert.Equal(t, second, c.ProcessDefinitions["order"].Latest, "nothing is allocated on a rejection")

	// the same content without its tag is a new deployment
	untagged, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 500})
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
	// their version sequence instead of restarting at 1 and reserves every
	// tag the partitions hold
	var c Cluster
	observed := []ProcessDefinitionAllocation{
		{Key: 7, Version: 3, Checksum: "ccc", VersionTag: "legacy"},
		{Key: 5, Version: 1, Checksum: "aaa", VersionTag: "first"},
	}

	next, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ddd", Sequence: 100, Observed: observed,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(4), next.Version)
	assert.Equal(t, map[string]ProcessDefinitionAllocation{"legacy": observed[0], "first": observed[1]}, c.ProcessDefinitions["order"].VersionTags, "the observed tags are protected too")
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "eee", VersionTag: "first", Sequence: 150,
	})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "a tag an older version holds on the partitions is taken")

	// an observed definition with the same content as the request is
	// returned as the existing one without allocating
	var fresh Cluster
	same, existing, err := fresh.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ccc", Sequence: 100, Observed: observed,
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, observed[0], same)
	assert.Equal(t, observed[0], fresh.ProcessDefinitions["order"].Latest, "the observed definition is recorded as the latest")

	// the order of the observations does not matter to the result
	var shuffled Cluster
	_, _, err = shuffled.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "ccc", Sequence: 100, Observed: []ProcessDefinitionAllocation{observed[1], observed[0]},
	})
	require.NoError(t, err)
	assert.Equal(t, fresh, shuffled)

	// the replicated state is authoritative once it is ahead: a partition
	// lagging behind (a fan-out still in flight) never moves the sequence
	// backwards, but a tag it holds on an older version replaces the tag
	// recorded for an allocation that may never have reached it
	behind := []ProcessDefinitionAllocation{{Key: 1, Version: 1, Checksum: "aaa", VersionTag: "legacy"}}
	later, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
		ProcessID: "order", Checksum: "fff", Sequence: 200, Observed: behind,
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Equal(t, int32(5), later.Version)
	assert.Equal(t, behind[0], c.ProcessDefinitions["order"].VersionTags["legacy"], "the partitions are authoritative for the tags they hold")
}

func TestAllocateProcessDefinitionRejectsIncompleteObservations(t *testing.T) {
	for _, observed := range []ProcessDefinitionAllocation{
		{Key: 0, Version: 2, Checksum: "ccc"},
		{Key: 7, Version: 2, Checksum: ""},
		{Key: 7, Version: 0, Checksum: "ccc"},
	} {
		var c Cluster
		_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{
			ProcessID: "order", Checksum: "ddd", Sequence: 100, Observed: []ProcessDefinitionAllocation{observed},
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
		{ProcessID: "order", Checksum: "aaa", VersionTag: "stable", Sequence: 100},
		{ProcessID: "order", Checksum: "bbb", Sequence: 200},
		{ProcessID: "invoice", Checksum: "ccc", Sequence: 300},
	} {
		_, _, err := c.AllocateProcessDefinition(req)
		require.NoError(t, err)
	}

	restored := []ObservedProcessDefinition{
		{ProcessID: "order", Key: 12, Version: 2, Checksum: "yyy"},
		{ProcessID: "payment", Key: 21, Version: 1, Checksum: "zzz"},
		{ProcessID: "order", Key: 11, Version: 1, Checksum: "xxx", VersionTag: "v1"},
	}
	require.NoError(t, c.ResetProcessDefinitions(restored))
	order := ProcessDefinitionAllocation{Key: 11, Version: 1, Checksum: "xxx", VersionTag: "v1"}
	assert.Equal(t, map[string]ProcessDefinitionVersions{
		"order":   {Latest: ProcessDefinitionAllocation{Key: 12, Version: 2, Checksum: "yyy"}, VersionTags: map[string]ProcessDefinitionAllocation{"v1": order}},
		"payment": {Latest: ProcessDefinitionAllocation{Key: 21, Version: 1, Checksum: "zzz"}},
	}, c.ProcessDefinitions, "a process the partitions do not hold is forgotten")

	// the order of the definitions does not matter to the result
	var reversed Cluster
	require.NoError(t, reversed.ResetProcessDefinitions([]ObservedProcessDefinition{restored[2], restored[1], restored[0]}))
	assert.Equal(t, c.ProcessDefinitions, reversed.ProcessDefinitions)

	// allocations continue from the restored definitions
	next, existing, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "www", VersionTag: "stable", Sequence: 400})
	require.NoError(t, err, "the tag of an allocation the partitions do not hold is free again")
	assert.False(t, existing)
	assert.Equal(t, int32(3), next.Version)
	_, _, err = c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "vvv", VersionTag: "v1", Sequence: 500})
	var rejected *ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected, "a tag the partitions hold is taken")

	// a definition without identity is rejected and nothing changes
	before := *c.DeepCopy()
	err = c.ResetProcessDefinitions([]ObservedProcessDefinition{{ProcessID: "order", Key: 0, Version: 1, Checksum: "xxx"}})
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, before, c)

	require.NoError(t, c.ResetProcessDefinitions(nil))
	assert.Empty(t, c.ProcessDefinitions, "partitions holding nothing empty the registry")
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
	_, _, err := c.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "aaa", Sequence: 100, VersionTag: "t"})
	require.NoError(t, err)

	clone := c.DeepCopy()
	_, _, err = clone.AllocateProcessDefinition(ProcessDefinitionAllocationRequest{ProcessID: "order", Checksum: "bbb", Sequence: 200, VersionTag: "u"})
	require.NoError(t, err)

	assert.Equal(t, int32(1), c.ProcessDefinitions["order"].Latest.Version)
	assert.Equal(t, map[string]ProcessDefinitionAllocation{"t": c.ProcessDefinitions["order"].Latest}, c.ProcessDefinitions["order"].VersionTags)
	assert.Equal(t, int32(2), clone.ProcessDefinitions["order"].Latest.Version)
	assert.Len(t, clone.ProcessDefinitions["order"].VersionTags, 2)
}
