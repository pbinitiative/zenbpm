package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeSuspensionCluster is a cluster of partitions, each with its own storage, which a processSuspender changes
// the way the partition leaders do.
type fakeSuspensionCluster struct {
	stores map[uint32]*inmemory.Storage
	// existing is the processes which are deployed.
	existing map[string]bool
	// failing is the partitions which fail the change.
	failing map[uint32]error
	// afterAssign runs when a partition has taken the next version of a change itself.
	afterAssign func(partitionId uint32, version int64)
	// beforeApply runs before a partition is told of a change another partition took.
	beforeApply func(partitionId uint32, version int64)
	// firstAnswer replaces the version the first partition answers.
	firstAnswer *int64

	mu    sync.Mutex
	calls map[uint32]int
	// deadlines is how long the contexts the other partitions were told of a change with had left.
	deadlines []time.Duration
}

func newFakeSuspensionCluster(partitions int) *fakeSuspensionCluster {
	fc := &fakeSuspensionCluster{
		stores:   map[uint32]*inmemory.Storage{},
		existing: map[string]bool{"process": true},
		failing:  map[uint32]error{},
		calls:    map[uint32]int{},
	}
	for p := 1; p <= partitions; p++ {
		fc.stores[uint32(p)] = inmemory.NewStorage() // #nosec G115 -- test partition ids are tiny
	}
	return fc
}

func (fc *fakeSuspensionCluster) suspender() processSuspender {
	partitionIds := make([]uint32, 0, len(fc.stores))
	for p := 1; p <= len(fc.stores); p++ {
		partitionIds = append(partitionIds, uint32(p)) // #nosec G115 -- test partition ids are tiny
	}
	return processSuspender{
		partitionIds: partitionIds,
		exists: func(_ context.Context, _ uint32, bpmnProcessId string) (bool, error) {
			return fc.existing[bpmnProcessId], nil
		},
		set: func(ctx context.Context, partitionId uint32, bpmnProcessId string, call suspensionCall) (suspensionChange, error) {
			fc.mu.Lock()
			fc.calls[partitionId]++
			fc.mu.Unlock()
			// a call to a partition leader which is made with a context which is done fails, like the real one
			if err := ctx.Err(); err != nil {
				return suspensionChange{}, err
			}
			if deadline, ok := ctx.Deadline(); ok && call.version != 0 {
				fc.mu.Lock()
				fc.deadlines = append(fc.deadlines, time.Until(deadline))
				fc.mu.Unlock()
			}
			if err := fc.failing[partitionId]; err != nil {
				return suspensionChange{}, err
			}
			store := fc.stores[partitionId]
			if call.version == 0 {
				taken, err := store.ChangeProcessSuspension(ctx, bpmnProcessId, call.suspended, storage.AboveVersion(call.minVersion))
				if err != nil {
					return suspensionChange{}, err
				}
				if fc.afterAssign != nil {
					fc.afterAssign(partitionId, taken)
				}
				if fc.firstAnswer != nil {
					taken = *fc.firstAnswer
				}
				return suspensionChange{version: taken, held: storage.ProcessSuspension{Suspended: call.suspended, Version: taken}}, nil
			}
			if fc.beforeApply != nil && partitionId != 1 {
				fc.beforeApply(partitionId, call.version)
			}
			held, err := store.ApplyProcessSuspension(ctx, bpmnProcessId, call.suspended, call.version)
			return suspensionChange{version: call.version, held: held}, err
		},
	}
}

func (fc *fakeSuspensionCluster) held(t *testing.T, partitionId uint32) storage.ProcessSuspension {
	t.Helper()
	store := fc.stores[partitionId]
	suspended, err := store.IsProcessSuspended(t.Context(), "process")
	require.NoError(t, err)
	return storage.ProcessSuspension{Suspended: suspended, Version: store.ProcessSuspensions["process"].Version}
}

func TestSuspendingAProcessReachesEveryPartition(t *testing.T) {
	fc := newFakeSuspensionCluster(3)

	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", true))
	for partitionId := range fc.stores {
		assert.True(t, fc.held(t, partitionId).Suspended, "partition %d", partitionId)
	}

	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", false))
	for partitionId := range fc.stores {
		assert.False(t, fc.held(t, partitionId).Suspended, "partition %d", partitionId)
	}
}

func TestSuspendingAnUnknownProcessIsNotFoundAndChangesNothing(t *testing.T) {
	fc := newFakeSuspensionCluster(2)

	err := fc.suspender().Suspend(t.Context(), "unknown", true)

	zerr, ok := errors.AsType[*zenerr.ZenError](err)
	require.True(t, ok)
	assert.Equal(t, zenerr.NotFoundCode, zerr.Code)
	assert.Empty(t, fc.calls, "no partition was asked to change anything")
}

// TestSuspensionAChangeWhichReachesAPartitionAfterALaterOneDoesNotUndoIt makes the change which took the lower version
// on the first partition reach the other partitions after the change which took the higher one.
func TestSuspensionAChangeWhichReachesAPartitionAfterALaterOneDoesNotUndoIt(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	firstTookItsVersion := make(chan struct{})
	releaseFirst := make(chan struct{})
	fc.afterAssign = func(_ uint32, version int64) {
		if version == 1 {
			close(firstTookItsVersion)
		}
	}
	fc.beforeApply = func(_ uint32, version int64) {
		if version == 1 {
			select {
			case <-releaseFirst:
			case <-time.After(5 * time.Second):
			}
		}
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		assert.NoError(t, fc.suspender().Suspend(t.Context(), "process", true))
	})
	select {
	case <-firstTookItsVersion:
	case <-time.After(5 * time.Second):
		t.Fatal("the first change never took its version")
	}
	// the change which takes version 2 reaches every partition while the one with version 1 is held back
	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", false))
	close(releaseFirst)
	wg.Wait()

	for partitionId := range fc.stores {
		assert.Equal(t, storage.ProcessSuspension{Suspended: false, Version: 2}, fc.held(t, partitionId), "partition %d", partitionId)
	}
}

func TestSuspensionAChangeIsMadeAgainAboveAPartitionLeftAheadByARestore(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	// the partitions were restored from backups taken at different moments
	for partitionId, held := range map[uint32]storage.ProcessSuspension{
		1: {Suspended: true, Version: 3},
		2: {Suspended: true, Version: 5},
		3: {Suspended: true, Version: 3},
	} {
		_, err := fc.stores[partitionId].ApplyProcessSuspension(t.Context(), "process", held.Suspended, held.Version)
		require.NoError(t, err)
	}

	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", false))

	for partitionId := range fc.stores {
		assert.Equal(t, storage.ProcessSuspension{Suspended: false, Version: 6}, fc.held(t, partitionId), "partition %d is resumed, and it is not the older change of the first partition which won", partitionId)
	}
}

func TestSuspensionPartitionsWhichAgreeAreLeftAlone(t *testing.T) {
	fc := newFakeSuspensionCluster(3)

	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", true))

	// the change, the other partitions, and nothing else: no second round and no look at the first partition
	assert.Equal(t, map[uint32]int{1: 1, 2: 1, 3: 1}, fc.calls)
}

func TestSuspensionFailureOfAPartitionNamesItAndTheOthersStillGetTheChange(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	down := errors.New("no leader")
	fc.failing[2] = down

	err := fc.suspender().Suspend(t.Context(), "process", true)

	require.ErrorIs(t, err, down)
	assert.ErrorContains(t, err, "partition 2")
	assert.ErrorContains(t, err, "send the request again")
	zerr, ok := errors.AsType[*zenerr.ZenError](err)
	require.True(t, ok)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code, "whichever partitions failed, the caller is told of one condition")
	assert.True(t, fc.held(t, 1).Suspended)
	assert.True(t, fc.held(t, 3).Suspended, "a failing partition does not keep the others from getting the change")
	assert.False(t, fc.held(t, 2).Suspended)

	// sending the request again brings the partition up to date
	delete(fc.failing, 2)
	require.NoError(t, fc.suspender().Suspend(t.Context(), "process", true))
	assert.True(t, fc.held(t, 2).Suspended)
}

func TestSuspensionFailureOfTheFirstPartitionChangesNothingAnywhere(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	fc.failing[1] = fmt.Errorf("no leader")

	err := fc.suspender().Suspend(t.Context(), "process", true)

	require.Error(t, err)
	assert.Equal(t, map[uint32]int{1: 1}, fc.calls, "no other partition is told of a change the first one did not take")
	for partitionId := range fc.stores {
		assert.False(t, fc.held(t, partitionId).Suspended, "partition %d", partitionId)
	}
}

func TestSuspensionAFirstPartitionWhichAnswersNoVersionIsAnError(t *testing.T) {
	// a binary which does not know the version answers zero, and zero tells the other partitions to take a
	// version of their own
	for _, answer := range []int64{0, -1} {
		t.Run(fmt.Sprintf("version %d", answer), func(t *testing.T) {
			fc := newFakeSuspensionCluster(3)
			fc.firstAnswer = &answer

			err := fc.suspender().Suspend(t.Context(), "process", true)

			require.ErrorContains(t, err, fmt.Sprintf("answered version %d", answer))
			assert.Equal(t, map[uint32]int{1: 1}, fc.calls, "no other partition is told of the change")
		})
	}
}

func TestSuspensionAChangeIsMadeAboveAPartitionHoldingAVersionTheFirstPartitionNeverTook(t *testing.T) {
	tests := []struct {
		name  string
		setup map[uint32]storage.ProcessSuspension
		// version every partition ends with
		version int64
	}{
		{
			name:    "the first partition never heard of the process",
			setup:   map[uint32]storage.ProcessSuspension{2: {Suspended: true, Version: 5}},
			version: 6,
		},
		{
			name: "another partition holds the same version in another state",
			setup: map[uint32]storage.ProcessSuspension{
				1: {Suspended: true, Version: 4},
				2: {Suspended: true, Version: 5},
			},
			version: 6,
		},
		{
			name: "a partition holds a version a round above the first one",
			setup: map[uint32]storage.ProcessSuspension{
				1: {Suspended: false, Version: 1},
				3: {Suspended: true, Version: 2},
			},
			version: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newFakeSuspensionCluster(3)
			for partitionId, held := range tt.setup {
				_, err := fc.stores[partitionId].ApplyProcessSuspension(t.Context(), "process", held.Suspended, held.Version)
				require.NoError(t, err)
			}

			require.NoError(t, fc.suspender().Suspend(t.Context(), "process", false))

			for partitionId := range fc.stores {
				assert.Equal(t, storage.ProcessSuspension{Suspended: false, Version: tt.version}, fc.held(t, partitionId), "partition %d", partitionId)
			}
		})
	}
}

func TestSuspensionPartitionsWhichKeepDisagreeingAreAnError(t *testing.T) {
	fc := newFakeSuspensionCluster(2)
	// every time the first partition takes a version another change has already raised the second one above it
	fc.afterAssign = func(_ uint32, version int64) {
		_, err := fc.stores[2].ApplyProcessSuspension(t.Context(), "process", true, version+10)
		require.NoError(t, err)
	}

	err := fc.suspender().Suspend(t.Context(), "process", false)

	require.ErrorContains(t, err, "keep disagreeing")
	zerr, ok := errors.AsType[*zenerr.ZenError](err)
	require.True(t, ok)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code)
}

func TestSuspensionOnEveryPartitionReachesEveryPartitionOnce(t *testing.T) {
	var mu sync.Mutex
	reached := map[uint32]int{}

	err := onEveryPartition(t.Context(), []uint32{1, 2, 3}, func(_ context.Context, partitionId uint32) error {
		mu.Lock()
		defer mu.Unlock()
		reached[partitionId]++
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, map[uint32]int{1: 1, 2: 1, 3: 1}, reached)
}

func TestSuspensionOnEveryPartitionWaitsForAllAndReturnsEveryFailure(t *testing.T) {
	two, three := errors.New("partition 2 is down"), errors.New("partition 3 is down")
	var mu sync.Mutex
	reached := map[uint32]int{}

	err := onEveryPartition(t.Context(), []uint32{1, 2, 3}, func(_ context.Context, partitionId uint32) error {
		mu.Lock()
		reached[partitionId]++
		mu.Unlock()
		switch partitionId {
		case 2:
			return two
		case 3:
			return three
		}
		return nil
	})

	require.ErrorIs(t, err, two)
	require.ErrorIs(t, err, three)
	assert.Equal(t, map[uint32]int{1: 1, 2: 1, 3: 1}, reached)
}

func TestSuspensionCallErrorTellsALeaderWhichDoesNotKnowTheCallFromOtherFailures(t *testing.T) {
	outdated := suspensionCallError(t.Context(), 2, "process", status.Error(codes.Unimplemented, "unknown method SetProcessSuspension"))
	zerr, ok := errors.AsType[*zenerr.ZenError](outdated)
	require.True(t, ok)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code, "a transient answer for the caller, not a failure of the engine")
	require.ErrorIs(t, outdated, errPartitionLeaderOutdated)
	assert.ErrorContains(t, outdated, "partition 2")
	assert.ErrorContains(t, outdated, "finish upgrading the cluster")

	// any other failure of the call to the leader is a condition of the cluster, a 502 for the caller ...
	other := suspensionCallError(t.Context(), 2, "process", status.Error(codes.DeadlineExceeded, "leader went away"))
	zerr, ok = errors.AsType[*zenerr.ZenError](other)
	require.True(t, ok)
	assert.Equal(t, zenerr.ClusterErrorCode, zerr.Code)
	assert.NotErrorIs(t, other, errPartitionLeaderOutdated)

	// ... unless the caller itself is gone
	gone, cancel := context.WithCancel(t.Context())
	cancel()
	zerr, ok = errors.AsType[*zenerr.ZenError](suspensionCallError(gone, 2, "process", status.Error(codes.Canceled, "caller left")))
	require.True(t, ok)
	assert.Equal(t, zenerr.TechnicalErrorCode, zerr.Code)
}

func TestSuspensionAnOutdatedPartitionLeaderIsNotToldToSendTheRequestAgain(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	fc.failing[2] = suspensionCallError(t.Context(), 2, "process", status.Error(codes.Unimplemented, "unknown method"))

	err := fc.suspender().Suspend(t.Context(), "process", true)

	require.ErrorIs(t, err, errPartitionLeaderOutdated)
	assert.ErrorContains(t, err, "need the upgrade to be finished")
}

func TestSuspensionTheOtherPartitionsAreToldOfAChangeEvenWhenTheCallerGoesAwayAfterTheFirstTookIt(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	ctx, cancel := context.WithCancel(t.Context())
	fc.afterAssign = func(uint32, int64) {
		cancel() // the client hangs up as soon as the first partition took the change
	}

	err := fc.suspender().Suspend(ctx, "process", true)

	require.NoError(t, err)
	for partitionId := range fc.stores {
		assert.Equal(t, storage.ProcessSuspension{Suspended: true, Version: 1}, fc.held(t, partitionId), "partition %d", partitionId)
	}
	require.Len(t, fc.deadlines, 2, "the other partitions were told under a context with a deadline")
	for _, left := range fc.deadlines {
		assert.Positive(t, left)
		assert.LessOrEqual(t, left, suspensionFanOutTimeout, "the time the others have is bounded")
	}
}

func TestSuspensionARepairAfterARestoreIsCarriedThroughWhenTheCallerGoesAway(t *testing.T) {
	fc := newFakeSuspensionCluster(3)
	for partitionId, held := range map[uint32]storage.ProcessSuspension{
		1: {Suspended: true, Version: 3},
		2: {Suspended: true, Version: 5},
		3: {Suspended: true, Version: 3},
	} {
		_, err := fc.stores[partitionId].ApplyProcessSuspension(t.Context(), "process", held.Suspended, held.Version)
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	fc.afterAssign = func(_ uint32, version int64) {
		if version == 4 {
			cancel() // the client hangs up after the first round, before the change is made again above version 5
		}
	}

	require.NoError(t, fc.suspender().Suspend(ctx, "process", false))

	for partitionId := range fc.stores {
		assert.Equal(t, storage.ProcessSuspension{Suspended: false, Version: 6}, fc.held(t, partitionId), "partition %d", partitionId)
	}
}
