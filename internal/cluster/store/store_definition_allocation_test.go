package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteProcessDefinitionAllocationSerializesConcurrentDeployments verifies
// that concurrent deployments of one process id, each proposing its own key,
// are allocated distinct consecutive versions through the raft log, that an
// identical content gets the allocation that already exists, and that the
// allocation survives a snapshot round trip.
func TestWriteProcessDefinitionAllocationSerializesConcurrentDeployments(t *testing.T) {
	s := newBootstrappedTestStore(t)
	ctx := context.Background()

	const deployments = 8
	type outcome struct {
		allocation state.ProcessDefinitionAllocation
		existing   bool
	}
	outcomes := make([]outcome, deployments)
	var wg sync.WaitGroup
	for i := range deployments {
		wg.Go(func() {
			allocation, existing, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
				ProcessId:       new("order"),
				Checksum:        new(fmt.Sprintf("content-%d", i)),
				TimestampMillis: new(time.Now().UnixMilli()),
			})
			assert.NoError(t, err)
			outcomes[i] = outcome{allocation: allocation, existing: existing}
		})
	}
	wg.Wait()

	versions := map[int32]state.ProcessDefinitionAllocation{}
	keys := map[int64]struct{}{}
	for i, o := range outcomes {
		assert.False(t, o.existing, "deployment %d carried new content", i)
		_, taken := versions[o.allocation.Version]
		assert.False(t, taken, "version %d allocated twice", o.allocation.Version)
		versions[o.allocation.Version] = o.allocation
		_, taken = keys[o.allocation.Key]
		assert.False(t, taken, "key %d allocated twice", o.allocation.Key)
		keys[o.allocation.Key] = struct{}{}
		assert.Equal(t, uint32(zenflake.GlobalResourceNode), zenflake.GetPartitionId(o.allocation.Key))
	}
	for v := int32(1); v <= deployments; v++ {
		assert.Contains(t, versions, v)
	}

	latest := s.ClusterState().ProcessDefinitions["order"].Latest
	assert.Equal(t, int32(deployments), latest.Version)

	// retrying the latest deployment (or deploying it concurrently twice)
	// reuses the allocation instead of taking a new version
	again, existing, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"),
		Checksum:  new(latest.Checksum),
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, latest, again)

	// an allocation whose deployment was never confirmed is returned to a
	// retry even after another revision was allocated; a confirmed one is not
	unconfirmed, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("unconfirmed"),
	})
	require.NoError(t, err)
	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("after-unconfirmed"),
	})
	require.NoError(t, err)
	retried, existing, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("unconfirmed"),
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, unconfirmed, retried)
	confirmed, wasIncomplete, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		Action: proto.ProcessDefinitionAllocation_ACTION_CONFIRM.Enum(), ProcessId: new("order"), Key: new(unconfirmed.Key),
	})
	require.NoError(t, err)
	assert.True(t, wasIncomplete)
	assert.Equal(t, unconfirmed, confirmed)
	redeployed, existing, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("unconfirmed"),
	})
	require.NoError(t, err)
	assert.False(t, existing)
	assert.Greater(t, redeployed.Version, unconfirmed.Version)
	latest = s.ClusterState().ProcessDefinitions["order"].Latest

	// the allocation is part of the replicated state: a snapshot carries it
	fsm := NewFSM(s)
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	snapFile, err := os.Create(filepath.Join(t.TempDir(), "snapshot"))
	require.NoError(t, err)
	require.NoError(t, snapshot.Persist(&mockSnapshotSink{snapFile}))
	s.stateMu.Lock()
	s.state = state.Cluster{}
	s.stateMu.Unlock()
	restoreFrom, err := os.Open(snapFile.Name())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreFrom.Close()) }()
	require.NoError(t, fsm.Restore(restoreFrom))
	assert.Equal(t, latest, s.ClusterState().ProcessDefinitions["order"].Latest)
}

func TestWriteProcessDefinitionAllocationReportsRejectionWithoutChangingState(t *testing.T) {
	s := newBootstrappedTestStore(t)
	ctx := context.Background()

	_, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("a"), VersionTag: new("stable"),
	})
	require.NoError(t, err)

	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("b"), VersionTag: new("stable"),
	})
	var rejected *state.ProcessDefinitionAllocationRejectedError
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, int32(1), s.ClusterState().ProcessDefinitions["order"].Latest.Version)

	// an action this binary does not know is refused instead of being
	// applied as an allocation
	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		Action: proto.ProcessDefinitionAllocation_Action(99).Enum(), ProcessId: new("order"), Checksum: new("c"),
	})
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, int32(1), s.ClusterState().ProcessDefinitions["order"].Latest.Version)
}

func TestWriteProcessDefinitionAllocationRequiresLeader(t *testing.T) {
	cfg := config.Cluster{NodeId: "non-leader", Raft: config.ClusterRaft{Dir: t.TempDir()}}
	s, listener := newMustTestStore(t, cfg)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close(true)) })

	_, _, err := s.WriteProcessDefinitionAllocation(context.Background(), &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("a"),
	})
	require.ErrorIs(t, err, zenerr.ErrNotLeader, "callers route the allocation to the cluster leader by this sentinel")
}

// newBootstrappedTestStore opens a single-node store that is its own leader.
func newBootstrappedTestStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Cluster{NodeId: "leader", Raft: config.ClusterRaft{Dir: t.TempDir()}}
	s, listener := newMustTestStore(t, cfg)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close(true)) })
	require.NoError(t, s.Bootstrap(&state.Node{Id: s.raftID, Addr: s.Addr(), Partitions: map[uint32]state.NodePartition{}}))
	_, err := s.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	return s
}
