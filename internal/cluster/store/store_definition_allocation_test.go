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
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
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
		ProcessId:       new("order"),
		Checksum:        new(latest.Checksum),
		TimestampMillis: new(time.Now().UnixMilli()),
	})
	require.NoError(t, err)
	assert.True(t, existing)
	assert.Equal(t, latest, again)

	// a reset (a cluster restore reconciling the partitions it replaced)
	// rebuilds the registry from the definitions the partitions hold; only
	// the restore operation owning the cluster may apply it
	reset := func(restoreID string, epoch uint64) error {
		_, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
			Action: proto.ProcessDefinitionAllocation_ACTION_RESET.Enum(),
			Definitions: []*proto.ObservedProcessDefinition{
				{ProcessId: new("order"), Key: new(int64(77)), Version: new(int32(3)), Checksum: new("restored"), VersionTag: new("stable")},
			},
			RestoreOperationId: new(restoreID), RestoreEpoch: new(epoch),
		})
		return err
	}
	var restoreRejected *state.RestoreRejectedError
	require.ErrorAs(t, reset("restore-1", 1), &restoreRejected, "no restore owns the cluster")
	s.stateMu.Lock()
	s.state.Restore = state.RestoreOperation{ID: "restore-1", Epoch: 2, Status: state.RestoreStatusActive, Phase: state.RestorePhaseReconciling}
	s.stateMu.Unlock()
	require.ErrorAs(t, reset("restore-1", 1), &restoreRejected, "a superseded coordinator")
	assert.Equal(t, latest, s.ClusterState().ProcessDefinitions["order"].Latest, "a refused reset changes nothing")
	require.NoError(t, reset("restore-1", 2))
	latest = s.ClusterState().ProcessDefinitions["order"].Latest
	assert.Equal(t, state.ProcessDefinitionAllocation{Key: 77, Version: 3, Checksum: "restored", VersionTag: "stable"}, latest)
	// while the restore gates the cluster nothing is allocated; afterwards a
	// deployment names the restore generation it observed the partitions under
	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{ProcessId: new("order"), Checksum: new("gated"), RestoreOperationId: new("restore-1"), RestoreEpoch: new(uint64(2))})
	require.ErrorAs(t, err, &restoreRejected)
	s.stateMu.Lock()
	s.state.Restore.Status, s.state.Restore.Phase = state.RestoreStatusCompleted, state.RestorePhaseDone
	s.stateMu.Unlock()
	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{ProcessId: new("order"), Checksum: new("stale")})
	require.ErrorAs(t, err, &restoreRejected, "observed before the restore")
	afterRestore, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{ProcessId: new("order"), Checksum: new("fresh"), RestoreOperationId: new("restore-1"), RestoreEpoch: new(uint64(2)), TimestampMillis: new(time.Now().UnixMilli())})
	require.NoError(t, err)
	assert.Equal(t, int32(4), afterRestore.Version)
	latest = s.ClusterState().ProcessDefinitions["order"].Latest

	// the allocation is part of the replicated state: a snapshot carries it
	fsm := NewFSM(s)
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	snapFile, err := os.Create(filepath.Join(t.TempDir(), "snapshot"))
	require.NoError(t, err)
	require.NoError(t, snapshot.Persist(&mockSnapshotSink{snapFile}))
	restoredStore := &Store{logger: s.logger}
	restoredFSM := NewFSM(restoredStore)
	restoreFrom, err := os.Open(snapFile.Name())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreFrom.Close()) }()
	require.NoError(t, restoredFSM.Restore(restoreFrom))
	assert.Equal(t, latest, restoredStore.ClusterState().ProcessDefinitions["order"].Latest)
}

// TestWriteProcessDefinitionAllocationRequiresMembersProtocolVersion verifies
// that the leader refuses to commit the allocation command while a member of
// the raft configuration has not announced a protocol version that includes
// it, so that a member running an older binary is never sent a command it
// would stop on, and commits it once every member announced.
func TestWriteProcessDefinitionAllocationRequiresMembersProtocolVersion(t *testing.T) {
	s := newBootstrappedTestStore(t)
	ctx := context.Background()
	allocate := func() error {
		_, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
			ProcessId: new("order"), Checksum: new("aaa"), TimestampMillis: new(time.Now().UnixMilli()),
		})
		return err
	}
	// a non-voter joins the configuration (it applies the log like a voter;
	// unreachable, so that it never announces on its own) and has announced
	// nothing yet
	require.NoError(t, s.raft.AddNonvoter("peer-1", "127.0.0.1:1", 0, 5*time.Second).Error())
	err := allocate()
	var member *state.MemberProtocolVersionError
	require.ErrorAs(t, err, &member)
	assert.Equal(t, "peer-1", member.Member)
	assert.True(t, member.Unannounced())
	assert.Equal(t, state.ProtocolVersionProcessDefinitionAllocation, member.Required)
	assert.Empty(t, s.ClusterState().ProcessDefinitions, "nothing was committed")

	// the peer announces its version: the command is committed again
	require.NoError(t, s.WriteNodeChange(&proto.NodeChange{NodeId: new("peer-1"), ProtocolVersion: new(state.CurrentProtocolVersion)}))
	require.NoError(t, allocate())

	// a member that announced an older version than a future command needs
	err = s.requireMemberProtocolVersion(state.CurrentProtocolVersion + 1)
	require.ErrorAs(t, err, &member)
	assert.False(t, member.Unannounced())
	assert.Equal(t, state.CurrentProtocolVersion, member.Reported)

	// After activation, losing a member's announcement cannot remove commands
	// already in the log. A shutdown must not disable deployment availability.
	require.NoError(t, s.WriteNodeChange(&proto.NodeChange{NodeId: new("peer-1"), State: proto.NodeState_NODE_STATE_SHUTDOWN.Enum()}))
	require.Zero(t, s.ClusterState().Nodes["peer-1"].ProtocolVersion)
	require.NoError(t, allocate(), "the required protocol is already in the log")

	// a member that left is not waited for
	require.NoError(t, s.raft.RemoveServer("peer-1", 0, 5*time.Second).Error())
	require.NoError(t, allocate())
}

// TestJoinRequiresTheProtocolVersionTheLogHolds verifies that once the log
// holds a command, a binary that does not know it can no longer join: it
// would stop when the log delivers the command. A joining binary announces
// its protocol version in the join request; binaries that predate the
// announcement send none.
func TestJoinRequiresTheProtocolVersionTheLogHolds(t *testing.T) {
	s := newBootstrappedTestStore(t)
	ctx := context.Background()
	assert.Zero(t, s.ClusterState().MinProtocolVersion, "a log without the command requires nothing")

	_, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("aaa"), TimestampMillis: new(time.Now().UnixMilli()),
	})
	require.NoError(t, err)
	assert.Equal(t, state.ProtocolVersionProcessDefinitionAllocation, s.ClusterState().MinProtocolVersion, "the command raised the requirement")

	err = s.Join(&zproto.JoinRequest{Id: new("old-peer"), Address: new("127.0.0.1:1"), Voter: new(false)})
	var member *state.MemberProtocolVersionError
	require.ErrorAs(t, err, &member)
	assert.Equal(t, "old-peer", member.Member)
	assert.True(t, member.Unannounced())
	err = s.Join(&zproto.JoinRequest{Id: new("new-peer"), Address: new("127.0.0.1:2"), Voter: new(false), ProtocolVersion: new(state.CurrentProtocolVersion)})
	require.NoError(t, err)
	future := s.raft.GetConfiguration()
	require.NoError(t, future.Error())
	ids := []string{}
	for _, server := range future.Configuration().Servers {
		ids = append(ids, string(server.ID))
	}
	assert.ElementsMatch(t, []string{s.raftID, "new-peer"}, ids)

	// the requirement is part of the replicated state: a snapshot carries it
	fsm := NewFSM(s)
	snapshot, err2 := fsm.Snapshot()
	require.NoError(t, err2)
	snapFile, err2 := os.Create(filepath.Join(t.TempDir(), "snapshot"))
	require.NoError(t, err2)
	require.NoError(t, snapshot.Persist(&mockSnapshotSink{snapFile}))
	restoredStore := &Store{logger: s.logger}
	restoredFSM := NewFSM(restoredStore)
	restoreFrom, err2 := os.Open(snapFile.Name())
	require.NoError(t, err2)
	defer func() { require.NoError(t, restoreFrom.Close()) }()
	require.NoError(t, restoredFSM.Restore(restoreFrom))
	assert.Equal(t, state.ProtocolVersionProcessDefinitionAllocation, restoredStore.ClusterState().MinProtocolVersion)
}

func TestWriteProcessDefinitionAllocationReportsRejectionWithoutChangingState(t *testing.T) {
	s := newBootstrappedTestStore(t)
	ctx := context.Background()

	_, _, err := s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("a"), VersionTag: new("stable"), TimestampMillis: new(time.Now().UnixMilli()),
	})
	require.NoError(t, err)

	_, _, err = s.WriteProcessDefinitionAllocation(ctx, &proto.ProcessDefinitionAllocation{
		ProcessId: new("order"), Checksum: new("b"), VersionTag: new("stable"), TimestampMillis: new(time.Now().UnixMilli()),
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
	// the leader announces its protocol version when it takes leadership;
	// commands of that version are refused until the announcement is applied
	testPoll(t, func() bool {
		return s.ClusterState().Nodes[s.raftID].ProtocolVersion == state.CurrentProtocolVersion
	}, 50*time.Millisecond, 10*time.Second)
	return s
}
