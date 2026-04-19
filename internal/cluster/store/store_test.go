package store

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/rqlitecompat/random"
	"github.com/rqlite/rqlite/v10/tcp"
	"github.com/stretchr/testify/require"
)

// Test_NonOpenStore tests that a non-open Store handles public methods correctly.
func TestNonOpenStore(t *testing.T) {
	c := config.Cluster{
		NodeId: random.String(),
	}
	s, ln := newMustTestStore(t, c)
	defer func() { require.NoError(t, s.Close(true)) }()
	defer func() { require.NoError(t, ln.Close()) }()

	if err := s.Stepdown(false); err != zenerr.ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if s.IsLeader() {
		t.Fatalf("store incorrectly marked as leader")
	}
	if s.HasLeader() {
		t.Fatalf("store incorrectly marked as having leader")
	}
	if _, err := s.IsVoter(); err != zenerr.ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if _, err := s.CommitIndex(); err != zenerr.ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if addr, err := s.LeaderAddr(); addr != "" || err != nil {
		t.Fatalf("wrong leader address returned for non-open store: %s", addr)
	}
	if id, err := s.LeaderID(); id != "" || err != nil {
		t.Fatalf("wrong leader ID returned for non-open store: %s", id)
	}
	if addr, id := s.LeaderWithID(); addr != "" || id != "" {
		t.Fatalf("wrong leader address and ID returned for non-open store: %s", id)
	}
	if s.HasLeaderID() {
		t.Fatalf("store incorrectly marked as having leader ID")
	}
	if _, err := s.Nodes(); err != zenerr.ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
}

// Test_OpenStoreSingleNode tests that a single node basically operates.
func TestOpenStoreSingleNode(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
	}

	s, ln := newMustTestStore(t, c)
	defer func() { require.NoError(t, s.Close(true)) }()
	defer func() { require.NoError(t, ln.Close()) }()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if !s.HasLeaderID() {
		t.Fatalf("store not marked as having leader ID")
	}
	_, err = s.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	id, err := waitForLeaderID(s, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}
}

// Test_StoreRestartSingleNode tests that a store shutdown and opening a new instance results in the same state.
func TestStoreRestartSingleNode(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
		NodeId: random.String(),
	}

	s, ln := newMustTestStore(t, c)
	defer func(store *Store) { require.NoError(t, store.Close(true)) }(s)
	defer func(listener net.Listener) { require.NoError(t, listener.Close()) }(ln)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}

	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	testNodeId := "test-node-id"

	err = s.WriteNodeChange(&proto.NodeChange{
		NodeId: &testNodeId,
		State:  proto.NodeState_NODE_STATE_ERROR.Enum(),
		Role:   proto.Role_ROLE_TYPE_UNKNOWN.Enum(),
	})
	if err != nil {
		t.Fatalf("failed to write node change: %s", err)
	}
	err = s.Close(true)
	if err != nil {
		t.Fatalf("failed to close the store: %s", err)
	}

	s, ln = newMustTestStore(t, c)
	defer func(store *Store) { require.NoError(t, store.Close(true)) }(s)
	defer func(listener net.Listener) { require.NoError(t, listener.Close()) }(ln)
	if err = s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}

	_, err = s.WaitForLeader(20 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	// wait until fsm applies the log to the state
	require.Eventually(t, func() bool {
		testNode, ok := s.state.Nodes[testNodeId]
		if !ok {
			return false
		}
		return testNode.State == state.NodeState(proto.NodeState_NODE_STATE_ERROR)
	}, 5*time.Second, 100*time.Millisecond, "expected testNode to be found in the store in error state")
}

// TestShutdownNodeIsIdempotent verifies that calling shutdownNode twice on
// a node that is already in NodeStateShutdown does not produce a second
// Raft log entry.
//
// Regression: the raft observer fires FailedHeartbeatObservation every
// heartbeat interval for a dead peer. shutdownNode was writing a fresh
// NodeChange on every call, producing unbounded Raft log growth and a
// runaway cluster-state-change notification loop on all nodes.
func TestShutdownNodeIsIdempotent(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
		NodeId: random.String(),
	}

	s, ln := newMustTestStore(t, c)
	defer s.Close(true)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %s", err)
	}

	// Seed a peer in the SHUTDOWN state by writing the NodeChange directly,
	// which is the same path shutdownNode itself takes. This ensures the
	// precondition (node already marked Shutdown) is established via the FSM.
	peerId := "peer-1"
	if err := s.WriteNodeChange(&proto.NodeChange{
		NodeId: &peerId,
		State:  proto.NodeState_NODE_STATE_SHUTDOWN.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}); err != nil {
		t.Fatalf("failed to seed peer as Shutdown: %s", err)
	}
	testPoll(t, func() bool {
		n, ok := s.state.Nodes[peerId]
		return ok && n.State == state.NodeStateShutdown
	}, 50*time.Millisecond, 5*time.Second)

	indexBefore := s.raft.LastIndex()

	// A fresh shutdownNode call for an already-shutdown node must be a no-op.
	if err := s.shutdownNode(raft.ServerID(peerId)); err != nil {
		t.Fatalf("shutdownNode returned error on idempotent call: %s", err)
	}

	// Small grace period so any (unwanted) new log entry would be committed and
	// visible to LastIndex. 100ms is well beyond the single-node apply window.
	time.Sleep(100 * time.Millisecond)
	indexAfter := s.raft.LastIndex()

	if indexAfter != indexBefore {
		t.Fatalf("shutdownNode on already-shutdown node produced new log entries: before=%d after=%d",
			indexBefore, indexAfter)
	}
}

// TestShutdownNodeClearsPartitionRoles verifies that when a node is marked
// as Shutdown its NodePartition entries are no longer advertised as
// RoleFollower — otherwise GetPartitionFollower could still route reads to
// a dead address.
//
// The test asserts the end-state of cluster state after shutdownNode; the
// exact mechanism (extra NodePartitionChange writes vs. FSM-level clearing)
// is an implementation choice.
func TestShutdownNodeClearsPartitionRoles(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
		NodeId: random.String(),
	}

	s, ln := newMustTestStore(t, c)
	defer s.Close(true)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %s", err)
	}

	peerId := "peer-1"

	// Register the peer as a Started Follower on partition 1. This mirrors
	// the pre-crash state of a live partition follower.
	if err := s.WriteNodeChange(&proto.NodeChange{
		NodeId: &peerId,
		State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}); err != nil {
		t.Fatalf("failed to register peer: %s", err)
	}
	if err := s.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      &peerId,
		PartitionId: new(uint32(1)),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED.Enum(),
		Role:        proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}); err != nil {
		t.Fatalf("failed to set peer partition role: %s", err)
	}
	testPoll(t, func() bool {
		n, ok := s.state.Nodes[peerId]
		if !ok {
			return false
		}
		p, ok := n.Partitions[1]
		return ok && p.Role == state.RoleFollower
	}, 50*time.Millisecond, 5*time.Second)

	// Now mark the peer as shutdown.
	if err := s.shutdownNode(raft.ServerID(peerId)); err != nil {
		t.Fatalf("shutdownNode returned error: %s", err)
	}

	// After shutdown, the node's partition role must not be Follower —
	// otherwise a stale Follower entry keeps the dead node eligible for
	// follower-routed reads.
	testPoll(t, func() bool {
		n, ok := s.state.Nodes[peerId]
		if !ok {
			return false
		}
		if n.State != state.NodeStateShutdown {
			return false
		}
		p, ok := n.Partitions[1]
		if !ok {
			// acceptable: partition entry removed entirely
			return true
		}
		return p.Role != state.RoleFollower
	}, 50*time.Millisecond, 5*time.Second)
}

// TestResumeNodeRestoresPartitionFollowerRole verifies that a follower whose
// NodePartition.Role was cleared to UNKNOWN by shutdownNode has it restored
// to Follower on heartbeat resume. Without this, role selectors that switch
// on Role silently skip the node (see Cluster.GetPartitionFollower), causing
// read load to pile on the leader after a transient cluster partition.
//
// The partition-leader slot is intentionally not restored here — that's the
// partition raft's authority, driven by PartitionNodeLeaderChange.
func TestResumeNodeRestoresPartitionFollowerRole(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
		NodeId: random.String(),
	}

	s, ln := newMustTestStore(t, c)
	defer s.Close(true)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %s", err)
	}

	peerId := "peer-1"

	// Register a follower on partition 1, mirroring the pre-partition state.
	if err := s.WriteNodeChange(&proto.NodeChange{
		NodeId: ptr.To(peerId),
		State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}); err != nil {
		t.Fatalf("failed to register peer: %s", err)
	}
	if err := s.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      ptr.To(peerId),
		PartitionId: ptr.To(uint32(1)),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED.Enum(),
		Role:        proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}); err != nil {
		t.Fatalf("failed to set peer partition role: %s", err)
	}
	testPoll(t, func() bool {
		n, ok := s.ClusterState().Nodes[peerId]
		if !ok {
			return false
		}
		p, ok := n.Partitions[1]
		return ok && p.Role == state.RoleFollower
	}, 50*time.Millisecond, 5*time.Second)

	// Shut the peer down: clears its partition role to UNKNOWN (zero value).
	if err := s.shutdownNode(raft.ServerID(peerId)); err != nil {
		t.Fatalf("shutdownNode returned error: %s", err)
	}
	testPoll(t, func() bool {
		n, ok := s.ClusterState().Nodes[peerId]
		if !ok || n.State != state.NodeStateShutdown {
			return false
		}
		p, ok := n.Partitions[1]
		return !ok || p.Role != state.RoleFollower
	}, 50*time.Millisecond, 5*time.Second)

	// Resume the peer. NodeState flips back to Started and the partition
	// role is restored to Follower as a side-effect of the NodeChange FSM apply.
	if err := s.resumeNode(raft.ServerID(peerId)); err != nil {
		t.Fatalf("resumeNode returned error: %s", err)
	}
	testPoll(t, func() bool {
		n, ok := s.ClusterState().Nodes[peerId]
		if !ok || n.State != state.NodeStateStarted {
			return false
		}
		p, ok := n.Partitions[1]
		return ok && p.Role == state.RoleFollower
	}, 50*time.Millisecond, 5*time.Second)
}

// Test_SingleNodeSnapshot tests that the Store correctly takes a snapshot
// and recovers from it.
func TestSingleNodeSnapshot(t *testing.T) {
	c := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: t.TempDir(),
		},
	}

	s, ln := newMustTestStore(t, c)
	defer func() { require.NoError(t, s.Close(true)) }()
	defer func() { require.NoError(t, ln.Close()) }()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}

	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	testNodeId := "test-node"

	if err := s.WriteNodeChange(&proto.NodeChange{
		NodeId: &testNodeId,
		State:  proto.NodeState_NODE_STATE_ERROR.Enum(),
		Role:   proto.Role_ROLE_TYPE_UNKNOWN.Enum(),
	}); err != nil {
		t.Fatalf("failed to write node change: %s", err)
	}

	// Snap the node and write to disk.
	f := s.raft.Snapshot()
	if f.Error() != nil {
		t.Fatalf("failed to snapshot node: %s", f.Error())
	}

	snapDir := t.TempDir()
	snapFile, err := os.Create(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to create snapshot file: %s", err.Error())
	}

	fsm := NewFSM(s)
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot from the store: %s", err)
	}

	sink := &mockSnapshotSink{snapFile}
	if err = snapshot.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot to disk: %s", err.Error())
	}

	// Zero out the state
	s.state = state.Cluster{}

	// Check restoration.
	snapFile, err = os.Open(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to open snapshot file: %s", err.Error())
	}
	defer func(file *os.File) { require.NoError(t, file.Close()) }(snapFile)
	if err := fsm.Restore(snapFile); err != nil {
		t.Fatalf("failed to restore snapshot from disk: %s", err.Error())
	}

	// Ensure state is back in the correct state.
	restoredNode, ok := s.state.Nodes[testNodeId]
	if !ok {
		t.Fatalf("failed to read test-node from restored snapshot")
	}
	if restoredNode.Id != "test-node" {
		t.Fatalf("expected node Id to be %s was %s", testNodeId, restoredNode.Id)
	}
	if restoredNode.State != state.NodeStateError {
		t.Fatalf("expected node Id to be %s was %s", testNodeId, restoredNode.Id)
	}
}

// waitForLeaderID waits until the Store's LeaderID is set, or the timeout
// expires. Because setting Leader ID requires Raft to set the cluster
// configuration, it's not entirely deterministic when it will be set.
func waitForLeaderID(s *Store, timeout time.Duration) (string, error) {
	tck := time.NewTicker(100 * time.Millisecond)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			id, err := s.LeaderID()
			if err != nil {
				return "", err
			}
			if id != "" {
				return id, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

type mockSnapshotSink struct {
	*os.File
}

func (m *mockSnapshotSink) ID() string {
	return "1"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func newMustTestStore(t *testing.T, c config.Cluster) (*Store, net.Listener) {
	addr := ""
	if c.Addr != "" {
		addr = c.Addr
	}
	mux, muxLn, err := network.NewNodeMux(addr)
	if err != nil {
		t.Fatalf("failed to start network mux: %s", err)
	}
	t.Cleanup(func() { require.NoError(t, muxLn.Close()) })
	ln := network.NewZenBpmRaftListener(mux)
	raftTn := tcp.NewLayer(ln, network.NewZenBpmRaftDialer())

	s := New(raftTn, nil, DefaultConfig(c))
	return s, ln
}

func testPoll(t *testing.T, f func() bool, checkPeriod time.Duration, timeout time.Duration) {
	t.Helper()
	tck := time.NewTicker(checkPeriod)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if f() {
				return
			}
		case <-tmr.C:
			t.Fatalf("timeout expired: %s", t.Name())
		}
	}
}
