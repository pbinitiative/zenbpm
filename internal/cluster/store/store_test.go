package store

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/rqlite/v8/tcp"
)

// Test_NonOpenStore tests that a non-open Store handles public methods correctly.
func TestNonOpenStore(t *testing.T) {
	c := config.Cluster{
		NodeId: random.String(),
	}
	s, ln := newMustTestStore(t, c)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Stepdown(false); !errors.Is(err, zenerr.ErrNotOpen) {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if s.IsLeader() {
		t.Fatalf("store incorrectly marked as leader")
	}
	if s.HasLeader() {
		t.Fatalf("store incorrectly marked as having leader")
	}
	if _, err := s.IsVoter(); !errors.Is(err, zenerr.ErrNotOpen) {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if _, err := s.CommitIndex(); !errors.Is(err, zenerr.ErrNotOpen) {
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
	if _, err := s.Nodes(); !errors.Is(err, zenerr.ErrNotOpen) {
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
	defer s.Close(true)
	defer ln.Close()
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
	defer s.Close(true)
	defer ln.Close()
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
	defer s.Close(true)
	defer ln.Close()
	if err = s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}

	_, err = s.WaitForLeader(20 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	// wait until fsm applies the log to the state
	testPoll(t, func() bool {
		testNode, ok := s.state.Nodes[testNodeId]
		if !ok {
			t.Error("expected testNode was not found in the store")
		}
		if testNode.State != state.NodeState(proto.NodeState_NODE_STATE_ERROR) {
			t.Error("testNode is in a wrong state")
		}
		return true
	}, 100*time.Millisecond, 5*time.Second)
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
	defer s.Close(true)
	defer ln.Close()
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

	s.WriteNodeChange(&proto.NodeChange{
		NodeId: &testNodeId,
		State:  proto.NodeState_NODE_STATE_ERROR.Enum(),
		Role:   proto.Role_ROLE_TYPE_UNKNOWN.Enum(),
	})

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
	defer snapFile.Close()

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
	defer snapFile.Close()
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
	mux, _, err := network.NewNodeMux(addr)
	if err != nil {
		t.Fatalf("failed to start network mux: %s", err)
	}
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
