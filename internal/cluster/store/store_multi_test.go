package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
)

func Test_MultiNode_VerifyLeader(t *testing.T) {
	c1 := config.Cluster{
		RaftDir: filepath.Join(t.TempDir(), "s1"),
	}
	s1, ln1 := newMustTestStore(t, c1)
	defer s1.Close(true)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open store s1: %s", err.Error())
	}

	err := s1.Bootstrap(&Node{
		Id:         s1.raftID,
		Addr:       s1.Addr(),
		Partitions: map[uint32]NodePartition{},
	})
	if err != nil {
		t.Fatalf("failed to bootstrap single-node store s1: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if err := s1.VerifyLeader(); err != nil {
		t.Fatalf("failed to verify leader on single node: %s", err.Error())
	}

	c2 := config.Cluster{
		RaftDir: filepath.Join(t.TempDir(), "s2"),
	}

	s2, ln2 := newMustTestStore(t, c2)
	defer s2.Close(true)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open store s2: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      s2.ID(),
		Address: s2.Addr(),
		Voter:   true,
	}); err != nil {
		t.Fatalf("failed to join single-node store s2: %s", err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	if err := s1.VerifyLeader(); err != nil {
		t.Fatalf("failed to verify leader on leader: %s", err.Error())
	}
	if err := s2.VerifyLeader(); err == nil {
		t.Fatalf("expected error verifying leader on follower")
	}
	if err := s2.Close(true); err != nil {
		t.Fatalf("failed to close follower: %s", err.Error())
	}

	// Be sure the follower is shutdown. It seems that still-open network
	// connections to the follower can cause VerifyLeader to return nil.
	time.Sleep(time.Second)
	if err := s1.VerifyLeader(); err == nil {
		t.Fatalf("expected error verifying leader due to lack of quorum")
	}
}

// Test_MultiNodeSimple tests that a the core operation of a multi-node
// cluster works as expected. That is, with a two node cluster, writes
// actually replicate, and reads are consistent.
func Test_MultiNodeSimple(t *testing.T) {
	testPartitionId := uint32(1)

	c1 := config.Cluster{
		RaftDir: filepath.Join(t.TempDir(), "s1"),
	}
	s1, ln1 := newMustTestStore(t, c1)
	defer s1.Close(true)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open store s1: %s", err.Error())
	}

	if err := s1.Bootstrap(&Node{
		Id:         s1.ID(),
		Addr:       s1.Addr(),
		Partitions: map[uint32]NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	c2 := config.Cluster{
		RaftDir: filepath.Join(t.TempDir(), "s2"),
	}
	s2, ln2 := newMustTestStore(t, c2)
	defer s2.Close(true)
	defer ln2.Close()

	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open store s2: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      s2.ID(),
		Address: s2.Addr(),
		Voter:   true,
	}); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Write some data.
	err := s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      s2.ID(),
		PartitionId: testPartitionId,
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED,
		Role:        proto.Role_ROLE_TYPE_LEADER,
	})
	if err != nil {
		t.Fatalf("failed to write partition change on s1 node: %s", err.Error())
	}
	// Verify that s2 has the same last index as s1
	testPoll(t, func() bool {
		return s1.raft.LastIndex() == s1.raft.AppliedIndex() &&
			s1.raft.AppliedIndex() == s2.raft.AppliedIndex()
	}, 50*time.Millisecond, 10*time.Second)

	// Verify that each node has the same state of partition
	verifyNodeState := func(t *testing.T, s *Store) {
		t.Helper()
		if s.state.Partitions[testPartitionId].LeaderId != s2.ID() {
			t.Logf("node %s has invalid leader of partition %d", s.ID(), testPartitionId)
			t.Fail()
		}
		if s.state.Nodes[s2.ID()].Partitions[testPartitionId].State != NodePartitionState(proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED) {
			t.Logf("node %s has invalid state of node partition %d", s.ID(), testPartitionId)
			t.Fail()
		}
	}
	verifyNodeState(t, s1)
	verifyNodeState(t, s2)

	// Write some more changes to state.
	err = s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      s2.ID(),
		PartitionId: testPartitionId,
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING,
		Role:        proto.Role_ROLE_TYPE_LEADER,
	})
	if err != nil {
		t.Fatalf("failed to write partition change on s1 node: %s", err.Error())
	}

	err = s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      s1.ID(),
		PartitionId: testPartitionId,
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED,
		Role:        proto.Role_ROLE_TYPE_LEADER,
	})
	if err != nil {
		t.Fatalf("failed to write partition change on s1 node: %s", err.Error())
	}

	// Wait for log application
	testPoll(t, func() bool {
		return s1.raft.LastIndex() == s1.raft.AppliedIndex() &&
			s1.raft.AppliedIndex() == s2.raft.AppliedIndex()
	}, 50*time.Millisecond, 10*time.Second)

	// Verify that each node has the same state of partition
	verifyChangedNodeState := func(t *testing.T, s *Store) {
		t.Helper()
		if s.state.Partitions[testPartitionId].LeaderId != s1.ID() {
			t.Logf("node %s has invalid leader of partition %d", s.ID(), testPartitionId)
			t.Fail()
		}
		if _, ok := s.state.Nodes[s2.ID()].Partitions[testPartitionId]; ok {
			t.Logf("expected node s2 to not have test partition")
			t.Fail()
		}
	}
	verifyChangedNodeState(t, s1)
	verifyChangedNodeState(t, s2)
}
