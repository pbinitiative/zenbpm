// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/rqlite/rqlite/v8/random"
)

func Test_MultiNode_VerifyLeader(t *testing.T) {
	c1 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s1"),
		},
	}
	s1, ln1 := newMustTestStore(t, c1)
	defer s1.Close(true)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open store s1: %s", err.Error())
	}

	err := s1.Bootstrap(&state.Node{
		Id:         s1.raftID,
		Addr:       s1.Addr(),
		Partitions: map[uint32]state.NodePartition{},
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
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s2"),
		},
	}

	s2, ln2 := newMustTestStore(t, c2)
	defer s2.Close(true)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open store s2: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      ptr.To(s2.ID()),
		Address: ptr.To(s2.Addr()),
		Voter:   ptr.To(true),
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

// Test_MultiNodeSimple tests that the core operation of a multi-node
// cluster works as expected. That is, with a two node cluster, writes
// actually replicate, and reads are consistent.
func Test_MultiNodeSimple(t *testing.T) {
	testPartitionId := uint32(1)

	c1 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s1"),
		},
	}
	s1, ln1 := newMustTestStore(t, c1)
	defer s1.Close(true)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open store s1: %s", err.Error())
	}

	if err := s1.Bootstrap(&state.Node{
		Id:         s1.ID(),
		Addr:       s1.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	c2 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s2"),
		},
	}
	s2, ln2 := newMustTestStore(t, c2)
	defer s2.Close(true)
	defer ln2.Close()

	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open store s2: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      ptr.To(s2.ID()),
		Address: ptr.To(s2.Addr()),
		Voter:   ptr.To(true),
	}); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Write some data.
	err := s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      ptr.To(s2.ID()),
		PartitionId: ptr.To(testPartitionId),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED.Enum(),
		Role:        proto.Role_ROLE_TYPE_LEADER.Enum(),
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
		if s.state.Nodes[s2.ID()].Partitions[testPartitionId].State != state.NodePartitionState(proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED) {
			t.Logf("node %s has invalid state of node partition %d", s.ID(), testPartitionId)
			t.Fail()
		}
	}
	verifyNodeState(t, s1)
	verifyNodeState(t, s2)

	// Write some more changes to state.
	err = s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      ptr.To(s2.ID()),
		PartitionId: ptr.To(testPartitionId),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING.Enum(),
		Role:        proto.Role_ROLE_TYPE_LEADER.Enum(),
	})
	if err != nil {
		t.Fatalf("failed to write partition change on s1 node: %s", err.Error())
	}

	err = s1.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      ptr.To(s1.ID()),
		PartitionId: ptr.To(testPartitionId),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED.Enum(),
		Role:        proto.Role_ROLE_TYPE_LEADER.Enum(),
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

func Test_MultiNodePeerObservations(t *testing.T) {
	c1 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s1"),
		},
		NodeId: fmt.Sprintf("s1-%s", random.String()),
	}
	s1, ln1 := newMustTestStore(t, c1)
	defer s1.Close(true)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open store s1: %s", err.Error())
	}

	if err := s1.Bootstrap(&state.Node{
		Id:         s1.ID(),
		Addr:       s1.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	c2 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s2"),
		},
		NodeId: fmt.Sprintf("s2-%s", random.String()),
	}
	s2, ln2 := newMustTestStore(t, c2)
	defer s2.Close(true)
	defer ln2.Close()

	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open store s2: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      ptr.To(s2.ID()),
		Address: ptr.To(s2.Addr()),
		Voter:   ptr.To(true),
	}); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}

	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Wait for log application
	testPoll(t, func() bool {
		return s1.raft.LastIndex() == s1.raft.AppliedIndex() &&
			s1.raft.AppliedIndex() == s2.raft.AppliedIndex()
	}, 50*time.Millisecond, 10*time.Second)

	verifyStoreUpdatedNodeState := func(t *testing.T, s *Store) {
		t.Helper()
		s1, ok := s.state.Nodes[s1.ID()]
		if !ok {
			t.Logf("expected s1 to be present in state of %s", s.ID())
			t.Fail()
		}
		if s1.Role != state.RoleLeader {
			t.Logf("expected s1 to be leader in state of %s", s.ID())
			t.Fail()
		}
		s2, ok := s.state.Nodes[s2.ID()]
		if !ok {
			t.Logf("expected s2 to be present in state of %s", s.ID())
			t.Fail()
		}
		if s2.Role != state.RoleFollower {
			t.Logf("expected s2 to be follower in state of %s", s.ID())
			t.Fail()
		}
	}

	verifyStoreUpdatedNodeState(t, s1)
	verifyStoreUpdatedNodeState(t, s2)

	c3 := config.Cluster{
		Raft: config.ClusterRaft{
			Dir: filepath.Join(t.TempDir(), "s2"),
		},
		NodeId: fmt.Sprintf("s3-%s", random.String()),
	}
	s3, ln3 := newMustTestStore(t, c3)
	defer s3.Close(true)
	defer ln3.Close()

	if err := s3.Open(); err != nil {
		t.Fatalf("failed to open store s3: %s", err.Error())
	}

	if err := s1.Join(&zproto.JoinRequest{
		Id:      ptr.To(s3.ID()),
		Address: ptr.To(s3.Addr()),
		Voter:   ptr.To(true),
	}); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s3.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// after closing s1 the leader should be s2 or s3 with correct state
	err := s1.Close(true)
	if err != nil {
		t.Fatalf("failed to close s1 store: %s", err)
	}

	// Wait for leader change
	testPoll(t, func() bool {
		s2Leader, _ := s2.LeaderID()
		s3Leader, _ := s3.LeaderID()
		return s2Leader != s1.ID() && s3Leader != s1.ID()
	}, 50*time.Millisecond, 10*time.Second)

	// wait for hearbeat to mark node as shut down
	testPoll(t, func() bool {
		s2node, err := s2.state.GetNode(s1.ID())
		if err != nil {
			t.Fatalf("failed to retrieve node from the store: %s", err)
		}
		s3node, err := s3.state.GetNode(s1.ID())
		return s2node.State == state.NodeStateShutdown && s3node.State == state.NodeStateShutdown
	}, 50*time.Millisecond, 10*time.Second)

	// verify that the same leader was picked
	newLeader := ""
	for _, n := range s2.state.Nodes {
		if n.Role == state.RoleLeader {
			newLeader = n.Id
			break
		}
	}
	for _, n := range s3.state.Nodes {
		if n.Role == state.RoleLeader {
			if newLeader != n.Id {
				t.Fatalf("expected s2 and s3 to have the same leader")
			}
		}
	}

	verifyNodeIsShutdown := func(t *testing.T, node *Store, s *Store) {
		st, ok := s.state.Nodes[node.ID()]
		if !ok {
			t.Logf("expected %s to be present in state of %s", node.ID(), s.ID())
			t.Fail()
		}
		if st.Role != state.RoleFollower {
			t.Logf("expected %s to be follower in state of %s", node.ID(), s.ID())
			t.Fail()
		}
		if st.State != state.NodeStateShutdown {
			t.Logf("expected %s to be shutdown in state of %s", node.ID(), s.ID())
			t.Fail()
		}
	}
	verifyNodeIsShutdown(t, s1, s2)
	verifyNodeIsShutdown(t, s1, s3)
}
