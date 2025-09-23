// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	pb "google.golang.org/protobuf/proto"
)

type ClusterStateObserverFunc func(ctx context.Context)

// FSM is Finite State Machine of the system state
type FSM struct {
	store *Store
	// context used by the observer of a previous call
	previousChangeCtxCancel    context.CancelFunc
	clusterStateChangeObserver ClusterStateObserverFunc
}

// NewFSM returns a new FSM.
func NewFSM(s *Store) *FSM {
	return &FSM{store: s, clusterStateChangeObserver: func(ctx context.Context) {
		// wait for store to be open until we start sending change notifications
		if s.open.Load() {
			s.clusterStateChangeObserver(ctx)
		}
	}}
}

var _ raft.FSM = &FSM{}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var command proto.Command
	if err := pb.Unmarshal(l.Data, &command); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	var res interface{}
	switch command.GetType() {
	case proto.Command_TYPE_NODE_CHANGE:
		nodeChangeCommand := command.GetNodeChange()
		res = f.applyNodeChange(nodeChangeCommand)
	case proto.Command_TYPE_NODE_PARTITION_CHANGE:
		partitionChangeCommand := command.GetNodePartitionChange()
		res = f.applyPartitionChange(partitionChangeCommand)
	default:
		panic(fmt.Sprintf("unrecognized command type: %s", command.Type))
	}
	if f.store.clusterStateChangeObserver != nil {
		// cancel the context of previous goroutine to let it know that there is a newer change
		if f.previousChangeCtxCancel != nil {
			f.previousChangeCtxCancel()
		}
		ctx, cancel := context.WithCancel(context.Background())
		f.previousChangeCtxCancel = cancel
		go f.clusterStateChangeObserver(ctx)
	}
	f.store.appliedTarget.Signal(l.Index)
	return res
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()

	return &fsmSnapshot{ClusterState: *f.store.state.DeepCopy()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	snapshot := fsmSnapshot{}
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.store.state = snapshot.ClusterState
	return nil
}

type FsmStore interface {
	LeaderID() (string, error)
	ClusterState() state.Cluster
}

func (f *FSM) applyNodeChange(nodeChangeCommand *proto.NodeChange) interface{} {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	changedState := FsmApplyNodeChange(f.store, nodeChangeCommand)
	f.store.state = changedState
	return nil
}

func (f *FSM) applyPartitionChange(partitionChangeCommand *proto.NodePartitionChange) interface{} {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	changedState := FsmApplyPartitionChange(f.store, partitionChangeCommand)
	f.store.state = changedState
	return nil
}

func FsmApplyNodeChange(store FsmStore, nodeChangeCommand *proto.NodeChange) state.Cluster {
	s := store.ClusterState()
	node, ok := s.Nodes[nodeChangeCommand.NodeId]
	// node is not yet present in the store
	role := state.RoleFollower
	leaderId, _ := store.LeaderID()
	if leaderId == nodeChangeCommand.NodeId {
		role = state.RoleLeader
	}
	if !ok {
		// TODO: check state of the node it should be starting
		node = state.Node{
			Id:         nodeChangeCommand.NodeId,
			Addr:       nodeChangeCommand.Addr,
			State:      state.NodeState(nodeChangeCommand.State),
			Partitions: map[uint32]state.NodePartition{},
		}
	}
	// if the leader has changed, change other nodes to be followers
	if leaderId == node.Id && node.Role < state.RoleLeader && role == state.RoleLeader {
		for k, n := range s.Nodes {
			n.Role = state.RoleFollower
			s.Nodes[k] = n
		}
	}
	node.Role = role
	if nodeChangeCommand.Addr != "" {
		node.Addr = nodeChangeCommand.Addr
	}
	if nodeChangeCommand.Suffrage != proto.RaftSuffrage_RAFT_SUFFRAGE_UNKNOWN {
		switch nodeChangeCommand.Suffrage {
		case proto.RaftSuffrage_RAFT_SUFFRAGE_VOTER:
			node.Suffrage = raft.Voter
		case proto.RaftSuffrage_RAFT_SUFFRAGE_NONVOTER:
			node.Suffrage = raft.Nonvoter
		}
	}
	if nodeChangeCommand.State != proto.NodeState_NODE_STATE_UNKNOWN {
		node.State = state.NodeState(nodeChangeCommand.State)
	}
	s.Nodes[nodeChangeCommand.NodeId] = node
	return s
}

func FsmApplyPartitionChange(store FsmStore, partitionChangeCommand *proto.NodePartitionChange) state.Cluster {
	s := store.ClusterState()
	node, ok := s.Nodes[partitionChangeCommand.NodeId]
	// node is not yet present in the store
	if !ok {
		node = state.Node{
			Id:         partitionChangeCommand.NodeId,
			Partitions: make(map[uint32]state.NodePartition),
		}
	}
	if partitionChangeCommand.State == proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING {
		delete(node.Partitions, partitionChangeCommand.PartitionId)
		s.Nodes[partitionChangeCommand.NodeId] = node
		return s
	}
	node.Partitions[partitionChangeCommand.PartitionId] = state.NodePartition{
		Id:    partitionChangeCommand.PartitionId,
		State: state.NodePartitionState(partitionChangeCommand.State),
		Role:  state.Role(partitionChangeCommand.Role),
	}
	if partitionChangeCommand.Role == proto.Role_ROLE_TYPE_LEADER {
		s.Partitions[partitionChangeCommand.PartitionId] = state.Partition{
			Id:       partitionChangeCommand.PartitionId,
			LeaderId: partitionChangeCommand.NodeId,
		}
	}
	s.Nodes[partitionChangeCommand.NodeId] = node
	return s
}
