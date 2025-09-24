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
	currState := store.ClusterState()
	node, ok := currState.Nodes[nodeChangeCommand.GetNodeId()]
	// node is not yet present in the store
	role := state.RoleFollower
	leaderId, _ := store.LeaderID()
	if leaderId == nodeChangeCommand.GetNodeId() {
		role = state.RoleLeader
	}
	if !ok {
		// TODO: check state of the node it should be starting
		node = state.Node{
			Id:         nodeChangeCommand.GetNodeId(),
			Addr:       nodeChangeCommand.GetAddr(),
			State:      state.NodeState(nodeChangeCommand.GetState()),
			Partitions: map[uint32]state.NodePartition{},
		}
	}
	// if the leader has changed, change other nodes to be followers
	if leaderId == node.Id && node.Role < state.RoleLeader && role == state.RoleLeader {
		for k, n := range currState.Nodes {
			n.Role = state.RoleFollower
			currState.Nodes[k] = n
		}
	}
	node.Role = role
	if nodeChangeCommand.GetAddr() != "" {
		node.Addr = nodeChangeCommand.GetAddr()
	}
	if nodeChangeCommand.GetSuffrage() != proto.RaftSuffrage_RAFT_SUFFRAGE_UNKNOWN {
		switch nodeChangeCommand.GetSuffrage() {
		case proto.RaftSuffrage_RAFT_SUFFRAGE_VOTER:
			node.Suffrage = raft.Voter
		case proto.RaftSuffrage_RAFT_SUFFRAGE_NONVOTER:
			node.Suffrage = raft.Nonvoter
		}
	}
	if nodeChangeCommand.GetState() != proto.NodeState_NODE_STATE_UNKNOWN {
		node.State = state.NodeState(nodeChangeCommand.GetState())
	}
	currState.Nodes[nodeChangeCommand.GetNodeId()] = node
	return currState
}

func FsmApplyPartitionChange(store FsmStore, partitionChangeCommand *proto.NodePartitionChange) state.Cluster {
	currState := store.ClusterState()
	node, ok := currState.Nodes[partitionChangeCommand.GetNodeId()]
	// node is not yet present in the store
	if !ok {
		node = state.Node{
			Id:         partitionChangeCommand.GetNodeId(),
			Partitions: make(map[uint32]state.NodePartition),
		}
	}
	if partitionChangeCommand.GetState() == proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING {
		delete(node.Partitions, partitionChangeCommand.GetPartitionId())
		currState.Nodes[partitionChangeCommand.GetNodeId()] = node
		return currState
	}
	node.Partitions[partitionChangeCommand.GetPartitionId()] = state.NodePartition{
		Id:    partitionChangeCommand.GetPartitionId(),
		State: state.NodePartitionState(partitionChangeCommand.GetState()),
		Role:  state.Role(partitionChangeCommand.GetRole()),
	}
	if partitionChangeCommand.GetRole() == proto.Role_ROLE_TYPE_LEADER {
		currState.Partitions[partitionChangeCommand.GetPartitionId()] = state.Partition{
			Id:       partitionChangeCommand.GetPartitionId(),
			LeaderId: partitionChangeCommand.GetNodeId(),
		}
	}
	currState.Nodes[partitionChangeCommand.GetNodeId()] = node
	return currState
}
