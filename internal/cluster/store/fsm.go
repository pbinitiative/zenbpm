package store

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	pb "google.golang.org/protobuf/proto"
)

// FSM is Finite State Machine of the system state
type FSM struct {
	s *Store
}

// NewFSM returns a new FSM.
func NewFSM(s *Store) *FSM {
	return &FSM{s: s}
}

var _ raft.FSM = &FSM{}

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var command proto.Command
	if err := pb.Unmarshal(l.Data, &command); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch command.GetType() {
	case proto.Command_TYPE_NODE_CHANGE:
		nodeChangeCommand := command.GetNodeChange()
		return f.applyNodeChange(nodeChangeCommand)
	case proto.Command_TYPE_NODE_PARTITION_CHANGE:
		partitionChangeCommand := command.GetNodePartitionChange()
		return f.applyPartitionChange(partitionChangeCommand)
	default:
		panic(fmt.Sprintf("unrecognized command type: %s", command.Type))
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()

	return &fsmSnapshot{ClusterState: *f.s.state.DeepCopy()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	snapshot := fsmSnapshot{}
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.s.state = snapshot.ClusterState
	return nil
}

func (f *FSM) applyNodeChange(nodeChangeCommand *proto.NodeChange) interface{} {
	node, ok := f.s.state.Nodes[nodeChangeCommand.NodeId]
	// node is not yet present in the store
	role := RoleFollower
	leaderId, _ := f.s.LeaderID()
	if leaderId == nodeChangeCommand.NodeId {
		role = RoleLeader
	}
	if !ok {
		// TODO: check state of the node it should be starting
		node = Node{
			Id:         nodeChangeCommand.NodeId,
			Addr:       nodeChangeCommand.Addr,
			State:      NodeState(nodeChangeCommand.State),
			Partitions: map[uint32]NodePartition{},
		}
	}
	// if the leader has changed, change other nodes to be followers
	if leaderId == node.Id && node.Role < RoleLeader && role == RoleLeader {
		for k, n := range f.s.state.Nodes {
			n.Role = RoleFollower
			f.s.state.Nodes[k] = n
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
		node.State = NodeState(nodeChangeCommand.State)
	}
	f.s.state.Nodes[nodeChangeCommand.NodeId] = node
	return nil
}

func (f *FSM) applyPartitionChange(partitionChangeCommand *proto.NodePartitionChange) interface{} {
	node, ok := f.s.state.Nodes[partitionChangeCommand.NodeId]
	// node is not yet present in the store
	if !ok {
		node = Node{
			Id:         partitionChangeCommand.NodeId,
			Partitions: make(map[uint32]NodePartition),
		}
	}
	if partitionChangeCommand.State == proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING {
		delete(node.Partitions, partitionChangeCommand.PartitionId)
		f.s.state.Nodes[partitionChangeCommand.NodeId] = node
		return nil
	}
	node.Partitions[partitionChangeCommand.PartitionId] = NodePartition{
		Id:    partitionChangeCommand.PartitionId,
		State: NodePartitionState(partitionChangeCommand.State),
		Role:  Role(partitionChangeCommand.Role),
	}
	if partitionChangeCommand.Role == proto.Role_ROLE_TYPE_LEADER {
		partition := f.s.state.Partitions[partitionChangeCommand.PartitionId]
		partition.LeaderId = partitionChangeCommand.NodeId
		f.s.state.Partitions[partitionChangeCommand.PartitionId] = partition
	}
	f.s.state.Nodes[partitionChangeCommand.NodeId] = node
	return nil
}
