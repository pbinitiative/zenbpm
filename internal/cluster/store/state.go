package store

import (
	"github.com/hashicorp/raft"
)

//go:generate go tool deepcopy-gen . --output-file zz_generated.deepcopy.go

// ClusterState keeps track of the cluster state and holds
// information about currently active partition and Nodes in the cluster.
// +k8s:deepcopy-gen=true
type ClusterState struct {
	Partitions map[uint32]Partition `json:"partitions"`
	Nodes      map[string]Node      `json:"nodes"`
}

func (c *ClusterState) GetNode(nodeId string) (Node, error) {
	for id, node := range c.Nodes {
		if id != nodeId {
			continue
		}
		return node, nil
	}
	return Node{}, ErrNodeNotFound
}

// +k8s:deepcopy-gen=true
type Partition struct {
	Id       uint32 `json:"id"`
	LeaderId string `json:"leaderId"`
}

//go:generate go tool stringer -type=NodeState
type NodeState int32

const (
	_ NodeState = iota
	// node is in an error state
	NodeStateError
	// node is active
	NodeStateStarted
	// node is not activated
	NodeStateShutdown
)

//go:generate go tool stringer -type=Role
type Role int32

const (
	_ Role = iota
	RoleFollower
	RoleLeader
)

//go:generate go tool stringer -type=NodePartitionState
type NodePartitionState int32

const (
	_ NodePartitionState = iota
	NodePartitionStateError
	NodePartitionStateJoining
	NodePartitionStateLeaving
	NodePartitionStateInitializing
	NodePartitionStateInitialized
)

// Node holds the information about raft server node,
// its state in the zen cluster and assigned partitions
// +k8s:deepcopy-gen=true
type Node struct {
	Id         string                   `json:"id"`
	Addr       string                   `json:"addr"`
	Suffrage   raft.ServerSuffrage      `json:"suffrage"`
	State      NodeState                `json:"state"`
	Role       Role                     `json:"role"`
	Partitions map[uint32]NodePartition `json:"partitions"`
	// TODO: add zones
}

type NodePartition struct {
	Id    uint32             `json:"id"`
	State NodePartitionState `json:"state"`
	// role of a node in partition group
	Role Role `json:"role"`
}

// Nodes is a set of Nodes.
type Nodes []Node

// IsReadOnly returns whether the given node, as specified by its Raft ID,
// is a read-only (non-voting) node. If no node is found with the given ID
// then found will be false.
func (s Nodes) IsReadOnly(id string) (readOnly bool, found bool) {
	readOnly = false
	found = false

	if s == nil || id == "" {
		return
	}

	for _, n := range s {
		if n.Id == id {
			readOnly = n.Suffrage == raft.Nonvoter
			found = true
			return
		}
	}
	return
}

// Contains returns whether the given node, as specified by its Raft ID,
// is a member of the set of servers.
func (s Nodes) Contains(id string) bool {
	if s == nil || id == "" {
		return false
	}

	for _, n := range s {
		if n.Id == id {
			return true
		}
	}
	return false
}

func (s Nodes) Less(i, j int) bool { return s[i].Id < s[j].Id }
func (s Nodes) Len() int           { return len(s) }
func (s Nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
