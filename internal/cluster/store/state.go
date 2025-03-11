package store

import "github.com/hashicorp/raft"

// ClusterState keeps track of the cluster state and holds
// information about currently active partition and Nodes in the cluster.
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

func (c *ClusterState) Copy() ClusterState {
	cs := ClusterState{}
	nodes := make(map[string]Node, len(c.Nodes))
	for nodeId, node := range c.Nodes {
		nodePartitions := make(map[uint32]NodePartition, len(node.Partitions))
		for k, p := range node.Partitions {
			nodePartitions[k] = NodePartition{
				Id:    p.Id,
				State: p.State,
				Role:  p.Role,
			}
		}
		nodes[nodeId] = Node{
			Id:         node.Id,
			Addr:       node.Addr,
			State:      node.State,
			Role:       node.Role,
			Partitions: nodePartitions,
		}
	}
	cs.Nodes = nodes

	partitions := make(map[uint32]Partition, len(c.Partitions))
	for i, p := range c.Partitions {
		partitions[i] = Partition{
			Id:       p.Id,
			LeaderId: p.LeaderId,
		}
	}
	cs.Partitions = partitions
	return cs
}

type Partition struct {
	Id       uint32 `json:"id"`
	LeaderId string `json:"leaderId"`
}

type NodeState int32

const (
	_ = iota
	// node has joined the main cluster and is waiting for instructions from leader
	// or being elected as a leader
	NodeStateStarting
	// node was elected as a leader or received instructions from a leader to join partition group
	NodeStateStarted
	// node is in an error state
	NodeStateError
)

type Role int32

const (
	_ = iota
	RoleFollower
	RoleLeader
)

type NodePartitionState int32

const (
	_ = iota
	NodePartitionStateError
	NodePartitionStateJoining
	NodePartitionStateLeaving
	NodePartitionStateInitializing
	NodePartitionStateInitialized
)

// Node holds the information about raft server node,
// its state in the zen cluster and assigned partitions
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
