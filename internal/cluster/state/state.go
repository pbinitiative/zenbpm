package state

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
)

//go:generate go tool deepcopy-gen . --output-file zz_generated.deepcopy.go

// Cluster keeps track of the cluster state and holds
// information about currently active partition and Nodes in the cluster.
// +k8s:deepcopy-gen=true
type Cluster struct {
	// Config stores desired cluster configuration. At the start of the cluster its picked up from app configuration and later updated by calling GRPC API.
	Config ClusterConfig `json:"clusterConfig"`
	// Partitions stores current cluster partition state
	Partitions map[uint32]Partition `json:"partitions"`
	// Nodes stores information about current cluster members
	Nodes map[string]Node `json:"nodes"`
}

func (c Cluster) GetNode(nodeId string) (Node, error) {
	for id, node := range c.Nodes {
		if id != nodeId {
			continue
		}
		return node, nil
	}
	return Node{}, zenerr.ErrNodeNotFound
}

func (c Cluster) GetLeastStressedPartitionLeader() (Node, error) {
	minNode := Node{}
node:
	for _, node := range c.Nodes {
		if len(node.Partitions) != 0 && (minNode.Partitions == nil || len(minNode.Partitions) > len(node.Partitions)) {
			for _, partition := range node.Partitions {
				if partition.Role != RoleLeader {
					continue
				}
				minNode = node
				continue node
			}
		}
	}
	if minNode.Id == "" {
		return minNode, fmt.Errorf("failed to find node")
	}
	return minNode, nil
}

func (c Cluster) GetLeastStressedNode() (Node, error) {
	minNode := Node{}
	for _, node := range c.Nodes {
		if minNode.Partitions == nil || len(minNode.Partitions) > len(node.Partitions) {
			minNode = node
		}
	}
	if minNode.Id == "" {
		return minNode, fmt.Errorf("failed to find node")
	}
	return minNode, nil
}

// GetPartitionFollower preferably returns partition follower node and if it does not exist it returns the leader
func (c Cluster) GetPartitionFollower(partition uint32) (Node, error) {
	partitionState, ok := c.Partitions[partition]
	if !ok {
		return Node{}, fmt.Errorf("partition not found")
	}
	partitionLeaderId := partitionState.LeaderId
	var partitionFollower Node
	for _, node := range c.Nodes {
		nodePartition, hasPartition := node.Partitions[partition]
		if !hasPartition {
			continue
		}
		if nodePartition.Role == RoleFollower {
			partitionFollower = node
			break
		}
	}
	var winningNode Node
	if partitionFollower.Addr == "" {
		winningNode = c.Nodes[partitionLeaderId]
	} else {
		winningNode = partitionFollower
	}
	return winningNode, nil
}

// GetLeastStressedPartitionLeader returns leader of partition that has the least instances running
func (c Cluster) LeastStressedPartition() (Partition, error) {
	pick := rand.Intn(len(c.Partitions) - 1)
	i := 0
	for _, partition := range c.Partitions {
		if i != pick {
			i++
			continue
		}
		return partition, nil
	}
	return Partition{}, fmt.Errorf("failed to find node")
}

func (c Cluster) AnyNodeHasPartition(partitionId int) bool {
	for _, node := range c.Nodes {
		if _, ok := node.Partitions[uint32(partitionId)]; ok {
			return true
		}
	}
	return false
}

func (c Cluster) PrintDebug() {
	bytes, _ := json.MarshalIndent(c, "", " ")
	fmt.Println(string(bytes))
}

// GetPartitionIdFromString Simple hash function to assign partition id to any str string
func (c Cluster) GetPartitionIdFromString(str string) uint32 {
	if len(c.Partitions) == 0 {
		return 0
	}
	var bitSum uint32 = 0
	for _, character := range str {
		bitSum = bitSum + uint32(character)
	}
	return bitSum%uint32(len(c.Partitions)) + 1
}

// +k8s:deepcopy-gen=true
type ClusterConfig struct {
	DesiredPartitions uint32 `json:"desiredPartitions"`
	// Version           int    `json:"version"`
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
