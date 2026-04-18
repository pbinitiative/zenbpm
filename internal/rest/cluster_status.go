package rest

import (
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

// clusterStatusView is the presentation shape of state.Cluster for the
// /system/status HTTP endpoint. Enum fields render as their String() name so
// the response is human-readable. The internal state.Cluster type still
// marshals with numeric enums — which is what raft snapshots rely on.
type clusterStatusView struct {
	ClusterConfig clusterStatusConfigView      `json:"clusterConfig"`
	Partitions    map[uint32]state.Partition   `json:"partitions"`
	Nodes         map[string]clusterStatusNode `json:"nodes"`
}

type clusterStatusConfigView struct {
	DesiredPartitions uint32 `json:"desiredPartitions"`
}

type clusterStatusNode struct {
	Id         string                                `json:"id"`
	Addr       string                                `json:"addr"`
	Suffrage   string                                `json:"suffrage"`
	State      string                                `json:"state"`
	Role       string                                `json:"role"`
	Partitions map[uint32]clusterStatusNodePartition `json:"partitions"`
}

type clusterStatusNodePartition struct {
	Id    uint32 `json:"id"`
	State string `json:"state"`
	Role  string `json:"role"`
}

func buildClusterStatusView(c state.Cluster) clusterStatusView {
	nodes := make(map[string]clusterStatusNode, len(c.Nodes))
	for id, n := range c.Nodes {
		parts := make(map[uint32]clusterStatusNodePartition, len(n.Partitions))
		for pid, p := range n.Partitions {
			parts[pid] = clusterStatusNodePartition{
				Id:    p.Id,
				State: p.State.String(),
				Role:  p.Role.String(),
			}
		}
		nodes[id] = clusterStatusNode{
			Id:         n.Id,
			Addr:       n.Addr,
			Suffrage:   suffrageName(n.Suffrage),
			State:      n.State.String(),
			Role:       n.Role.String(),
			Partitions: parts,
		}
	}
	return clusterStatusView{
		ClusterConfig: clusterStatusConfigView{DesiredPartitions: c.Config.DesiredPartitions},
		Partitions:    c.Partitions,
		Nodes:         nodes,
	}
}

func suffrageName(s raft.ServerSuffrage) string {
	switch s {
	case raft.Voter:
		return "Voter"
	case raft.Nonvoter:
		return "Nonvoter"
	case raft.Staging:
		return "Staging"
	}
	return s.String()
}
