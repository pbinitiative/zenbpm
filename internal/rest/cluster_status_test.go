package rest

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildClusterStatusViewRendersEnumsAsStrings(t *testing.T) {
	c := state.Cluster{
		Config: state.ClusterConfig{DesiredPartitions: 3},
		Partitions: map[uint32]state.Partition{
			1: {Id: 1, LeaderId: "node-1"},
		},
		Nodes: map[string]state.Node{
			"node-1": {
				Id:       "node-1",
				Addr:     "127.0.0.1:8091",
				Suffrage: raft.Voter,
				State:    state.NodeStateStarted,
				Role:     state.RoleLeader,
				Partitions: map[uint32]state.NodePartition{
					1: {Id: 1, State: state.NodePartitionStateInitialized, Role: state.RoleLeader},
				},
			},
			"node-2": {
				Id:       "node-2",
				Addr:     "127.0.0.1:8092",
				Suffrage: raft.Nonvoter,
				State:    state.NodeStateShutdown,
				Role:     state.RoleFollower,
			},
		},
	}

	b, err := json.Marshal(buildClusterStatusView(c))
	require.NoError(t, err)
	s := string(b)

	assert.Contains(t, s, `"suffrage":"Voter"`)
	assert.Contains(t, s, `"suffrage":"Nonvoter"`)
	assert.Contains(t, s, `"state":"NodeStateStarted"`)
	assert.Contains(t, s, `"state":"NodeStateShutdown"`)
	assert.Contains(t, s, `"role":"RoleLeader"`)
	assert.Contains(t, s, `"role":"RoleFollower"`)
	assert.Contains(t, s, `"state":"NodePartitionStateInitialized"`)
	assert.Contains(t, s, `"desiredPartitions":3`)
}
