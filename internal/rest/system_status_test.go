package rest

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSystemStatusResponse(t *testing.T) {
	t.Run("includes build metadata and cluster state", systemStatusResponseIncludesBuildMetadataAndClusterState)
}

func systemStatusResponseIncludesBuildMetadataAndClusterState(t *testing.T) {
	response := newSystemStatusResponse(
		buildinfo.Info{
			Version: "1.5.0",
			Commit:  "0123456789abcdef0123456789abcdef01234567",
		},
		state.Cluster{
			Config: state.ClusterConfig{DesiredPartitions: 3},
			Partitions: map[uint32]state.Partition{
				1: {Id: 1, LeaderId: "node-1"},
			},
			Nodes: map[string]state.Node{},
		},
	)

	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"version": "1.5.0",
		"commit": "0123456789abcdef0123456789abcdef01234567",
		"clusterConfig": {"desiredPartitions": 3},
		"partitions": {"1": {"id": 1, "leaderId": "node-1"}},
		"nodes": {}
	}`, string(encoded))
}
