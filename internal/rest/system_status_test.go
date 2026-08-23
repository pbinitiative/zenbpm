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
	t.Run("separates git and build metadata and includes cluster state", func(t *testing.T) {
		response := newSystemStatusResponse(
			buildinfo.Info{
				Version:   "v1.5.0",
				Commit:    "0123456789abcdef0123456789abcdef01234567",
				Branch:    "main",
				BuildTime: "2026-08-07T12:13:14Z",
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
			"git": {
				"branch": "main",
				"commitId": "0123456789ab"
			},
			"build": {
				"version": "v1.5.0",
				"time": "2026-08-07T12:13:14Z"
			},
			"clusterConfig": {"desiredPartitions": 3},
			"partitions": {"1": {"id": 1, "leaderId": "node-1"}},
			"nodes": {}
		}`, string(encoded))
	})

	t.Run("preserves a commit ID that is already short", func(t *testing.T) {
		assert.Equal(t, "unknown", shortCommitID("unknown"))
	})
}
