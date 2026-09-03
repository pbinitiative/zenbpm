package controller

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func TestParsePartitionServerID(t *testing.T) {
	tests := []struct {
		name            string
		serverID        raft.ServerID
		wantNodeID      string
		wantPartitionID uint32
		wantErr         bool
	}{
		{name: "simple node ID", serverID: "zen-node1-partition-5", wantNodeID: "node1", wantPartitionID: 5},
		{name: "node ID with hyphens", serverID: "zen-test-node-1-partition-1", wantNodeID: "test-node-1", wantPartitionID: 1},
		{name: "larger partition number", serverID: "zen-my-node-partition-42", wantNodeID: "my-node", wantPartitionID: 42},
		{name: "empty string", serverID: "", wantErr: true},
		{name: "garbage input", serverID: "garbage", wantErr: true},
		{name: "empty node ID", serverID: "zen--partition-1", wantErr: true},
		{name: "missing zen prefix", serverID: "node1-partition-5", wantErr: true},
		{name: "non-numeric partition", serverID: "zen-node1-partition-abc", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeID, partitionID, err := parsePartitionServerID(tc.serverID)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNodeID, nodeID)
			assert.Equal(t, tc.wantPartitionID, partitionID)
		})
	}
}
