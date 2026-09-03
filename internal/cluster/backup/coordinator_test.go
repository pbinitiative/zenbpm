package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
)

func TestRunClusterRestoreRefusesWhenAlreadyRestoring(t *testing.T) {
	deps := RestoreDeps{
		ClusterState: func() state.Cluster {
			return state.Cluster{Restoring: true, Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "n1"}}}
		},
	}
	// garbage reader: if the guard fires first, the bundle is never opened
	_, err := RunClusterRestore(context.Background(), deps, strings.NewReader("not a tar"), true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")
	assert.NotContains(t, err.Error(), "bundle") // proves guard ordering: refused before bundle validation
}
