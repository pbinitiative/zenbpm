package store

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWriteMaintenanceChangeError(t *testing.T) {
	t.Run("returns the Raft error when the node is not the leader", func(t *testing.T) {
		cfg := config.Cluster{
			NodeId: "non-leader",
			Raft: config.ClusterRaft{
				Dir: t.TempDir(),
			},
		}
		store, listener := newMustTestStore(t, cfg)
		t.Cleanup(func() { require.NoError(t, listener.Close()) })
		require.NoError(t, store.Open())
		t.Cleanup(func() { require.NoError(t, store.Close(true)) })

		err := store.WriteMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: new(true)})

		require.ErrorIs(t, err, raft.ErrNotLeader)
	})
}
