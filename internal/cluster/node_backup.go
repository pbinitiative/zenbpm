package cluster

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
)

// ClusterBackup streams a whole-cluster backup bundle (plain tar) into w.
// Callable on any node; partition data is pulled from each partition leader.
func (node *ZenNode) ClusterBackup(ctx context.Context, w io.Writer) (*backup.Manifest, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-backup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create backup spool dir: %w", err)
	}
	defer os.RemoveAll(spoolDir)
	return backup.RunClusterBackup(ctx, node.store.ClusterState(), node.client, spoolDir, w)
}
