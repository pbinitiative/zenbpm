package cluster

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/sql"
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

// ClusterRestore restores the whole cluster from a backup bundle.
// Without force it refuses when the cluster holds any definitions/instances.
func (node *ZenNode) ClusterRestore(ctx context.Context, r io.Reader, force bool) (*backup.RestoreReport, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create restore spool dir: %w", err)
	}
	defer os.RemoveAll(spoolDir)
	migDir := node.controller.Config.Persistence.Migration.Dir
	if migDir == "" {
		migDir = sql.DefaultMigrationsDir
	}
	binSchema, err := backup.BinarySchemaVersion(migDir)
	if err != nil {
		return nil, err
	}
	deps := backup.RestoreDeps{
		Clients:      node.client,
		ClusterState: node.store.ClusterState,
		SetRestoring: func(restoring bool) error {
			return node.store.WriteMaintenanceChange(&protoc.ClusterMaintenanceChange{Restoring: new(restoring)})
		},
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
	}
	return backup.RunClusterRestore(ctx, deps, r, force)
}
