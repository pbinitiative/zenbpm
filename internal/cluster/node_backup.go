package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/sql"
)

// ClusterBackup streams a whole-cluster backup bundle (plain tar) into w.
// Callable on any node; partition data is pulled from each partition leader.
func (node *ZenNode) ClusterBackup(ctx context.Context, w io.Writer) (*backup.Manifest, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-backup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create backup spool dir: %w", err)
	}
	defer node.removeSpoolDir(spoolDir)
	return backup.RunClusterBackup(ctx, node.store.ClusterState(), node.client, spoolDir, w)
}

// ClusterRestore restores the whole cluster from a backup bundle. It must run
// on the cluster raft leader (restore state is written through raft). Without
// force it refuses when the fenced cluster holds any definitions/instances.
// On a failure after the restore took ownership the returned report carries
// the operation id and the phase that failed.
func (node *ZenNode) ClusterRestore(ctx context.Context, r io.Reader, force bool) (*backup.RestoreReport, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create restore spool dir: %w", err)
	}
	defer node.removeSpoolDir(spoolDir)
	migDir := node.controller.Config.Persistence.Migration.Dir
	if migDir == "" {
		migDir = sql.DefaultMigrationsDir
	}
	binSchema, err := backup.BinarySchemaVersion(migDir)
	if err != nil {
		return nil, err
	}
	deps := backup.RestoreDeps{
		Clients:             node.client,
		ClusterState:        node.store.ClusterState,
		ApplyRestoreChange:  node.applyRestoreChange,
		CoordinatorID:       node.store.NodeID(),
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
		Timeouts:            backup.RestoreTimeoutsFromConfig(node.controller.Config.Restore),
		Limits:              backup.RestoreLimitsFromConfig(node.controller.Config.Restore),
	}
	return backup.RunClusterRestore(ctx, deps, r, force)
}

// removeSpoolDir deletes a backup/restore spool directory. A leftover spool
// must not fail an operation that already completed, so it only warns.
func (node *ZenNode) removeSpoolDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		node.logger.Warn("failed to remove spool dir", "dir", dir, "err", err)
	}
}

func (node *ZenNode) applyRestoreChange(ctx context.Context, change *protoc.RestoreOperationChange) (state.RestoreOperation, error) {
	return node.store.WriteRestoreChange(ctx, change)
}

// RestoreIngestTimeout is how long receiving and validating an uploaded
// bundle may take; the REST layer applies it as the request read deadline.
func (node *ZenNode) RestoreIngestTimeout() time.Duration {
	return backup.RestoreTimeoutsFromConfig(node.controller.Config.Restore).WithDefaults().Ingest
}

// RestoreOperation returns the current (or most recent) restore operation
// recorded in the cluster state, and whether one exists.
func (node *ZenNode) RestoreOperation() (state.RestoreOperation, bool) {
	op := node.store.ClusterState().Restore
	return op, op.Exists()
}

// AbortClusterRestore terminates the restore operation with the given id
// regardless of its owner and lifts the cluster gate. It is the operator's
// escape hatch after a coordinator crash or a restore that cannot be retried;
// partially restored data is left as it is. Must run on the cluster raft leader.
// The transition is bounded by the configured state-apply timeout like every
// other restore transition: the HTTP server imposes no request deadline, and
// an abort that hangs on a stalled raft apply would defeat its purpose.
func (node *ZenNode) AbortClusterRestore(ctx context.Context, operationID string, reason string) (state.RestoreOperation, error) {
	if reason == "" {
		reason = "aborted by operator"
	}
	applyCtx, cancel := context.WithTimeout(ctx, backup.RestoreTimeoutsFromConfig(node.controller.Config.Restore).WithDefaults().StateApply)
	defer cancel()
	return node.applyRestoreChange(applyCtx, &protoc.RestoreOperationChange{
		Action:          protoc.RestoreOperationChange_RESTORE_ACTION_ABORT.Enum(),
		OperationId:     new(operationID),
		Error:           new(reason),
		TimestampMillis: new(time.Now().UnixMilli()),
	})
}
