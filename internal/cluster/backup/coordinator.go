package backup

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

// ClientProvider is the subset of client.ClientManager the coordinator needs.
type ClientProvider interface {
	PartitionLeader(partition uint32) (proto.ZenServiceClient, error)
}

// RunClusterBackup checks that every partition has a leader, then streams a
// bundle of all partition backups into w.
func RunClusterBackup(ctx context.Context, cs state.Cluster, clients ClientProvider, spoolDir string, w io.Writer) (*Manifest, error) {
	if len(cs.Partitions) == 0 {
		return nil, fmt.Errorf("cluster has no partitions")
	}
	ids := make([]uint32, 0, len(cs.Partitions))
	for id, p := range cs.Partitions {
		if p.LeaderId == "" {
			return nil, fmt.Errorf("partition %d has no leader; refusing to start backup", id)
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return WriteBundle(ctx, w, spoolDir, ids, fetchFromLeader(clients))
}

func fetchFromLeader(clients ClientProvider) FetchFunc {
	return func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return FetchResult{}, fmt.Errorf("failed to get leader client for partition %d: %w", id, err)
		}
		stream, err := leader.PartitionBackup(ctx, &proto.PartitionBackupRequest{PartitionId: new(id)})
		if err != nil {
			return FetchResult{}, fmt.Errorf("failed to open backup stream for partition %d: %w", id, err)
		}
		for {
			chunk, err := stream.Recv()
			if err != nil {
				return FetchResult{}, fmt.Errorf("backup stream for partition %d failed: %w", id, err)
			}
			if chunk.GetEof() {
				return FetchResult{SHA256: chunk.GetSha256(), SchemaVersion: chunk.GetSchemaVersion()}, nil
			}
			if _, err := dst.Write(chunk.GetData()); err != nil {
				return FetchResult{}, fmt.Errorf("failed to spool backup of partition %d: %w", id, err)
			}
		}
	}
}

// RestoreDeps carries the coordinator's dependencies so both ZenNode (REST)
// and the gRPC server can drive a restore.
type RestoreDeps struct {
	Clients             ClientProvider
	ClusterState        func() state.Cluster
	SetRestoring        func(restoring bool) error
	BinarySchemaVersion string
	SpoolDir            string
}

// RunClusterRestore validates the bundle, gates the cluster, loads every
// partition sequentially, reconciles derived state, and un-gates.
// On failure after gating, the cluster is deliberately LEFT in Restoring
// state — the operator retries the restore (the operation is idempotent).
func RunClusterRestore(ctx context.Context, deps RestoreDeps, r io.Reader, force bool) (*RestoreReport, error) {
	report := &RestoreReport{StartedAtMillis: time.Now().UnixMilli()}
	cs := deps.ClusterState()

	bundle, err := OpenBundle(r, deps.SpoolDir)
	if err != nil {
		return nil, fmt.Errorf("invalid backup bundle: %w", err)
	}
	defer bundle.Close()
	if err := bundle.Manifest.Validate(uint32(len(cs.Partitions)), deps.BinarySchemaVersion); err != nil {
		return nil, fmt.Errorf("bundle cannot be restored into this cluster: %w", err)
	}

	if !force {
		empty, err := clusterIsEmpty(ctx, cs, deps.Clients)
		if err != nil {
			return nil, fmt.Errorf("failed to check whether cluster is empty: %w", err)
		}
		if !empty {
			return nil, fmt.Errorf("cluster contains data; pass force=true to overwrite it")
		}
	}

	if err := deps.SetRestoring(true); err != nil {
		return nil, fmt.Errorf("failed to enter restore mode: %w", err)
	}
	// Poll until Restoring is observed as true before any destructive load.
	if err := pollRestoring(ctx, deps.ClusterState, true, 5*time.Second, 100*time.Millisecond); err != nil {
		return nil, fmt.Errorf("cluster did not enter restoring state (possible raft not-leader error; retry on leader): %w", err)
	}

	ids := make([]uint32, 0, len(bundle.Manifest.Partitions))
	for id := range bundle.Manifest.Partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// sequential loads bound coordinator memory: each store.Load holds one
	// full partition image as a single raft entry
	for _, id := range ids {
		start := time.Now()
		if err := restoreOnePartition(ctx, deps, bundle, id); err != nil {
			return nil, fmt.Errorf("restore of partition %d failed (cluster left in restoring state; retry the restore): %w", id, err)
		}
		report.Partitions = append(report.Partitions, PartitionRestoreResult{
			PartitionID: id,
			LoadMillis:  time.Since(start).Milliseconds(),
		})
	}

	if err := reconcile(ctx, deps, report); err != nil {
		return nil, fmt.Errorf("post-restore reconciliation failed (cluster left in restoring state; retry the restore): %w", err)
	}

	if err := deps.SetRestoring(false); err != nil {
		return nil, fmt.Errorf("restore finished but failed to leave restore mode: %w", err)
	}
	// Poll until Restoring is observed as false.
	if err := pollRestoring(ctx, deps.ClusterState, false, 5*time.Second, 100*time.Millisecond); err != nil {
		return report, fmt.Errorf("restore completed but cluster is still gated in restoring state (retry un-gate manually): %w", err)
	}
	report.FinishedAtMillis = time.Now().UnixMilli()
	return report, nil
}

func restoreOnePartition(ctx context.Context, deps RestoreDeps, bundle *Bundle, id uint32) error {
	leader, err := deps.Clients.PartitionLeader(id)
	if err != nil {
		return fmt.Errorf("failed to get leader client: %w", err)
	}
	stream, err := leader.PartitionRestore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open restore stream: %w", err)
	}
	meta := bundle.Manifest.Partitions[id]
	err = stream.Send(&proto.RestoreChunk{Payload: &proto.RestoreChunk_Meta{Meta: &proto.RestoreMeta{
		PartitionId: new(id),
		Sha256:      new(meta.SHA256),
		SizeBytes:   new(meta.SizeBytes),
	}}})
	if err != nil {
		return fmt.Errorf("failed to send restore meta: %w", err)
	}
	f, err := bundle.PartitionFile(id)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, backupChunkSize)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			if err := stream.Send(&proto.RestoreChunk{Payload: &proto.RestoreChunk_Data{Data: data}}); err != nil {
				return fmt.Errorf("failed to send restore data: %w", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("partition leader rejected restore: %w", err)
	}
	return nil
}

func clusterIsEmpty(ctx context.Context, cs state.Cluster, clients ClientProvider) (bool, error) {
	for id := range cs.Partitions {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return false, err
		}
		resp, err := leader.PartitionDataStats(ctx, &proto.PartitionDataStatsRequest{PartitionId: new(id)})
		if err != nil {
			return false, err
		}
		if resp.GetProcessDefinitions() > 0 || resp.GetProcessInstances() > 0 {
			return false, nil
		}
	}
	return true, nil
}

// pollRestoring polls ClusterState until Restoring matches want, up to timeout.
func pollRestoring(ctx context.Context, clusterState func() state.Cluster, want bool, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if clusterState().Restoring == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Restoring=%v", want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// reconcile is completed in Tasks 12-13; at this stage it is a no-op shell.
func reconcile(ctx context.Context, deps RestoreDeps, report *RestoreReport) error {
	return nil
}
