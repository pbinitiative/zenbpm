package backup

import (
	"context"
	"fmt"
	"io"
	"sort"

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
				return FetchResult{}, err
			}
		}
	}
}
