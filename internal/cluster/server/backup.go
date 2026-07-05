package server

import (
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PartitionRestore loads a partition database image shipped by the restore
// coordinator, then re-runs schema migrations (the image may be older than
// this binary).
func (s *Server) PartitionRestore(stream grpc.ClientStreamingServer[proto.RestoreChunk, proto.PartitionRestoreResponse]) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read restore header: %s", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "first restore chunk must carry meta")
	}
	partitionNode := s.controller.GetPartition(ctx, meta.GetPartitionId())
	if partitionNode == nil {
		return status.Errorf(codes.NotFound, "partition %d is not hosted on this node", meta.GetPartitionId())
	}
	if !s.store.ClusterState().Restoring {
		return status.Errorf(codes.FailedPrecondition, "cluster is not in restoring state")
	}
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)

	if err := backup.ReceivePartitionRestore(ctx, spoolDir, meta, stream.Recv, partitionNode.DB.Store); err != nil {
		return status.Errorf(codes.Internal, "partition restore failed: %s", err)
	}
	if err := partitionNode.DB.RunMigrations(ctx); err != nil {
		return status.Errorf(codes.Internal, "post-restore migrations failed: %s", err)
	}
	return stream.SendAndClose(&proto.PartitionRestoreResponse{})
}

// clusterBackupChunkWriter adapts the ClusterBackup stream into an io.Writer
// carrying raw tar bytes.
type clusterBackupChunkWriter struct {
	send func(*proto.BackupChunk) error
}

func (w *clusterBackupChunkWriter) Write(p []byte) (int, error) {
	data := append([]byte(nil), p...)
	if err := w.send(&proto.BackupChunk{Data: data}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ClusterBackup streams the whole-cluster backup bundle (tar) to a gRPC client.
func (s *Server) ClusterBackup(req *proto.ClusterBackupRequest, stream grpc.ServerStreamingServer[proto.BackupChunk]) error {
	spoolDir, err := os.MkdirTemp("", "zenbpm-backup-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)
	w := &clusterBackupChunkWriter{send: stream.Send}
	if _, err := backup.RunClusterBackup(stream.Context(), s.store.ClusterState(), s.client, spoolDir, w); err != nil {
		return status.Errorf(codes.Internal, "cluster backup failed: %s", err)
	}
	return stream.Send(&proto.BackupChunk{Eof: new(true)})
}

// PartitionBackup streams a point-in-time copy of a locally-led partition to
// the backup coordinator. The final chunk carries the source-side sha256.
func (s *Server) PartitionBackup(req *proto.PartitionBackupRequest, stream grpc.ServerStreamingServer[proto.BackupChunk]) error {
	ctx := stream.Context()
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	schemaVersion, err := partitionNode.DB.SchemaVersion(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read schema version: %s", err)
	}
	err = backup.StreamPartitionBackup(ctx, partitionNode.DB.Store, schemaVersion, stream.Send)
	if err != nil {
		return status.Errorf(codes.Internal, "partition backup failed: %s", err)
	}
	return nil
}
