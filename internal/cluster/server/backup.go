package server

import (
	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
