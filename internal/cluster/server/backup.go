package server

import (
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/sql"
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

// ListActiveMessageSubscriptions returns every ACTIVE message subscription row
// on a locally-hosted partition. Called by the restore coordinator to rebuild
// pointer tables after a cluster restore.
func (s *Server) ListActiveMessageSubscriptions(ctx context.Context, req *proto.ListActiveMessageSubscriptionsRequest) (*proto.ListActiveMessageSubscriptionsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	rows, err := partitionNode.DB.ListActiveMessageSubscriptions(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.ListActiveMessageSubscriptionsResponse{Rows: rows}, nil
}

// RebuildMessageSubscriptionPointers wipes and re-inserts the pointer table for
// a locally-hosted partition. Only permitted while the cluster is in restoring state.
func (s *Server) RebuildMessageSubscriptionPointers(ctx context.Context, req *proto.RebuildMessageSubscriptionPointersRequest) (*proto.RebuildMessageSubscriptionPointersResponse, error) {
	if !s.store.ClusterState().Restoring {
		return nil, status.Errorf(codes.FailedPrecondition, "cluster is not in restoring state")
	}
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	if err := partitionNode.DB.RebuildMessageSubscriptionPointers(ctx, req.GetPointers()); err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.RebuildMessageSubscriptionPointersResponse{}, nil
}

// PartitionDataStats returns row counts for a locally-hosted partition.
func (s *Server) PartitionDataStats(ctx context.Context, req *proto.PartitionDataStatsRequest) (*proto.PartitionDataStatsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	defs, insts, err := partitionNode.DB.DataStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read data stats: %s", err)
	}
	return &proto.PartitionDataStatsResponse{ProcessDefinitions: new(defs), ProcessInstances: new(insts)}, nil
}

// ClusterRestore accepts a backup bundle over gRPC and drives the same
// coordinator as the REST endpoint.
func (s *Server) ClusterRestore(stream grpc.ClientStreamingServer[proto.RestoreChunk, proto.ClusterRestoreResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read restore header: %s", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "first restore chunk must carry meta")
	}

	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)
	binSchema, err := backup.BinarySchemaVersion(sql.DefaultMigrationsDir)
	if err != nil {
		return status.Errorf(codes.Internal, "%s", err)
	}

	pr, pw := io.Pipe()
	go func() {
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				pw.Close()
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if d := chunk.GetData(); len(d) > 0 {
				if _, err := pw.Write(d); err != nil {
					pw.CloseWithError(err)
					return
				}
			}
			if chunk.GetEof() {
				pw.Close()
				return
			}
		}
	}()
	deps := backup.RestoreDeps{
		Clients:      s.client,
		ClusterState: s.store.ClusterState,
		SetRestoring: func(restoring bool) error {
			return s.store.WriteMaintenanceChange(&protoc.ClusterMaintenanceChange{Restoring: new(restoring)})
		},
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
	}
	report, err := backup.RunClusterRestore(stream.Context(), deps, pr, meta.GetForce())
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "cluster restore failed: %s", err)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode restore report: %s", err)
	}
	return stream.SendAndClose(&proto.ClusterRestoreResponse{ReportJson: reportJSON})
}
