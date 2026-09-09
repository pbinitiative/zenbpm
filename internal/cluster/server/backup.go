package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoreOwner verifies that the restore token identifies the operation that
// currently owns the cluster, as seen by this node's replicated state. A stale
// coordinator (superseded epoch, aborted or finished operation) is refused
// with FailedPrecondition before anything destructive happens.
func (s *Server) restoreOwner(operationID string, epoch uint64) (partition.RestoreToken, error) {
	current := s.store.ClusterState().Restore
	if !current.Owns(operationID, epoch) {
		return partition.RestoreToken{}, status.Errorf(codes.FailedPrecondition,
			"restore operation %s (epoch %d) does not own the cluster: current operation %q epoch %d status %q",
			operationID, epoch, current.ID, current.Epoch, current.Status)
	}
	return partition.RestoreToken{OperationID: operationID, Epoch: epoch}, nil
}

// fencedLeader returns the locally hosted partition when this node leads it
// and its database is fenced for token.
func (s *Server) fencedLeader(ctx context.Context, partitionID uint32, token partition.RestoreToken) (*partition.ZenPartitionNode, error) {
	partitionNode := s.controller.GetPartition(ctx, partitionID)
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", partitionID)
	}
	if !partitionNode.IsLeader(ctx) {
		return nil, status.Errorf(codes.FailedPrecondition, "this node is not the leader of partition %d", partitionID)
	}
	fence, fenced := partitionNode.DB.RestoreFence()
	if !fenced || fence != token {
		return nil, status.Errorf(codes.FailedPrecondition, "partition %d has not entered maintenance mode for restore %s", partitionID, token)
	}
	return partitionNode, nil
}

// PartitionRestore loads a partition database image shipped by the restore
// coordinator, then re-runs schema migrations (the image may be older than
// this binary). The stream must carry the token of the restore operation that
// owns the cluster and the partition must already be fenced for it.
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
	token, err := s.restoreOwner(meta.GetRestoreOperationId(), meta.GetRestoreEpoch())
	if err != nil {
		return err
	}
	partitionNode, err := s.fencedLeader(ctx, meta.GetPartitionId(), token)
	if err != nil {
		return err
	}
	ctx = partition.WithRestoreToken(ctx, token)
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer removeSpoolDir(spoolDir)

	load := func(lr *rqcmd.LoadRequest) error {
		// The load runs under the partition fence: a takeover, abort or fence
		// release on this partition waits for it, and a fence that changed since
		// admission refuses it. Ownership is re-verified right before the
		// destructive step as well.
		return partitionNode.DB.LoadUnderFence(token, func() error {
			if !s.store.ClusterState().Restore.Owns(token.OperationID, token.Epoch) {
				return fmt.Errorf("restore operation %s no longer owns the cluster", token)
			}
			return partitionNode.DB.Store.Load(ctx, lr)
		})
	}
	if err := backup.ReceivePartitionRestore(ctx, spoolDir, meta, stream.Recv, s.restoreLimits, load); err != nil {
		if errors.Is(err, partition.ErrPartitionFenced) {
			return status.Errorf(codes.FailedPrecondition, "partition restore refused: %s", err)
		}
		if errors.Is(err, zenerr.ErrResourceLimit) {
			return status.Errorf(codes.ResourceExhausted, "partition restore refused: %s", err)
		}
		return status.Errorf(codes.Internal, "partition restore failed: %s", err)
	}
	if err := partitionNode.DB.RunMigrations(ctx); err != nil {
		return status.Errorf(codes.Internal, "post-restore migrations failed: %s", err)
	}
	return stream.SendAndClose(&proto.PartitionRestoreResponse{})
}

// PartitionRestoreStatus reports how far a locally hosted partition has
// entered (or left) maintenance mode for a restore operation.
func (s *Server) PartitionRestoreStatus(ctx context.Context, req *proto.PartitionRestoreStatusRequest) (*proto.PartitionRestoreStatusResponse, error) {
	token := partition.RestoreToken{OperationID: req.GetRestoreOperationId(), Epoch: req.GetRestoreEpoch()}
	st := s.controller.PartitionMaintenanceStatus(ctx, req.GetPartitionId(), token)
	applied := s.store.ClusterState().Restore.Owns(token.OperationID, token.Epoch)
	return &proto.PartitionRestoreStatusResponse{
		Hosted:         new(st.Hosted),
		Leader:         new(st.Leader),
		RestoreApplied: new(applied),
		EngineStopped:  new(st.Quiesced),
		WriteFenced:    new(st.Fenced),
		EngineRunning:  new(st.EngineRunning),
		Initialized:    new(st.Initialized),
	}, nil
}

// ImportDefinition writes a BPMN or DMN definition into a fenced partition
// during restore reconciliation. No engine is involved, so importing can never
// resume process execution while the cluster is in restore.
func (s *Server) ImportDefinition(ctx context.Context, req *proto.ImportDefinitionRequest) (*proto.ImportDefinitionResponse, error) {
	token, err := s.restoreOwner(req.GetRestoreOperationId(), req.GetRestoreEpoch())
	if err != nil {
		return nil, err
	}
	partitionNode, err := s.fencedLeader(ctx, req.GetPartitionId(), token)
	if err != nil {
		return nil, err
	}
	ctx = partition.WithRestoreToken(ctx, token)
	importer := partitionNode.NewDefinitionImporter()
	defer importer.Close()
	switch req.GetType() {
	case proto.DefinitionType_DEFINITION_TYPE_PROCESS:
		err = importer.ImportProcessDefinition(ctx, req.GetKey(), req.GetVersion(), req.GetData(), req.GetRegisterProcessDefinitionSubscriptions())
	case proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE:
		decisionVersions := make(map[string]int32, len(req.GetDecisions()))
		for _, d := range req.GetDecisions() {
			decisionVersions[d.GetDecisionId()] = d.GetVersion()
		}
		err = importer.ImportDmnResourceDefinition(ctx, req.GetKey(), req.GetVersion(), req.GetData(), decisionVersions)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown definition type %s", req.GetType())
	}
	if errors.Is(err, storage.ErrUniqueConstraint) {
		// the partition already holds another definition at that version: the
		// coordinator reports the diverged history instead of failing the restore
		return nil, status.Errorf(codes.AlreadyExists, "%s", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.ImportDefinitionResponse{}, nil
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
	defer removeSpoolDir(spoolDir)
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
// a locally-hosted partition. Only the restore operation that owns the cluster
// may do that, and only on a partition already fenced for it.
func (s *Server) RebuildMessageSubscriptionPointers(ctx context.Context, req *proto.RebuildMessageSubscriptionPointersRequest) (*proto.RebuildMessageSubscriptionPointersResponse, error) {
	token, err := s.restoreOwner(req.GetRestoreOperationId(), req.GetRestoreEpoch())
	if err != nil {
		return nil, err
	}
	partitionNode, err := s.fencedLeader(ctx, req.GetPartitionId(), token)
	if err != nil {
		return nil, err
	}
	if err := partitionNode.DB.RebuildMessageSubscriptionPointers(partition.WithRestoreToken(ctx, token), req.GetPointers()); err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.RebuildMessageSubscriptionPointersResponse{}, nil
}

// ListDefinitions returns the definition refs (process + DMN) for a locally-hosted partition.
// Called by the restore coordinator to compute definition sync requirements.
func (s *Server) ListDefinitions(ctx context.Context, req *proto.ListDefinitionsRequest) (*proto.ListDefinitionsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	refs, err := partitionNode.DB.ListDefinitionRefs(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.ListDefinitionsResponse{Definitions: refs}, nil
}

// GetDefinitionResource returns the raw resource bytes for a single definition on a locally-hosted partition.
// Called by the restore coordinator to import definitions that are missing on other partitions.
func (s *Server) GetDefinitionResource(ctx context.Context, req *proto.GetDefinitionResourceRequest) (*proto.GetDefinitionResourceResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	resource, err := partitionNode.DB.GetDefinitionResource(ctx, req.GetKey(), req.GetType())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.GetDefinitionResourceResponse{
		Data:         resource.Data,
		ResourceName: new(resource.ResourceName),
		Version:      new(resource.Version),
		Decisions:    resource.Decisions,
	}, nil
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

// errRestoreStreamClosed is the reason the request body is closed once the
// coordinator stops reading it (finished, or rejected the restore early).
var errRestoreStreamClosed = errors.New("cluster restore stream closed by the coordinator")

// restoreStreamReader turns the chunks of a ClusterRestore request into an
// io.Reader for the restore coordinator. A pump goroutine copies chunks into a
// pipe; Close releases the pump when the coordinator stops reading before the
// client stops sending. The pump ends on every path: a closed pipe fails its
// next write, and the stream context (cancelled when the handler returns)
// fails its next Recv. Done is closed once the pump has exited.
type restoreStreamReader struct {
	pr   *io.PipeReader
	done chan struct{}
}

func newRestoreStreamReader(ctx context.Context, recv func() (*proto.RestoreChunk, error)) *restoreStreamReader {
	pr, pw := io.Pipe()
	r := &restoreStreamReader{pr: pr, done: make(chan struct{})}
	safego.Go("cluster-restore-stream-pump", safego.DefaultLogger, func() {
		defer close(r.done)
		// Close and CloseWithError on an io.PipeWriter always return nil.
		_ = pw.CloseWithError(pumpRestoreStream(ctx, recv, pw))
	})
	return r
}

// pumpRestoreStream copies data chunks into w until the stream ends (EOF or an
// eof chunk), the context is done, or w rejects a write. It returns nil on a
// clean end of stream so the reader sees a plain EOF.
func pumpRestoreStream(ctx context.Context, recv func() (*proto.RestoreChunk, error), w io.Writer) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if d := chunk.GetData(); len(d) > 0 {
			if _, err := w.Write(d); err != nil {
				return err
			}
		}
		if chunk.GetEof() {
			return nil
		}
	}
}

func (r *restoreStreamReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

// Close stops feeding the coordinator; a pump blocked on a write returns.
func (r *restoreStreamReader) Close() error {
	return r.pr.CloseWithError(errRestoreStreamClosed)
}

// Done is closed when the pump goroutine has terminated.
func (r *restoreStreamReader) Done() <-chan struct{} {
	return r.done
}

// ClusterRestore accepts a backup bundle over gRPC and drives the same
// coordinator as the REST endpoint.
func (s *Server) ClusterRestore(stream grpc.ClientStreamingServer[proto.RestoreChunk, proto.ClusterRestoreResponse]) error {
	ctx := stream.Context()
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
	defer removeSpoolDir(spoolDir)
	binSchema, err := backup.BinarySchemaVersion(sql.DefaultMigrationsDir)
	if err != nil {
		return status.Errorf(codes.Internal, "%s", err)
	}

	body := newRestoreStreamReader(ctx, stream.Recv)
	defer func() {
		if err := body.Close(); err != nil {
			log.Warn("failed to close restore stream reader: %v", err)
		}
	}()
	deps := backup.RestoreDeps{
		Clients:             s.client,
		ClusterState:        s.store.ClusterState,
		ApplyRestoreChange:  s.applyRestoreChange,
		CoordinatorID:       s.store.NodeID(),
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
		Timeouts:            s.restoreTimeouts,
		Limits:              s.restoreLimits,
	}
	report, err := backup.RunClusterRestore(ctx, deps, body, meta.GetForce())
	if err != nil {
		return status.Errorf(restoreErrorCode(err), "cluster restore failed: %s", err)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode restore report: %s", err)
	}
	return stream.SendAndClose(&proto.ClusterRestoreResponse{ReportJson: reportJSON})
}

// removeSpoolDir deletes a backup/restore spool directory. A leftover spool
// must not fail an operation that already completed, so it only warns.
func removeSpoolDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Warn("failed to remove spool dir %s: %v", dir, err)
	}
}

// applyRestoreChange commits a restore transition through the cluster raft.
func (s *Server) applyRestoreChange(ctx context.Context, change *protoc.RestoreOperationChange) (state.RestoreOperation, error) {
	return s.store.WriteRestoreChange(ctx, change)
}

// restoreErrorCode maps a coordinator error onto a gRPC status code: refused
// before ownership (in progress, bad bundle, non-empty cluster) is a failed
// precondition, a phase failure is internal, a cancelled request is Canceled.
// An upload that outlived the ingest deadline is DeadlineExceeded, matching
// the REST mapping, so clients can tell it from a corrupt bundle.
func restoreErrorCode(err error) codes.Code {
	switch {
	case errors.Is(err, zenerr.ErrResourceLimit):
		return codes.ResourceExhausted
	case errors.Is(err, backup.ErrInvalidBundle) && backup.IsDeadlineExceeded(err):
		return codes.DeadlineExceeded
	case errors.Is(err, zenerr.ErrNotLeader), errors.Is(err, backup.ErrRestoreInProgress), errors.Is(err, backup.ErrInvalidBundle), errors.Is(err, backup.ErrClusterNotEmpty):
		return codes.FailedPrecondition
	case backup.IsCanceled(err):
		return codes.Canceled
	case backup.IsDeadlineExceeded(err):
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}
