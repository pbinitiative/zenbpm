package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRestoreStreamReaderTerminatesWhenCoordinatorRejectsEarly covers the
// leak the coordinator can cause: it refuses the restore right away and never
// reads the body while the client keeps streaming data. Closing the reader
// must release the pump goroutine.
func TestRestoreStreamReaderTerminatesWhenCoordinatorRejectsEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := 0
	recv := func() (*proto.RestoreChunk, error) {
		// an endless client: every call yields another data chunk
		sent++
		return &proto.RestoreChunk{Payload: &proto.RestoreChunk_Data{Data: make([]byte, 1024)}}, nil
	}
	r := newRestoreStreamReader(ctx, recv)

	// the coordinator never reads: the pump blocks on the pipe
	select {
	case <-r.Done():
		t.Fatal("pump must block while nobody reads")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, r.Close())
	select {
	case <-r.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pump goroutine leaked after the coordinator closed the stream")
	}
	assert.Greater(t, sent, 0)

	// reads after Close fail immediately, never block
	_, err := r.Read(make([]byte, 8))
	assert.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestRestoreStreamReaderTerminatesWhenStreamContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	recv := func() (*proto.RestoreChunk, error) {
		// a client that stopped sending: Recv only returns once the stream context ends
		<-ctx.Done()
		return nil, ctx.Err()
	}
	r := newRestoreStreamReader(ctx, recv)
	defer func() { require.NoError(t, r.Close()) }()

	cancel() // the handler returned: gRPC cancels the stream context
	select {
	case <-r.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pump goroutine leaked after the stream context ended")
	}
	_, err := r.Read(make([]byte, 8))
	assert.ErrorIs(t, err, context.Canceled, "the coordinator sees why the body ended")
}

func TestRestoreStreamReaderDeliversDataUntilEOF(t *testing.T) {
	chunks := []*proto.RestoreChunk{
		{Payload: &proto.RestoreChunk_Data{Data: []byte("hello ")}},
		{Payload: &proto.RestoreChunk_Data{Data: []byte("world")}},
		{Eof: new(true)},
	}
	i := 0
	recv := func() (*proto.RestoreChunk, error) {
		if i >= len(chunks) {
			return nil, io.EOF
		}
		c := chunks[i]
		i++
		return c, nil
	}
	r := newRestoreStreamReader(context.Background(), recv)
	defer func() { require.NoError(t, r.Close()) }()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(got))
	select {
	case <-r.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not terminate after the eof chunk")
	}
}

func TestRestoreStreamReaderPropagatesRecvError(t *testing.T) {
	boom := errors.New("connection reset")
	recv := func() (*proto.RestoreChunk, error) { return nil, boom }
	r := newRestoreStreamReader(context.Background(), recv)
	defer func() { require.NoError(t, r.Close()) }()
	_, err := io.ReadAll(r)
	assert.ErrorIs(t, err, boom)
}

func TestRestoreErrorCode(t *testing.T) {
	assert.Equal(t, codes.FailedPrecondition, restoreErrorCode(backup.ErrRestoreInProgress))
	assert.Equal(t, codes.FailedPrecondition, restoreErrorCode(errors.Join(errors.New("apply"), zenerr.ErrNotLeader)))
	assert.Equal(t, codes.FailedPrecondition, restoreErrorCode(errors.Join(errors.New("x"), backup.ErrInvalidBundle)))
	assert.Equal(t, codes.FailedPrecondition, restoreErrorCode(&backup.PhaseError{Err: backup.ErrClusterNotEmpty}))
	assert.Equal(t, codes.Canceled, restoreErrorCode(&backup.PhaseError{Err: context.Canceled}))
	assert.Equal(t, codes.DeadlineExceeded, restoreErrorCode(context.DeadlineExceeded))
	assert.Equal(t, codes.Internal, restoreErrorCode(&backup.PhaseError{Err: errors.New("boom")}))
}

func TestPartitionRestoreRejectsStaleOwner(t *testing.T) {
	tStore := &testStore{clusterState: state.Cluster{
		Restore: state.RestoreOperation{ID: "op-1", Epoch: 2, Status: state.RestoreStatusActive, Phase: state.RestorePhaseLoading},
	}}
	srv := &Server{store: tStore, controller: &fakeController{}}

	for name, meta := range map[string]*proto.RestoreMeta{
		"superseded epoch":  {PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(1))},
		"unknown operation": {PartitionId: new(uint32(1)), RestoreOperationId: new("op-9"), RestoreEpoch: new(uint64(2))},
		"no token at all":   {PartitionId: new(uint32(1))},
	} {
		t.Run(name, func(t *testing.T) {
			stream := &fakeRestoreServerStream{ctx: context.Background(), chunks: []*proto.RestoreChunk{{Payload: &proto.RestoreChunk_Meta{Meta: meta}}}}
			err := srv.PartitionRestore(stream)
			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Nil(t, stream.response, "a stale owner must never get an acknowledgement")
		})
	}

	// the rebuild and import paths are fenced the same way
	_, err := srv.RebuildMessageSubscriptionPointers(context.Background(), &proto.RebuildMessageSubscriptionPointersRequest{
		PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(1)),
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = srv.ImportDefinition(context.Background(), &proto.ImportDefinitionRequest{
		PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(1)),
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestPartitionRestoreStatusReflectsControllerAndClusterState(t *testing.T) {
	tStore := &testStore{clusterState: state.Cluster{
		Restore: state.RestoreOperation{ID: "op-1", Epoch: 2, Status: state.RestoreStatusActive, Phase: state.RestorePhaseQuiescing},
	}}
	ctrl := &fakeController{status: partition.MaintenanceStatus{Hosted: true, Leader: true, Quiesced: true, Fenced: true, Initialized: true}}
	srv := &Server{store: tStore, controller: ctrl}

	resp, err := srv.PartitionRestoreStatus(context.Background(), &proto.PartitionRestoreStatusRequest{
		PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(2)),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetHosted())
	assert.True(t, resp.GetLeader())
	assert.True(t, resp.GetRestoreApplied())
	assert.True(t, resp.GetEngineStopped())
	assert.True(t, resp.GetWriteFenced())
	assert.False(t, resp.GetEngineRunning())
	assert.True(t, resp.GetInitialized())
	assert.Equal(t, partition.RestoreToken{OperationID: "op-1", Epoch: 2}, ctrl.askedFor)

	// a node that has not applied the acquisition yet reports it as not applied
	resp, err = srv.PartitionRestoreStatus(context.Background(), &proto.PartitionRestoreStatusRequest{
		PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(3)),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetRestoreApplied())

	// a partition whose engine is still shutting down is not acknowledged as stopped
	ctrl.status = partition.MaintenanceStatus{Hosted: true, Leader: true, Fenced: true, Initialized: true}
	resp, err = srv.PartitionRestoreStatus(context.Background(), &proto.PartitionRestoreStatusRequest{
		PartitionId: new(uint32(1)), RestoreOperationId: new("op-1"), RestoreEpoch: new(uint64(2)),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetEngineStopped(), "engine stop must be acknowledged only once Stop returned")

	// an unhosted partition is neither stopped nor running
	ctrl.status = partition.MaintenanceStatus{}
	resp, err = srv.PartitionRestoreStatus(context.Background(), &proto.PartitionRestoreStatusRequest{PartitionId: new(uint32(7))})
	require.NoError(t, err)
	assert.False(t, resp.GetHosted())
	assert.False(t, resp.GetEngineStopped())
}

type fakeController struct {
	status   partition.MaintenanceStatus
	askedFor partition.RestoreToken
}

func (c *fakeController) PartitionEngine(ctx context.Context, partitionId uint32) *bpmn.Engine {
	return nil
}
func (c *fakeController) Engines(ctx context.Context) map[uint32]*bpmn.Engine { return nil }
func (c *fakeController) PartitionQueries(ctx context.Context, partitionId uint32) *sql.Queries {
	return nil
}
func (c *fakeController) GetPartition(ctx context.Context, partitionId uint32) *partition.ZenPartitionNode {
	return nil
}
func (c *fakeController) PartitionMaintenanceStatus(ctx context.Context, partitionId uint32, token partition.RestoreToken) partition.MaintenanceStatus {
	c.askedFor = token
	return c.status
}

type fakeRestoreServerStream struct {
	grpc.ServerStream
	ctx      context.Context
	chunks   []*proto.RestoreChunk
	response *proto.PartitionRestoreResponse
}

func (s *fakeRestoreServerStream) Context() context.Context { return s.ctx }

func (s *fakeRestoreServerStream) Recv() (*proto.RestoreChunk, error) {
	if len(s.chunks) == 0 {
		return nil, io.EOF
	}
	c := s.chunks[0]
	s.chunks = s.chunks[1:]
	return c, nil
}

func (s *fakeRestoreServerStream) SendAndClose(resp *proto.PartitionRestoreResponse) error {
	s.response = resp
	return nil
}
