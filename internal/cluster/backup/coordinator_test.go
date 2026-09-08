package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testSchemaVersion = "0007_x.up.sql"

func TestRunClusterRestoreRefusesWhenAnotherCoordinatorOwnsTheRestore(t *testing.T) {
	fc := newFakeCluster(t, 1)
	fc.cs.Restore = state.RestoreOperation{
		ID: "other", Epoch: 3, CoordinatorID: "node-b", Status: state.RestoreStatusActive, Phase: state.RestorePhaseLoading,
		LeaseExpiresAtMillis: time.Now().Add(time.Minute).UnixMilli(),
	}
	// garbage reader: if the guard fires first, the bundle is never opened
	_, err := RunClusterRestore(context.Background(), fc.deps(), strings.NewReader("not a tar"), true)
	require.ErrorIs(t, err, ErrRestoreInProgress)
	assert.NotContains(t, err.Error(), "bundle", "refused before bundle validation")
	assert.Empty(t, fc.actions(), "no state transition attempted")
}

func TestRunClusterRestoreAcquisitionIsAtomic(t *testing.T) {
	fc := newFakeCluster(t, 1)
	// the state looked free when the coordinator read it, but another
	// coordinator acquired in between: the FSM refuses the acquisition
	fc.applyHook = func(change *protoc.RestoreOperationChange) {
		if change.GetAction() == protoc.RestoreOperationChange_RESTORE_ACTION_ACQUIRE && change.GetCoordinatorId() == "node-a" {
			fc.applyHook = nil
			require.NoError(t, fc.cs.ApplyRestoreChange(state.RestoreChange{
				Action: state.RestoreActionAcquire, OperationID: "racer", CoordinatorID: "node-b",
				NowMillis: time.Now().UnixMilli(), LeaseMillis: 60_000,
			}))
		}
	}
	_, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(fc.bundle(t)), true)
	require.ErrorIs(t, err, ErrRestoreInProgress)
	assert.Equal(t, "racer", fc.ClusterState().Restore.ID)
	assert.Empty(t, fc.partitions[1].loaded)
}

func TestRunClusterRestoreSucceedsAndReturnsPromptly(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[1].subs = []*proto.MessageSubscriptionRow{{Key: new(int64(11)), Name: new("m"), CorrelationKey: new("ck"), CreatedAt: new(int64(5)), State: new(int64(1))}}

	start := time.Now()
	report, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(fc.bundle(t)), true)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "a restore whose partitions answer immediately must not wait for any deadline")

	assert.Equal(t, "op-1", report.OperationID)
	assert.Equal(t, uint64(1), report.Epoch)
	assert.Equal(t, "node-a", report.CoordinatorID)
	assert.Equal(t, state.RestorePhaseDone, report.Phase)
	assert.Len(t, report.Partitions, 2)
	assert.NotZero(t, report.FinishedAtMillis)
	assert.Equal(t, 1, report.PointersRebuilt)

	final := fc.ClusterState().Restore
	assert.Equal(t, state.RestoreStatusCompleted, final.Status)
	assert.Equal(t, state.RestorePhaseDone, final.Phase)
	assert.Equal(t, uint32(2), final.CompletedPartitions)
	assert.False(t, fc.ClusterState().RestoreInProgress())

	// every partition was loaded exactly once, with the owner's token, and
	// only after it reported maintenance mode
	for id, p := range fc.partitions {
		require.Len(t, p.loaded, 1, "partition %d", id)
		assert.Equal(t, fc.payloads[id], p.loaded[0].data)
		assert.Equal(t, "op-1", p.loaded[0].meta.GetRestoreOperationId())
		assert.Equal(t, uint64(1), p.loaded[0].meta.GetRestoreEpoch())
		assert.True(t, p.loaded[0].quiescedAtLoad, "partition %d was loaded before it quiesced", id)
		assert.Equal(t, "op-1", p.rebuiltWith.GetRestoreOperationId())
	}

	// the operation moved through every phase in order
	assert.Equal(t, []string{
		"ACQUIRE", "UPDATE:QUIESCING:0", "UPDATE:VALIDATING:0", "UPDATE:LOADING:0", "UPDATE:LOADING:1", "UPDATE:LOADING:2",
		"UPDATE:RECONCILING:2", "UPDATE:RESUMING:2", "COMPLETE",
	}, fc.actionsWithoutHeartbeats())
}

func TestRunClusterRestoreEmptyCheckRunsAfterFencing(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[2].definitions = 1

	report, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(fc.bundle(t)), false)
	require.ErrorIs(t, err, ErrClusterNotEmpty)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseValidating, phaseErr.Phase)
	assert.Equal(t, "op-1", phaseErr.OperationID)
	require.NotNil(t, report)
	assert.Equal(t, "op-1", report.OperationID)

	for id, p := range fc.partitions {
		assert.True(t, p.statsCheckedAfterQuiesce, "partition %d: the empty-cluster check ran before writes were fenced", id)
		assert.Empty(t, p.loaded, "partition %d: nothing may be loaded into a non-empty cluster", id)
	}
	final := fc.ClusterState().Restore
	assert.Equal(t, state.RestoreStatusFailed, final.Status)
	assert.Contains(t, final.Error, "cluster contains data")
	assert.False(t, final.DataModified)
	assert.False(t, fc.ClusterState().RestoreInProgress(), "a refusal before loading must not leave the cluster gated")
}

func TestRunClusterRestoreEmptyClusterNeedsNoForce(t *testing.T) {
	fc := newFakeCluster(t, 2)
	_, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(fc.bundle(t)), false)
	require.NoError(t, err)
	assert.Equal(t, state.RestoreStatusCompleted, fc.ClusterState().Restore.Status)
}

func TestRunClusterRestoreBarrierTimesOut(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[2].neverQuiesce = true
	deps := fc.deps()
	deps.Timeouts.Barrier = 200 * time.Millisecond

	start := time.Now()
	_, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseQuiescing, phaseErr.Phase)
	assert.Contains(t, err.Error(), "partition 2")
	assert.Less(t, time.Since(start), 5*time.Second)
	for _, p := range fc.partitions {
		assert.Empty(t, p.loaded, "nothing may be loaded while a partition leader is not quiesced")
	}
	assert.Equal(t, state.RestoreStatusFailed, fc.ClusterState().Restore.Status)
	assert.False(t, fc.ClusterState().RestoreInProgress())
}

func TestRunClusterRestoreStalledPartitionStreamTimesOut(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[1].closeAndRecv = func(ctx context.Context) error {
		<-ctx.Done() // the leader never answers
		return ctx.Err()
	}
	deps := fc.deps()
	deps.Timeouts.PartitionLoad = 200 * time.Millisecond

	start := time.Now()
	report, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseLoading, phaseErr.Phase)
	assert.Contains(t, err.Error(), "did not finish loading within")
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, state.RestorePhaseLoading, report.Phase)

	final := fc.ClusterState().Restore
	assert.Equal(t, state.RestoreStatusFailed, final.Status)
	assert.True(t, final.DataModified)
	assert.True(t, fc.ClusterState().RestoreInProgress(), "a failure while loading keeps the cluster gated")
	assert.Empty(t, fc.partitions[2].loaded, "loads are sequential: partition 2 is never touched")
}

func TestRunClusterRestoreStalledReconcileCallTimesOut(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[2].listDefinitions = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	deps := fc.deps()
	deps.Timeouts.Reconcile = 200 * time.Millisecond

	start := time.Now()
	_, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseReconciling, phaseErr.Phase)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Len(t, fc.partitions[1].loaded, 1)
	assert.Len(t, fc.partitions[2].loaded, 1)
	assert.True(t, fc.ClusterState().RestoreInProgress())
}

func TestRunClusterRestoreStalledReadinessWaitTimesOut(t *testing.T) {
	fc := newFakeCluster(t, 2)
	fc.partitions[1].neverResume = true
	deps := fc.deps()
	deps.Timeouts.Readiness = 200 * time.Millisecond

	start := time.Now()
	_, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseResuming, phaseErr.Phase)
	assert.Contains(t, err.Error(), "partition 1")
	assert.Less(t, time.Since(start), 5*time.Second)

	final := fc.ClusterState().Restore
	assert.Equal(t, state.RestoreStatusFailed, final.Status)
	assert.Equal(t, state.RestorePhaseResuming, final.Phase)
	assert.False(t, fc.ClusterState().RestoreInProgress(), "the data is fully loaded: a readiness timeout must not re-gate the cluster")
}

func TestRunClusterRestoreReconcilesDefinitionSkew(t *testing.T) {
	fc := newFakeCluster(t, 2)
	bpmn := []byte(`<?xml version="1.0"?><bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"><bpmn:process id="skewed-process" isExecutable="true"/></bpmn:definitions>`)
	dmn := []byte(`<?xml version="1.0"?><definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="skewed-dmn" name="d"/>`)
	// partition 1 snapshot was taken after the deploys, partition 2's before
	fc.partitions[1].definitionRefs = []*proto.DefinitionRef{
		{Key: new(int64(100)), Type: proto.DefinitionType_DEFINITION_TYPE_PROCESS.Enum()},
		{Key: new(int64(200)), Type: proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE.Enum()},
	}
	fc.partitions[1].resources = map[int64][]byte{100: bpmn, 200: dmn}

	report, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(fc.bundle(t)), true)
	require.NoError(t, err)

	require.Len(t, fc.partitions[2].imported, 2)
	assert.Empty(t, fc.partitions[1].imported)
	byKey := map[int64]*proto.ImportDefinitionRequest{}
	for _, req := range fc.partitions[2].imported {
		byKey[req.GetKey()] = req
		assert.Equal(t, "op-1", req.GetRestoreOperationId())
		assert.Equal(t, uint64(1), req.GetRestoreEpoch())
		assert.True(t, fc.partitions[2].importedWhileFenced, "definitions must be imported while the partition is fenced")
	}
	assert.Equal(t, bpmn, byKey[100].GetData())
	assert.Equal(t, proto.DefinitionType_DEFINITION_TYPE_PROCESS, byKey[100].GetType())
	owner := fc.ClusterState().DefinitionSubscriptionPartition("skewed-process")
	assert.Equal(t, owner == 2, byKey[100].GetRegisterProcessDefinitionSubscriptions(),
		"subscriptions are registered only on the partition that owns them under the deployment rule")
	assert.Equal(t, dmn, byKey[200].GetData())
	assert.Equal(t, proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE, byKey[200].GetType())
	assert.False(t, byKey[200].GetRegisterProcessDefinitionSubscriptions())

	assert.Equal(t, []DefinitionSyncEntry{
		{Key: 100, Type: "process", ToPartitions: []uint32{2}},
		{Key: 200, Type: "dmn", ToPartitions: []uint32{2}},
	}, report.DefinitionsSynced)
}

func TestRunClusterRestoreClientDisconnectCancelsAndRecordsFailure(t *testing.T) {
	fc := newFakeCluster(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	fc.partitions[1].closeAndRecv = func(loadCtx context.Context) error {
		cancel() // the client goes away mid-load
		<-loadCtx.Done()
		return loadCtx.Err()
	}
	report, err := RunClusterRestore(ctx, fc.deps(), bytes.NewReader(fc.bundle(t)), true)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseLoading, phaseErr.Phase)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, "op-1", report.OperationID)

	final := fc.ClusterState().Restore
	assert.Equal(t, state.RestoreStatusFailed, final.Status, "the terminal status is recorded even though the request context is gone")
	assert.Contains(t, final.Error, "LOADING")
	assert.Equal(t, "FAIL", fc.actions()[len(fc.actions())-1])
}

func TestRunClusterRestoreStopsWhenFencedOut(t *testing.T) {
	fc := newFakeCluster(t, 2)
	deps := fc.deps()
	deps.Timeouts.Lease = 90 * time.Millisecond // heartbeat every 30ms
	takenOver := make(chan struct{})
	fc.partitions[1].closeAndRecv = func(ctx context.Context) error {
		// while the load is in flight the operation is aborted by the operator
		fc.mu.Lock()
		require.NoError(t, fc.cs.ApplyRestoreChange(state.RestoreChange{Action: state.RestoreActionAbort, OperationID: "op-1", NowMillis: time.Now().UnixMilli()}))
		fc.mu.Unlock()
		close(takenOver)
		<-ctx.Done() // the run is cancelled by the heartbeat rejection
		return ctx.Err()
	}
	start := time.Now()
	_, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	<-takenOver
	require.ErrorIs(t, err, ErrRestoreOwnershipLost)
	var phaseErr *PhaseError
	require.ErrorAs(t, err, &phaseErr)
	assert.Equal(t, state.RestorePhaseLoading, phaseErr.Phase)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Empty(t, fc.partitions[2].loaded, "a fenced-out coordinator must not touch further partitions")
	assert.Equal(t, state.RestoreStatusAborted, fc.ClusterState().Restore.Status, "the stale owner cannot overwrite the abort")
}

func TestRunClusterRestoreRejectsBundleWithUnexpectedPartition(t *testing.T) {
	fc := newFakeCluster(t, 1)
	// a 2-partition bundle into a 1-partition cluster
	other := newFakeCluster(t, 2)
	_, err := RunClusterRestore(context.Background(), fc.deps(), bytes.NewReader(other.bundle(t)), true)
	require.ErrorIs(t, err, ErrInvalidBundle)
	assert.Empty(t, fc.actions(), "an invalid bundle is refused before ownership is taken")
}

// --- fakes ------------------------------------------------------------------

type loadedImage struct {
	meta           *proto.RestoreMeta
	data           []byte
	quiescedAtLoad bool
}

// fakePartition models one partition leader as the coordinator sees it.
type fakePartition struct {
	id uint32
	// state driven by the fake "controller"
	quiesced      bool
	engineRunning bool
	neverQuiesce  bool
	neverResume   bool

	definitions, instances   int64
	statsCheckedAfterQuiesce bool

	loaded []loadedImage

	definitionRefs      []*proto.DefinitionRef
	resources           map[int64][]byte
	imported            []*proto.ImportDefinitionRequest
	importedWhileFenced bool
	subs                []*proto.MessageSubscriptionRow
	rebuiltWith         *proto.RebuildMessageSubscriptionPointersRequest

	// stall hooks
	closeAndRecv    func(ctx context.Context) error
	listDefinitions func(ctx context.Context) error
}

// fakeCluster is an in-memory cluster: it applies restore transitions with the
// real state machine and mimics the controller reaction on every node
// (quiesce while gated, resume when the gate is lifted).
type fakeCluster struct {
	t          *testing.T
	mu         sync.Mutex
	cs         state.Cluster
	partitions map[uint32]*fakePartition
	payloads   map[uint32][]byte
	applied    []string
	nextID     int
	applyHook  func(change *protoc.RestoreOperationChange)
}

func newFakeCluster(t *testing.T, partitions int) *fakeCluster {
	fc := &fakeCluster{t: t, partitions: map[uint32]*fakePartition{}, payloads: map[uint32][]byte{}}
	fc.cs = state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}}
	for i := 1; i <= partitions; i++ {
		id := uint32(i)
		fc.cs.Partitions[id] = state.Partition{Id: id, LeaderId: "node-a"}
		fc.partitions[id] = &fakePartition{id: id, engineRunning: true, resources: map[int64][]byte{}}
		fc.payloads[id] = gzipBytes(t, sqliteish(t, fmt.Sprintf("partition-%d", id)))
	}
	return fc
}

func (fc *fakeCluster) bundle(t *testing.T) []byte {
	ids := make([]uint32, 0, len(fc.payloads))
	for id := range fc.payloads {
		ids = append(ids, id)
	}
	sortUint32(ids)
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), ids, testFetch(fc.payloads))
	require.NoError(t, err)
	return buf.Bytes()
}

func sortUint32(ids []uint32) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}

func (fc *fakeCluster) deps() RestoreDeps {
	return RestoreDeps{
		Clients:             fc,
		ClusterState:        fc.ClusterState,
		ApplyRestoreChange:  fc.ApplyRestoreChange,
		CoordinatorID:       "node-a",
		BinarySchemaVersion: testSchemaVersion,
		SpoolDir:            fc.t.TempDir(),
		Timeouts:            RestoreTimeouts{Barrier: 5 * time.Second, PartitionLoad: 5 * time.Second, Reconcile: 5 * time.Second, Readiness: 5 * time.Second, StateApply: time.Second, Lease: 5 * time.Second},
		NewOperationID: func() string {
			fc.mu.Lock()
			defer fc.mu.Unlock()
			fc.nextID++
			return fmt.Sprintf("op-%d", fc.nextID)
		},
		PollInterval: 5 * time.Millisecond,
	}
}

func (fc *fakeCluster) ClusterState() state.Cluster {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return *fc.cs.DeepCopy()
}

func (fc *fakeCluster) actions() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]string(nil), fc.applied...)
}

// actionsWithoutHeartbeats drops consecutive duplicate updates (lease renewals).
func (fc *fakeCluster) actionsWithoutHeartbeats() []string {
	var out []string
	for _, a := range fc.actions() {
		if len(out) > 0 && out[len(out)-1] == a {
			continue
		}
		out = append(out, a)
	}
	return out
}

func (fc *fakeCluster) ApplyRestoreChange(ctx context.Context, change *protoc.RestoreOperationChange) (state.RestoreOperation, error) {
	if err := ctx.Err(); err != nil {
		return state.RestoreOperation{}, err
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.applyHook != nil {
		fc.applyHook(change)
	}
	newState := *fc.cs.DeepCopy()
	if err := newState.ApplyRestoreChange(restoreChangeFromProto(change)); err != nil {
		return fc.cs.Restore, err
	}
	fc.cs = newState
	action := strings.TrimPrefix(change.GetAction().String(), "RESTORE_ACTION_")
	if change.GetAction() == protoc.RestoreOperationChange_RESTORE_ACTION_UPDATE {
		action = fmt.Sprintf("%s:%s:%d", action, newState.Restore.Phase, change.GetCompletedPartitions())
	}
	fc.applied = append(fc.applied, action)
	fc.reactLocked()
	return newState.Restore, nil
}

// reactLocked mimics the controller on every node: gated → engines stopped and
// writes fenced; gate lifted → engines restarted.
func (fc *fakeCluster) reactLocked() {
	gated := fc.cs.RestoreInProgress()
	for _, p := range fc.partitions {
		if gated {
			if !p.neverQuiesce {
				p.quiesced = true
				p.engineRunning = false
			}
		} else {
			p.quiesced = false
			if !p.neverResume {
				p.engineRunning = true
			}
		}
	}
}

func restoreChangeFromProto(change *protoc.RestoreOperationChange) state.RestoreChange {
	actions := map[protoc.RestoreOperationChange_Action]state.RestoreAction{
		protoc.RestoreOperationChange_RESTORE_ACTION_ACQUIRE:  state.RestoreActionAcquire,
		protoc.RestoreOperationChange_RESTORE_ACTION_UPDATE:   state.RestoreActionUpdate,
		protoc.RestoreOperationChange_RESTORE_ACTION_COMPLETE: state.RestoreActionComplete,
		protoc.RestoreOperationChange_RESTORE_ACTION_FAIL:     state.RestoreActionFail,
		protoc.RestoreOperationChange_RESTORE_ACTION_ABORT:    state.RestoreActionAbort,
	}
	var phase state.RestorePhase
	for p, wire := range restorePhaseToProtoValue {
		if wire == change.GetPhase() {
			phase = p
		}
	}
	return state.RestoreChange{
		Action:              actions[change.GetAction()],
		OperationID:         change.GetOperationId(),
		Epoch:               change.GetEpoch(),
		CoordinatorID:       change.GetCoordinatorId(),
		Phase:               phase,
		TotalPartitions:     change.GetTotalPartitions(),
		CompletedPartitions: change.GetCompletedPartitions(),
		Error:               change.GetError(),
		Force:               change.GetForce(),
		NowMillis:           change.GetTimestampMillis(),
		LeaseMillis:         change.GetLeaseMillis(),
	}
}

func (fc *fakeCluster) PartitionLeader(partition uint32) (proto.ZenServiceClient, error) {
	p, ok := fc.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("no leader for partition %d", partition)
	}
	return &fakeClient{fc: fc, p: p}, nil
}

// fakeClient answers the partition RPCs the coordinator uses, applying the
// same ownership checks as the real server.
type fakeClient struct {
	proto.ZenServiceClient
	fc *fakeCluster
	p  *fakePartition
}

func (c *fakeClient) owns(id string, epoch uint64) error {
	if !c.fc.ClusterState().Restore.Owns(id, epoch) {
		return errors.New("stale restore owner")
	}
	return nil
}

func (c *fakeClient) PartitionRestoreStatus(ctx context.Context, req *proto.PartitionRestoreStatusRequest, _ ...grpc.CallOption) (*proto.PartitionRestoreStatusResponse, error) {
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	applied := c.fc.cs.Restore.Owns(req.GetRestoreOperationId(), req.GetRestoreEpoch())
	return &proto.PartitionRestoreStatusResponse{
		Hosted:         new(true),
		Leader:         new(true),
		RestoreApplied: new(applied),
		EngineStopped:  new(!c.p.engineRunning),
		WriteFenced:    new(c.p.quiesced && applied),
		EngineRunning:  new(c.p.engineRunning),
		Initialized:    new(true),
	}, nil
}

func (c *fakeClient) PartitionDataStats(ctx context.Context, req *proto.PartitionDataStatsRequest, _ ...grpc.CallOption) (*proto.PartitionDataStatsResponse, error) {
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	c.p.statsCheckedAfterQuiesce = c.p.quiesced
	return &proto.PartitionDataStatsResponse{ProcessDefinitions: new(c.p.definitions), ProcessInstances: new(c.p.instances)}, nil
}

func (c *fakeClient) PartitionRestore(ctx context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[proto.RestoreChunk, proto.PartitionRestoreResponse], error) {
	return &fakeRestoreStream{ctx: ctx, c: c}, nil
}

type fakeRestoreStream struct {
	grpc.ClientStream
	ctx  context.Context
	c    *fakeClient
	meta *proto.RestoreMeta
	data []byte
}

func (s *fakeRestoreStream) Send(chunk *proto.RestoreChunk) error {
	if m := chunk.GetMeta(); m != nil {
		s.meta = m
	}
	s.data = append(s.data, chunk.GetData()...)
	return nil
}

func (s *fakeRestoreStream) CloseAndRecv() (*proto.PartitionRestoreResponse, error) {
	if s.c.p.closeAndRecv != nil {
		if err := s.c.p.closeAndRecv(s.ctx); err != nil {
			return nil, err
		}
	}
	if err := s.c.owns(s.meta.GetRestoreOperationId(), s.meta.GetRestoreEpoch()); err != nil {
		return nil, err
	}
	s.c.fc.mu.Lock()
	defer s.c.fc.mu.Unlock()
	s.c.p.loaded = append(s.c.p.loaded, loadedImage{meta: s.meta, data: s.data, quiescedAtLoad: s.c.p.quiesced})
	return &proto.PartitionRestoreResponse{}, nil
}

func (c *fakeClient) ListDefinitions(ctx context.Context, req *proto.ListDefinitionsRequest, _ ...grpc.CallOption) (*proto.ListDefinitionsResponse, error) {
	if c.p.listDefinitions != nil {
		if err := c.p.listDefinitions(ctx); err != nil {
			return nil, err
		}
	}
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	return &proto.ListDefinitionsResponse{Definitions: c.p.definitionRefs}, nil
}

func (c *fakeClient) GetDefinitionResource(ctx context.Context, req *proto.GetDefinitionResourceRequest, _ ...grpc.CallOption) (*proto.GetDefinitionResourceResponse, error) {
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	data, ok := c.p.resources[req.GetKey()]
	if !ok {
		return nil, fmt.Errorf("definition %d not on partition %d", req.GetKey(), c.p.id)
	}
	return &proto.GetDefinitionResourceResponse{Data: data, ResourceName: new("r")}, nil
}

func (c *fakeClient) ImportDefinition(ctx context.Context, req *proto.ImportDefinitionRequest, _ ...grpc.CallOption) (*proto.ImportDefinitionResponse, error) {
	if err := c.owns(req.GetRestoreOperationId(), req.GetRestoreEpoch()); err != nil {
		return nil, err
	}
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	c.p.imported = append(c.p.imported, req)
	c.p.importedWhileFenced = c.p.quiesced
	c.p.definitionRefs = append(c.p.definitionRefs, &proto.DefinitionRef{Key: new(req.GetKey()), Type: req.GetType().Enum()})
	return &proto.ImportDefinitionResponse{}, nil
}

func (c *fakeClient) ListActiveMessageSubscriptions(ctx context.Context, req *proto.ListActiveMessageSubscriptionsRequest, _ ...grpc.CallOption) (*proto.ListActiveMessageSubscriptionsResponse, error) {
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	return &proto.ListActiveMessageSubscriptionsResponse{Rows: c.p.subs}, nil
}

func (c *fakeClient) RebuildMessageSubscriptionPointers(ctx context.Context, req *proto.RebuildMessageSubscriptionPointersRequest, _ ...grpc.CallOption) (*proto.RebuildMessageSubscriptionPointersResponse, error) {
	if err := c.owns(req.GetRestoreOperationId(), req.GetRestoreEpoch()); err != nil {
		return nil, err
	}
	c.fc.mu.Lock()
	defer c.fc.mu.Unlock()
	c.p.rebuiltWith = req
	return &proto.RebuildMessageSubscriptionPointersResponse{}, nil
}

// TestRunClusterRestoreOverlapsHeartbeats runs restores whose partitions answer
// slowly enough for several lease renewals to happen mid-load. Under -race it
// proves that heartbeats (which rewrite the mutable operation record) never
// race the token reads of the load path.
func TestRunClusterRestoreOverlapsHeartbeats(t *testing.T) {
	for i := 0; i < 3; i++ {
		fc := newFakeCluster(t, 2)
		for _, p := range fc.partitions {
			p.closeAndRecv = func(ctx context.Context) error {
				select {
				case <-time.After(40 * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		deps := fc.deps()
		deps.Timeouts.Lease = 9 * time.Millisecond // heartbeat every 3ms
		report, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
		require.NoError(t, err)
		assert.Equal(t, "op-1", report.OperationID)
		assert.Greater(t, len(fc.actions()), 9, "expected lease renewals in between the phase updates")
		assert.Equal(t, state.RestoreStatusCompleted, fc.ClusterState().Restore.Status)
	}
}

func TestRunClusterRestoreBarrierTimeoutKeepsDeadlineIdentity(t *testing.T) {
	fc := newFakeCluster(t, 1)
	fc.partitions[1].neverQuiesce = true
	deps := fc.deps()
	deps.Timeouts.Barrier = 100 * time.Millisecond
	_, err := RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	require.Error(t, err)
	assert.True(t, IsDeadlineExceeded(err), "barrier timeout must be recognizable as a deadline: %v", err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	fc = newFakeCluster(t, 1)
	fc.partitions[1].closeAndRecv = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	deps = fc.deps()
	deps.Timeouts.PartitionLoad = 100 * time.Millisecond
	_, err = RunClusterRestore(context.Background(), deps, bytes.NewReader(fc.bundle(t)), true)
	require.Error(t, err)
	assert.True(t, IsDeadlineExceeded(err), "load timeout must be recognizable as a deadline: %v", err)
}

func TestIsDeadlineExceededRecognisesGrpcStatus(t *testing.T) {
	assert.True(t, IsDeadlineExceeded(status.Error(codes.DeadlineExceeded, "rpc")))
	assert.True(t, IsDeadlineExceeded(&PhaseError{Err: status.Error(codes.DeadlineExceeded, "rpc")}))
	assert.False(t, IsDeadlineExceeded(status.Error(codes.Internal, "rpc")))
	assert.True(t, IsCanceled(status.Error(codes.Canceled, "rpc")))
	assert.True(t, IsCanceled(context.Canceled))
	assert.False(t, IsCanceled(errors.New("boom")))
}

func TestRunClusterRestoreBoundsStalledUpload(t *testing.T) {
	fc := newFakeCluster(t, 1)
	deps := fc.deps()
	deps.Timeouts.Ingest = 100 * time.Millisecond
	stalled, w := io.Pipe()
	defer func() { require.NoError(t, w.Close()) }()
	start := time.Now()
	_, err := RunClusterRestore(context.Background(), deps, stalled, true)
	require.ErrorIs(t, err, ErrInvalidBundle)
	assert.True(t, IsDeadlineExceeded(err), "stalled upload must fail with the ingest deadline: %v", err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Empty(t, fc.actions(), "nothing is recorded for an upload that never completed")
}
