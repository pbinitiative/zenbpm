package backup

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// Errors a restore can fail with before it takes ownership of the cluster.
// Failures after that are reported as *PhaseError.
var (
	// ErrRestoreInProgress another coordinator owns an active restore.
	ErrRestoreInProgress = errors.New("a cluster restore is already in progress")
	// ErrInvalidBundle the bundle was rejected during validation.
	ErrInvalidBundle = errors.New("invalid backup bundle")
	// ErrClusterNotEmpty force=false and the fenced cluster still holds data.
	ErrClusterNotEmpty = errors.New("cluster contains data; pass force=true to overwrite it")
	// ErrRestoreOwnershipLost the coordinator was fenced out (its lease
	// expired and another coordinator took over, or the operation was aborted).
	ErrRestoreOwnershipLost = errors.New("restore ownership lost")
)

// IsDeadlineExceeded reports whether err stems from a phase deadline, whether
// it surfaced as a context error or as a gRPC DeadlineExceeded status.
func IsDeadlineExceeded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

// IsCanceled reports whether err stems from a cancelled request, whether it
// surfaced as a context error or as a gRPC Canceled status.
func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// PhaseError reports which restore phase failed. The operation is marked as
// FAILED in the cluster state with the same message.
type PhaseError struct {
	OperationID string
	Phase       state.RestorePhase
	Err         error
}

func (e *PhaseError) Error() string {
	return fmt.Sprintf("restore %s failed in phase %s: %v", e.OperationID, e.Phase, e.Err)
}

func (e *PhaseError) Unwrap() error {
	return e.Err
}

// RestoreTimeouts bounds every phase of a restore; a zero value takes the
// default. See config.Restore for the meaning of each field.
type RestoreTimeouts struct {
	Lease         time.Duration
	Barrier       time.Duration
	PartitionLoad time.Duration
	Reconcile     time.Duration
	Readiness     time.Duration
	StateApply    time.Duration
	// Ingest bounds receiving and validating the bundle before ownership is taken.
	Ingest time.Duration
}

// DefaultRestoreTimeouts returns the timeouts used when a field is zero.
func DefaultRestoreTimeouts() RestoreTimeouts {
	return RestoreTimeouts{
		Lease:         30 * time.Second,
		Barrier:       time.Minute,
		PartitionLoad: 30 * time.Minute,
		Reconcile:     10 * time.Minute,
		Readiness:     2 * time.Minute,
		StateApply:    10 * time.Second,
		Ingest:        time.Hour,
	}
}

// RestoreTimeoutsFromConfig maps the cluster configuration onto RestoreTimeouts.
func RestoreTimeoutsFromConfig(c config.Restore) RestoreTimeouts {
	return RestoreTimeouts{
		Lease:         c.LeaseDuration,
		Barrier:       c.BarrierTimeout,
		PartitionLoad: c.PartitionLoadTimeout,
		Reconcile:     c.ReconcileTimeout,
		Readiness:     c.ReadinessTimeout,
		StateApply:    c.StateApplyTimeout,
		Ingest:        c.IngestTimeout,
	}
}

// WithDefaults returns the timeouts with every zero field replaced by its
// default, so callers outside the coordinator apply the same bounds it does.
func (t RestoreTimeouts) WithDefaults() RestoreTimeouts {
	return t.withDefaults()
}

func (t RestoreTimeouts) withDefaults() RestoreTimeouts {
	def := DefaultRestoreTimeouts()
	pick := func(v, d time.Duration) time.Duration {
		if v <= 0 {
			return d
		}
		return v
	}
	return RestoreTimeouts{
		Lease:         pick(t.Lease, def.Lease),
		Barrier:       pick(t.Barrier, def.Barrier),
		PartitionLoad: pick(t.PartitionLoad, def.PartitionLoad),
		Reconcile:     pick(t.Reconcile, def.Reconcile),
		Readiness:     pick(t.Readiness, def.Readiness),
		StateApply:    pick(t.StateApply, def.StateApply),
		Ingest:        pick(t.Ingest, def.Ingest),
	}
}

// RestoreDeps carries the coordinator's dependencies so both ZenNode (REST)
// and the gRPC server can drive a restore.
type RestoreDeps struct {
	Clients      ClientProvider
	ClusterState func() state.Cluster
	// ApplyRestoreChange commits a restore transition through the cluster raft
	// and returns the resulting operation. A refused transition is reported as
	// a *state.RestoreRejectedError.
	ApplyRestoreChange func(ctx context.Context, change *protoc.RestoreOperationChange) (state.RestoreOperation, error)
	// CoordinatorID is the id of the node driving the restore.
	CoordinatorID       string
	BinarySchemaVersion string
	SpoolDir            string
	Timeouts            RestoreTimeouts
	Limits              RestoreLimits
	// NewOperationID generates restore operation ids; defaults to UUIDs.
	NewOperationID func() string
	// Now is the coordinator's clock; defaults to time.Now.
	Now func() time.Time
	// PollInterval paces barrier and readiness polling; defaults to 100ms.
	PollInterval time.Duration
}

func (d RestoreDeps) withDefaults() RestoreDeps {
	d.Timeouts = d.Timeouts.withDefaults()
	d.Limits = d.Limits.withDefaults()
	if d.NewOperationID == nil {
		d.NewOperationID = uuid.NewString
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.PollInterval <= 0 {
		d.PollInterval = 100 * time.Millisecond
	}
	return d
}

// RunClusterRestore validates the bundle, acquires the cluster-wide restore
// operation, waits until every partition leader is quiesced, loads every
// partition sequentially, reconciles derived state, lifts the gate and waits
// for the engines to come back.
//
// Every phase is bounded by deps.Timeouts and fails with a *PhaseError. A
// failure after partition data was touched leaves the cluster gated; the
// operator retries the restore (with force=true) or aborts the operation.
func RunClusterRestore(ctx context.Context, deps RestoreDeps, r io.Reader, force bool) (*RestoreReport, error) {
	deps = deps.withDefaults()
	report := &RestoreReport{StartedAtMillis: deps.Now().UnixMilli()}
	cs := deps.ClusterState()
	if len(cs.Partitions) == 0 {
		return nil, fmt.Errorf("cluster has no partitions")
	}
	// cheap early refusal; the authoritative check is the atomic acquisition below
	if cs.Restore.Active() && !cs.Restore.LeaseExpired(deps.Now().UnixMilli()) {
		return nil, fmt.Errorf("%w: operation %s owned by %s", ErrRestoreInProgress, cs.Restore.ID, cs.Restore.CoordinatorID)
	}

	partitionCount := uint32(len(cs.Partitions)) // #nosec G115 -- partition counts are far below MaxUint32
	// receiving and validating the upload is bounded on its own: a client that
	// stalls an incomplete bundle must not hold the request open forever
	ingestCtx, cancelIngest := context.WithTimeout(ctx, deps.Timeouts.Ingest)
	defer cancelIngest()
	// A Read blocked on a stalled upload cannot observe the context; closing
	// the source when the deadline passes is what unblocks it (the REST layer
	// additionally arms a body read deadline).
	stopClosing := context.AfterFunc(ingestCtx, func() {
		if closer, ok := r.(io.Closer); ok {
			_ = closer.Close()
		}
	})
	bundle, err := OpenBundle(ingestCtx, r, deps.SpoolDir, partitionCount, deps.Limits)
	stopClosing()
	if err != nil {
		if ingestCtx.Err() != nil && ctx.Err() == nil {
			return nil, ingestTimeoutError(deps.Timeouts.Ingest, ingestCtx.Err())
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidBundle, err)
	}
	defer func() {
		// a leftover spool file must not turn a finished restore into a failure
		if err := bundle.Close(); err != nil {
			log.Warn("failed to remove restore spool files: %v", err)
		}
	}()
	if err := bundle.Manifest.Validate(partitionCount, deps.BinarySchemaVersion); err != nil {
		return nil, fmt.Errorf("%w: bundle cannot be restored into this cluster: %w", ErrInvalidBundle, err)
	}
	// The ingest deadline bounds the whole validation phase, not only the
	// upload: a bundle whose verification outlived it must not go on to take
	// ownership of the cluster under the (unbounded) request context.
	if err := ingestCtx.Err(); err != nil && ctx.Err() == nil {
		return nil, ingestTimeoutError(deps.Timeouts.Ingest, err)
	}

	run := &restoreRun{deps: deps, bundle: bundle, force: force, report: report,
		// lazy on purpose: the snapshot has to be taken once the cluster is
		// fenced, not during ingest
		clusterState: sync.OnceValue(deps.ClusterState),
	}
	return run.execute(ctx)
}

// restoreRun is one restore attempt that owns the cluster.
type restoreRun struct {
	deps   RestoreDeps
	bundle *Bundle
	force  bool
	report *RestoreReport
	// clusterState is one snapshot of the replicated state, taken on first
	// use during reconciliation: the cluster is quiesced and fenced by then,
	// so every definition is routed against the same partition map without
	// deep-copying the state per definition.
	clusterState func() state.Cluster

	// identity is the fencing token of this run. It is written once by
	// acquire, before the heartbeat starts, and never changes afterwards, so
	// it can be read without locking.
	identity restoreIdentity

	// progressMu guards the phase and progress of the owned operation. It
	// serializes every UPDATE so that the lease heartbeat can never move the
	// phase backwards behind the main flow.
	progressMu sync.Mutex
	phase      state.RestorePhase
	completed  uint32
}

// restoreIdentity is the immutable (id, epoch) of an acquired operation.
type restoreIdentity struct {
	id            string
	epoch         uint64
	coordinatorID string
}

func (run *restoreRun) token() (string, uint64) {
	return run.identity.id, run.identity.epoch
}

// ingestTimeoutError is the refusal for an upload or validation that outlived
// the ingest deadline. It carries both ErrInvalidBundle (nothing was recorded,
// the request may be repeated) and the deadline error, so the API layers can
// report it as a timeout rather than as a corrupt archive.
func ingestTimeoutError(timeout time.Duration, cause error) error {
	return fmt.Errorf("%w: bundle upload and validation did not finish within %s: %w", ErrInvalidBundle, timeout, cause)
}

func (run *restoreRun) execute(ctx context.Context) (*RestoreReport, error) {
	if err := run.acquire(ctx); err != nil {
		return nil, err
	}
	run.report.OperationID = run.identity.id
	run.report.Epoch = run.identity.epoch
	run.report.CoordinatorID = run.identity.coordinatorID

	// The heartbeat keeps the lease alive during long loads; losing the lease
	// cancels the whole run so a fenced-out coordinator stops touching partitions.
	runCtx, cancel := context.WithCancelCause(ctx)
	heartbeatDone := make(chan struct{})
	safego.Go("cluster-restore-lease-heartbeat", safego.DefaultLogger, func() {
		defer close(heartbeatDone)
		run.heartbeat(runCtx, cancel)
	})
	err := run.phases(runCtx)
	cancel(nil)
	<-heartbeatDone

	if err != nil {
		if cause := context.Cause(runCtx); errors.Is(cause, ErrRestoreOwnershipLost) {
			err = &PhaseError{OperationID: run.identity.id, Phase: run.currentPhase(), Err: cause}
		}
		run.report.Phase = run.currentPhase()
		run.fail(ctx, err)
		return run.report, err
	}
	run.report.Phase = state.RestorePhaseDone
	run.report.FinishedAtMillis = run.deps.Now().UnixMilli()
	return run.report, nil
}

func (run *restoreRun) currentPhase() state.RestorePhase {
	run.progressMu.Lock()
	defer run.progressMu.Unlock()
	return run.phase
}

// acquire takes exclusive ownership of the restore. The FSM refuses it while
// another coordinator holds a live lease.
func (run *restoreRun) acquire(ctx context.Context) error {
	applyCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.StateApply)
	defer cancel()
	op, err := run.deps.ApplyRestoreChange(applyCtx, &protoc.RestoreOperationChange{
		Action:          protoc.RestoreOperationChange_RESTORE_ACTION_ACQUIRE.Enum(),
		OperationId:     new(run.deps.NewOperationID()),
		CoordinatorId:   new(run.deps.CoordinatorID),
		Force:           new(run.force),
		TotalPartitions: new(uint32(len(run.bundle.Manifest.Partitions))), // #nosec G115 -- partition counts are far below MaxUint32
		TimestampMillis: new(run.deps.Now().UnixMilli()),
		LeaseMillis:     new(run.deps.Timeouts.Lease.Milliseconds()),
	})
	if err != nil {
		var rejected *state.RestoreRejectedError
		if errors.As(err, &rejected) {
			return fmt.Errorf("%w: %w", ErrRestoreInProgress, err)
		}
		if errors.Is(err, zenerr.ErrNotLeader) {
			return fmt.Errorf("cluster restore must be started on the cluster raft leader: %w", err)
		}
		return fmt.Errorf("failed to acquire cluster restore: %w", err)
	}
	run.identity = restoreIdentity{id: op.ID, epoch: op.Epoch, coordinatorID: op.CoordinatorID}
	run.progressMu.Lock()
	run.phase = op.Phase
	run.progressMu.Unlock()
	return nil
}

// update advances the phase / progress of the owned operation and renews the
// lease. A rejection means the coordinator was fenced out.
func (run *restoreRun) update(ctx context.Context, phase state.RestorePhase, completed uint32) error {
	run.progressMu.Lock()
	defer run.progressMu.Unlock()
	if phase.Index() > run.phase.Index() {
		run.phase = phase
	}
	run.completed = completed
	return run.sendUpdateLocked(ctx)
}

func (run *restoreRun) sendUpdateLocked(ctx context.Context) error {
	applyCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.StateApply)
	defer cancel()
	id, epoch := run.token()
	_, err := run.deps.ApplyRestoreChange(applyCtx, &protoc.RestoreOperationChange{
		Action:              protoc.RestoreOperationChange_RESTORE_ACTION_UPDATE.Enum(),
		OperationId:         new(id),
		Epoch:               new(epoch),
		Phase:               restorePhaseToProto(run.phase).Enum(),
		CompletedPartitions: new(run.completed),
		TotalPartitions:     new(uint32(len(run.bundle.Manifest.Partitions))), // #nosec G115 -- partition counts are far below MaxUint32
		TimestampMillis:     new(run.deps.Now().UnixMilli()),
		LeaseMillis:         new(run.deps.Timeouts.Lease.Milliseconds()),
	})
	if err != nil {
		var rejected *state.RestoreRejectedError
		if errors.As(err, &rejected) {
			return fmt.Errorf("%w: %w", ErrRestoreOwnershipLost, err)
		}
		return fmt.Errorf("failed to record restore progress (phase %s): %w", run.phase, err)
	}
	return nil
}

// heartbeat renews the lease until ctx ends. Being fenced out cancels the run.
func (run *restoreRun) heartbeat(ctx context.Context, cancel context.CancelCauseFunc) {
	interval := run.deps.Timeouts.Lease / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		run.progressMu.Lock()
		err := run.sendUpdateLocked(ctx)
		run.progressMu.Unlock()
		if errors.Is(err, ErrRestoreOwnershipLost) {
			cancel(err)
			return
		}
		// transport failures are retried on the next tick; the main flow fails
		// on its own if the leader is gone
	}
}

// fail attempts to record the failure on the operation. It uses a context
// detached from the (possibly cancelled) request so that a client disconnect
// does not prevent the terminal status from being written. The write itself
// can still fail (leadership lost, apply timeout); the operation then stays
// ACTIVE until its lease expires, so the outcome is logged with the operation
// id for the operator to correlate.
func (run *restoreRun) fail(ctx context.Context, cause error) {
	applyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), run.deps.Timeouts.StateApply)
	defer cancel()
	id, epoch := run.token()
	_, err := run.deps.ApplyRestoreChange(applyCtx, &protoc.RestoreOperationChange{
		Action:          protoc.RestoreOperationChange_RESTORE_ACTION_FAIL.Enum(),
		OperationId:     new(id),
		Epoch:           new(epoch),
		Error:           new(cause.Error()),
		TimestampMillis: new(run.deps.Now().UnixMilli()),
	})
	if err != nil {
		log.Error("failed to record failure of cluster restore %s (epoch %d, cause: %v); the operation stays active until its lease expires: %v", id, epoch, cause, err)
	}
}

func (run *restoreRun) complete(ctx context.Context) error {
	applyCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.StateApply)
	defer cancel()
	id, epoch := run.token()
	op, err := run.deps.ApplyRestoreChange(applyCtx, &protoc.RestoreOperationChange{
		Action:          protoc.RestoreOperationChange_RESTORE_ACTION_COMPLETE.Enum(),
		OperationId:     new(id),
		Epoch:           new(epoch),
		TimestampMillis: new(run.deps.Now().UnixMilli()),
	})
	if err != nil {
		var rejected *state.RestoreRejectedError
		if errors.As(err, &rejected) {
			return fmt.Errorf("%w: %w", ErrRestoreOwnershipLost, err)
		}
		return fmt.Errorf("failed to mark restore as completed: %w", err)
	}
	run.progressMu.Lock()
	run.phase = op.Phase
	run.progressMu.Unlock()
	return nil
}

func (run *restoreRun) phaseErr(phase state.RestorePhase, err error) error {
	return &PhaseError{OperationID: run.identity.id, Phase: phase, Err: err}
}

func (run *restoreRun) phases(ctx context.Context) error {
	ids := run.partitionIDs()

	// QUIESCING: nothing destructive may happen before every partition leader
	// has applied the restore state, stopped its engine and fenced writes.
	if err := run.update(ctx, state.RestorePhaseQuiescing, 0); err != nil {
		return run.phaseErr(state.RestorePhaseQuiescing, err)
	}
	if err := run.waitForPartitions(ctx, ids, run.deps.Timeouts.Barrier, "partition leader has not entered restore maintenance", quiescedCondition); err != nil {
		return run.phaseErr(state.RestorePhaseQuiescing, err)
	}

	// VALIDATING: checks that only hold once writes are fenced.
	if err := run.update(ctx, state.RestorePhaseValidating, 0); err != nil {
		return run.phaseErr(state.RestorePhaseValidating, err)
	}
	if !run.force {
		checkCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.Barrier)
		empty, err := clusterIsEmpty(checkCtx, ids, run.deps.Clients)
		cancel()
		if err != nil {
			return run.phaseErr(state.RestorePhaseValidating, fmt.Errorf("failed to check whether cluster is empty: %w", err))
		}
		if !empty {
			return run.phaseErr(state.RestorePhaseValidating, ErrClusterNotEmpty)
		}
	}

	// LOADING: sequential loads bound coordinator memory — each store.Load
	// holds one full partition image as a single raft entry.
	if err := run.update(ctx, state.RestorePhaseLoading, 0); err != nil {
		return run.phaseErr(state.RestorePhaseLoading, err)
	}
	for i, id := range ids {
		start := run.deps.Now()
		if err := run.loadPartition(ctx, id); err != nil {
			return run.phaseErr(state.RestorePhaseLoading, fmt.Errorf("restore of partition %d failed: %w", id, err))
		}
		run.report.Partitions = append(run.report.Partitions, PartitionRestoreResult{
			PartitionID: id,
			LoadMillis:  run.deps.Now().Sub(start).Milliseconds(),
		})
		if err := run.update(ctx, state.RestorePhaseLoading, uint32(i+1)); err != nil { // #nosec G115 -- partition counts are far below MaxUint32
			return run.phaseErr(state.RestorePhaseLoading, err)
		}
	}

	// RECONCILING: derived state, still fenced.
	if err := run.update(ctx, state.RestorePhaseReconciling, run.completedCount()); err != nil {
		return run.phaseErr(state.RestorePhaseReconciling, err)
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.Reconcile)
	err := run.reconcile(reconcileCtx, ids)
	cancel()
	if err != nil {
		return run.phaseErr(state.RestorePhaseReconciling, err)
	}

	// RESUMING: the gate is lifted; wait until the engines are back before
	// reporting success.
	if err := run.update(ctx, state.RestorePhaseResuming, run.completedCount()); err != nil {
		return run.phaseErr(state.RestorePhaseResuming, err)
	}
	if err := run.waitForPartitions(ctx, ids, run.deps.Timeouts.Readiness, "partition has not resumed after restore", resumedCondition); err != nil {
		return run.phaseErr(state.RestorePhaseResuming, err)
	}
	if err := run.complete(ctx); err != nil {
		return run.phaseErr(state.RestorePhaseResuming, err)
	}
	return nil
}

func (run *restoreRun) completedCount() uint32 {
	run.progressMu.Lock()
	defer run.progressMu.Unlock()
	return run.completed
}

func (run *restoreRun) partitionIDs() []uint32 {
	ids := make([]uint32, 0, len(run.bundle.Manifest.Partitions))
	for id := range run.bundle.Manifest.Partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// partitionCondition decides whether a partition status satisfies a barrier.
type partitionCondition func(*proto.PartitionRestoreStatusResponse) bool

func quiescedCondition(st *proto.PartitionRestoreStatusResponse) bool {
	return st.GetHosted() && st.GetLeader() && st.GetRestoreApplied() && st.GetEngineStopped() && st.GetWriteFenced()
}

func resumedCondition(st *proto.PartitionRestoreStatusResponse) bool {
	return st.GetHosted() && st.GetLeader() && st.GetEngineRunning() && st.GetInitialized()
}

// waitForPartitions polls every partition leader until cond holds for all of
// them, or timeout passes. The leader is re-resolved on every poll so that a
// leader change during the wait is followed.
func (run *restoreRun) waitForPartitions(ctx context.Context, ids []uint32, timeout time.Duration, what string, cond partitionCondition) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, epoch := run.token()
	pending := make(map[uint32]string, len(ids))
	for _, pid := range ids {
		pending[pid] = "not checked yet"
	}
	ticker := time.NewTicker(run.deps.PollInterval)
	defer ticker.Stop()
	for {
		for pid := range pending {
			leader, err := run.deps.Clients.PartitionLeader(pid)
			if err != nil {
				pending[pid] = fmt.Sprintf("no leader: %s", err)
				continue
			}
			st, err := leader.PartitionRestoreStatus(waitCtx, &proto.PartitionRestoreStatusRequest{
				PartitionId:        new(pid),
				RestoreOperationId: new(id),
				RestoreEpoch:       new(epoch),
			})
			if err != nil {
				pending[pid] = fmt.Sprintf("status call failed: %s", err)
				continue
			}
			if cond(st) {
				delete(pending, pid)
				continue
			}
			pending[pid] = describePartitionStatus(st)
		}
		if len(pending) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("%s: %w", what, ctx.Err())
			}
			// keep the deadline identity so callers map it to a timeout response
			return fmt.Errorf("%s within %s (%w): %s", what, timeout, waitCtx.Err(), describePending(pending))
		case <-ticker.C:
		}
	}
}

func describePartitionStatus(st *proto.PartitionRestoreStatusResponse) string {
	return fmt.Sprintf("hosted=%t leader=%t restoreApplied=%t engineStopped=%t writeFenced=%t engineRunning=%t initialized=%t",
		st.GetHosted(), st.GetLeader(), st.GetRestoreApplied(), st.GetEngineStopped(), st.GetWriteFenced(), st.GetEngineRunning(), st.GetInitialized())
}

func describePending(pending map[uint32]string) string {
	ids := make([]uint32, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += "; "
		}
		out += fmt.Sprintf("partition %d: %s", id, pending[id])
	}
	return out
}

// loadPartition ships one partition image to its leader within the partition
// load timeout. The stream carries the restore token so a stale coordinator is
// refused by the partition.
func (run *restoreRun) loadPartition(ctx context.Context, id uint32) (err error) {
	loadCtx, cancel := context.WithTimeout(ctx, run.deps.Timeouts.PartitionLoad)
	defer cancel()
	leader, err := run.deps.Clients.PartitionLeader(id)
	if err != nil {
		return fmt.Errorf("failed to get leader client: %w", err)
	}
	stream, err := leader.PartitionRestore(loadCtx)
	if err != nil {
		return fmt.Errorf("failed to open restore stream: %w", err)
	}
	opID, epoch := run.token()
	meta := run.bundle.Manifest.Partitions[id]
	err = stream.Send(&proto.RestoreChunk{Payload: &proto.RestoreChunk_Meta{Meta: &proto.RestoreMeta{
		PartitionId:        new(id),
		Sha256:             new(meta.SHA256),
		SizeBytes:          new(meta.SizeBytes),
		RestoreOperationId: new(opID),
		RestoreEpoch:       new(epoch),
	}}})
	if err != nil {
		return fmt.Errorf("failed to send restore meta: %w", err)
	}
	f, err := run.bundle.PartitionFile(id)
	if err != nil {
		return err
	}
	defer zenerr.CloseJoin(f, &err, fmt.Sprintf("partition %d spool file", id))
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
		if loadCtx.Err() != nil && ctx.Err() == nil {
			return fmt.Errorf("partition leader did not finish loading within %s (%w): %w", run.deps.Timeouts.PartitionLoad, loadCtx.Err(), err)
		}
		return fmt.Errorf("partition leader rejected restore: %w", err)
	}
	return nil
}

func clusterIsEmpty(ctx context.Context, ids []uint32, clients ClientProvider) (bool, error) {
	for _, id := range ids {
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

// reconcile rebuilds derived state (pointer tables, definition sync) after all
// partition images have been loaded. It is idempotent — safe to retry if a
// prior attempt failed mid-way — and runs while the partitions are still
// fenced, so nothing but this coordinator writes to them.
func (run *restoreRun) reconcile(ctx context.Context, ids []uint32) error {
	if err := run.syncDefinitions(ctx, ids); err != nil {
		return err
	}
	cs := run.deps.ClusterState()
	opID, epoch := run.token()

	var all []*proto.MessageSubscriptionRow
	for _, id := range ids {
		leader, err := run.deps.Clients.PartitionLeader(id)
		if err != nil {
			return fmt.Errorf("pointer scan: failed to get leader for partition %d: %w", id, err)
		}
		resp, err := leader.ListActiveMessageSubscriptions(ctx, &proto.ListActiveMessageSubscriptionsRequest{PartitionId: new(id)})
		if err != nil {
			return fmt.Errorf("pointer scan on partition %d failed: %w", id, err)
		}
		all = append(all, resp.GetRows()...)
	}

	plan := PlanPointerRebuild(all, cs.GetPartitionIdForMessageSubscriptionPointer)
	run.report.PointerConflicts = plan.Conflicts

	// every partition gets a rebuild call — even with zero rows — to wipe stale pointers
	for _, id := range ids {
		rows := plan.ByPartition[id]
		leader, err := run.deps.Clients.PartitionLeader(id)
		if err != nil {
			return fmt.Errorf("pointer rebuild: failed to get leader for partition %d: %w", id, err)
		}
		_, err = leader.RebuildMessageSubscriptionPointers(ctx, &proto.RebuildMessageSubscriptionPointersRequest{
			PartitionId:        new(id),
			Pointers:           rows,
			RestoreOperationId: new(opID),
			RestoreEpoch:       new(epoch),
		})
		if err != nil {
			return fmt.Errorf("pointer rebuild on partition %d failed: %w", id, err)
		}
		run.report.PointersRebuilt += len(rows)
	}
	return nil
}

// syncDefinitions lists definitions on every partition, computes which
// partitions are missing definitions (a deploy landed mid-backup), and copies
// them through the restore-only import path — no engine is involved, so the
// cluster stays fenced. It runs BEFORE the pointer rebuild so that
// subscriptions created for imported definitions are included in the scan.
//
// A copy keeps the key and version it has on the source partition; a
// partition that already holds a different definition at that version is not
// changed (which definition is the latest must not silently flip) and the
// conflict is reported instead. Definition-level subscriptions are registered
// on exactly the partition that owns them under the normal deployment rule
// (state.Cluster.DefinitionSubscriptionPartition), so no partition ends up
// with duplicates.
func (run *restoreRun) syncDefinitions(ctx context.Context, ids []uint32) error {
	perPartition, err := run.scanDefinitions(ctx, ids)
	if err != nil {
		return err
	}
	missing := MissingDefinitions(perPartition)
	targets := make([]uint32, 0, len(missing))
	for part := range missing {
		targets = append(targets, part)
	}
	slices.Sort(targets)

	synced := map[int64]*DefinitionSyncEntry{}
	for _, part := range targets {
		for _, ref := range missing[part] {
			entry, ok := synced[ref.GetKey()]
			if !ok {
				// toPartitions is an array in the report even when every
				// copy was refused
				entry = &DefinitionSyncEntry{Key: ref.GetKey(), Type: definitionTypeName(ref.GetType()), ToPartitions: []uint32{}}
				synced[ref.GetKey()] = entry
			}
			conflict, err := run.importDefinition(ctx, perPartition, ref, part)
			if err != nil {
				return err
			}
			if conflict != "" {
				entry.Conflicts = append(entry.Conflicts, DefinitionSyncConflict{Partition: part, Reason: conflict})
				continue
			}
			entry.ToPartitions = append(entry.ToPartitions, part)
		}
	}
	for _, e := range synced {
		run.report.DefinitionsSynced = append(run.report.DefinitionsSynced, *e)
	}
	sort.Slice(run.report.DefinitionsSynced, func(i, j int) bool {
		return run.report.DefinitionsSynced[i].Key < run.report.DefinitionsSynced[j].Key
	})
	return nil
}

// scanDefinitions lists the definition refs every partition holds.
func (run *restoreRun) scanDefinitions(ctx context.Context, ids []uint32) (map[uint32][]*proto.DefinitionRef, error) {
	perPartition := map[uint32][]*proto.DefinitionRef{}
	for _, id := range ids {
		leader, err := run.deps.Clients.PartitionLeader(id)
		if err != nil {
			return nil, fmt.Errorf("definition scan: failed to get leader for partition %d: %w", id, err)
		}
		resp, err := leader.ListDefinitions(ctx, &proto.ListDefinitionsRequest{PartitionId: new(id)})
		if err != nil {
			return nil, fmt.Errorf("definition scan on partition %d failed: %w", id, err)
		}
		perPartition[id] = resp.GetDefinitions()
	}
	return perPartition, nil
}

// importDefinition copies one definition from a partition that holds it into
// part, preserving its key and versions. A target that already holds another
// definition at the same version refuses the copy; that is returned as a
// conflict reason (and logged) rather than an error, so the restore completes
// and the operator sees the diverged history in the report.
func (run *restoreRun) importDefinition(ctx context.Context, perPartition map[uint32][]*proto.DefinitionRef, ref *proto.DefinitionRef, part uint32) (conflict string, err error) {
	typ := definitionTypeName(ref.GetType())
	source, err := fetchDefinition(ctx, run.deps, perPartition, ref)
	if err != nil {
		return "", err
	}
	target, err := run.deps.Clients.PartitionLeader(part)
	if err != nil {
		return "", fmt.Errorf("failed to get leader for target partition %d: %w", part, err)
	}
	opID, epoch := run.token()
	req := &proto.ImportDefinitionRequest{
		PartitionId:        new(part),
		RestoreOperationId: new(opID),
		RestoreEpoch:       new(epoch),
		Type:               ref.GetType().Enum(),
		Key:                new(ref.GetKey()),
		Version:            new(source.version),
		Decisions:          source.decisions,
		Data:               source.data,
		ResourceName:       new(source.resourceName),
	}
	if ref.GetType() == proto.DefinitionType_DEFINITION_TYPE_PROCESS {
		processID, err := processIDFromDefinition(source.data)
		if err != nil {
			return "", fmt.Errorf("failed to read process id of definition %d: %w", ref.GetKey(), err)
		}
		req.RegisterProcessDefinitionSubscriptions = new(run.clusterState().DefinitionSubscriptionPartition(processID) == part)
	}
	resp, err := target.ImportDefinition(ctx, req)
	if status.Code(err) == codes.AlreadyExists {
		reason := status.Convert(err).Message()
		log.Warn("restore %s: %s definition %d was not imported into partition %d, the partition already holds another definition at version %d: %s",
			opID, typ, ref.GetKey(), part, source.version, reason)
		return reason, nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to import %s definition %d into partition %d: %w", typ, ref.GetKey(), part, err)
	}
	if resp.GetError() != nil {
		return "", fmt.Errorf("failed to import %s definition %d into partition %d: %s", typ, ref.GetKey(), part, resp.GetError().GetMessage())
	}
	return "", nil
}

func definitionTypeName(typ proto.DefinitionType) string {
	if typ == proto.DefinitionType_DEFINITION_TYPE_PROCESS {
		return "process"
	}
	return "dmn"
}

func processIDFromDefinition(data []byte) (string, error) {
	var definitions bpmn20.TDefinitions
	if err := xml.Unmarshal(data, &definitions); err != nil {
		return "", fmt.Errorf("failed to unmarshal BPMN: %w", err)
	}
	if definitions.Process.Id == "" {
		return "", fmt.Errorf("BPMN has no process id")
	}
	return definitions.Process.Id, nil
}

// definitionSource is a definition as read from the partition that holds it.
type definitionSource struct {
	data         []byte
	resourceName string
	version      int32
	decisions    []*proto.DecisionDefinitionRef
}

// fetchDefinition reads the definition from the lowest-numbered partition
// that holds it, together with the versions it has there.
func fetchDefinition(ctx context.Context, deps RestoreDeps, perPartition map[uint32][]*proto.DefinitionRef, ref *proto.DefinitionRef) (definitionSource, error) {
	sources := make([]uint32, 0, len(perPartition))
	for part := range perPartition {
		sources = append(sources, part)
	}
	slices.Sort(sources)
	for _, part := range sources {
		for _, r := range perPartition[part] {
			if r.GetKey() == ref.GetKey() && r.GetType() == ref.GetType() {
				leader, err := deps.Clients.PartitionLeader(part)
				if err != nil {
					return definitionSource{}, err
				}
				resp, err := leader.GetDefinitionResource(ctx, &proto.GetDefinitionResourceRequest{
					PartitionId: new(part), Key: new(ref.GetKey()), Type: ref.GetType().Enum(),
				})
				if err != nil {
					return definitionSource{}, fmt.Errorf("failed to fetch definition %d from partition %d: %w", ref.GetKey(), part, err)
				}
				return definitionSource{data: resp.GetData(), resourceName: resp.GetResourceName(), version: resp.GetVersion(), decisions: resp.GetDecisions()}, nil
			}
		}
	}
	return definitionSource{}, fmt.Errorf("definition %d not found on any partition", ref.GetKey())
}

var restorePhaseToProtoValue = map[state.RestorePhase]protoc.RestorePhase{
	state.RestorePhasePending:     protoc.RestorePhase_RESTORE_PHASE_PENDING,
	state.RestorePhaseQuiescing:   protoc.RestorePhase_RESTORE_PHASE_QUIESCING,
	state.RestorePhaseValidating:  protoc.RestorePhase_RESTORE_PHASE_VALIDATING,
	state.RestorePhaseLoading:     protoc.RestorePhase_RESTORE_PHASE_LOADING,
	state.RestorePhaseReconciling: protoc.RestorePhase_RESTORE_PHASE_RECONCILING,
	state.RestorePhaseResuming:    protoc.RestorePhase_RESTORE_PHASE_RESUMING,
	state.RestorePhaseDone:        protoc.RestorePhase_RESTORE_PHASE_DONE,
}

func restorePhaseToProto(phase state.RestorePhase) protoc.RestorePhase {
	return restorePhaseToProtoValue[phase]
}
