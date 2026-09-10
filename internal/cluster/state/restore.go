package state

import (
	"fmt"
)

// RestorePhase tracks how far a cluster restore operation has progressed.
// Phases are ordered; the numeric index of a phase (see restorePhaseIndex) is
// what the gating logic compares.
type RestorePhase string

const (
	// RestorePhasePending the operation was acquired but the partitions have
	// not been quiesced yet.
	RestorePhasePending RestorePhase = "PENDING"
	// RestorePhaseQuiescing waiting for every partition leader to stop its
	// engine and fence application writes.
	RestorePhaseQuiescing RestorePhase = "QUIESCING"
	// RestorePhaseValidating running the checks that need a fenced cluster
	// (the force=false empty-cluster check).
	RestorePhaseValidating RestorePhase = "VALIDATING"
	// RestorePhaseLoading partition images are being loaded. From here on the
	// cluster holds partially restored data until the operation finishes.
	RestorePhaseLoading RestorePhase = "LOADING"
	// RestorePhaseReconciling definitions are synced and pointer tables rebuilt.
	RestorePhaseReconciling RestorePhase = "RECONCILING"
	// RestorePhaseResuming the gate is lifted and the coordinator waits for
	// the partition engines to come back.
	RestorePhaseResuming RestorePhase = "RESUMING"
	// RestorePhaseDone terminal phase of a completed operation.
	RestorePhaseDone RestorePhase = "DONE"
)

var restorePhaseOrder = []RestorePhase{
	RestorePhasePending,
	RestorePhaseQuiescing,
	RestorePhaseValidating,
	RestorePhaseLoading,
	RestorePhaseReconciling,
	RestorePhaseResuming,
	RestorePhaseDone,
}

// Index returns the position of the phase in the restore lifecycle, -1 for an
// unknown phase.
func (p RestorePhase) Index() int {
	for i, phase := range restorePhaseOrder {
		if phase == p {
			return i
		}
	}
	return -1
}

// Valid reports whether p is a known restore phase.
func (p RestorePhase) Valid() bool {
	return p.Index() >= 0
}

// ModifiesData reports whether reaching the phase means partition data has been
// (or is being) overwritten.
func (p RestorePhase) ModifiesData() bool {
	return p.Index() >= RestorePhaseLoading.Index()
}

// RestoreStatus is the terminal / non-terminal status of a restore operation.
type RestoreStatus string

const (
	RestoreStatusActive    RestoreStatus = "ACTIVE"
	RestoreStatusCompleted RestoreStatus = "COMPLETED"
	RestoreStatusFailed    RestoreStatus = "FAILED"
	RestoreStatusAborted   RestoreStatus = "ABORTED"
)

// RestoreOperation is the raft-replicated record of the current (or most
// recent) cluster restore. Exactly one operation can be active at a time; the
// (ID, Epoch) pair is the fencing token that every destructive partition
// operation must carry.
// +k8s:deepcopy-gen=true
type RestoreOperation struct {
	ID string `json:"id"`
	// Epoch increases with every acquisition. A coordinator whose epoch is no
	// longer current has been fenced out.
	Epoch         uint64        `json:"epoch"`
	CoordinatorID string        `json:"coordinatorId"`
	Phase         RestorePhase  `json:"phase"`
	Status        RestoreStatus `json:"status"`
	Force         bool          `json:"force"`
	// DataModified is set once the operation started loading partition images.
	// It is inherited by a follow-up operation so that partially restored data
	// keeps the cluster gated until a restore succeeds or is aborted.
	DataModified         bool   `json:"dataModified"`
	TotalPartitions      uint32 `json:"totalPartitions"`
	CompletedPartitions  uint32 `json:"completedPartitions"`
	Error                string `json:"error,omitempty"`
	StartedAtMillis      int64  `json:"startedAtMillis"`
	UpdatedAtMillis      int64  `json:"updatedAtMillis"`
	FinishedAtMillis     int64  `json:"finishedAtMillis,omitempty"`
	LeaseExpiresAtMillis int64  `json:"leaseExpiresAtMillis"`
	// PreviousOperationID links a retry to the operation it superseded.
	PreviousOperationID string `json:"previousOperationId,omitempty"`
}

// Exists reports whether any restore operation was ever recorded.
func (r RestoreOperation) Exists() bool {
	return r.ID != ""
}

// Active reports whether the operation is owned by a coordinator and has not
// reached a terminal status. An active operation with an expired lease is
// still active until somebody acquires over it or aborts it.
func (r RestoreOperation) Active() bool {
	return r.Exists() && r.Status == RestoreStatusActive
}

// Terminal reports whether the operation finished (successfully or not).
func (r RestoreOperation) Terminal() bool {
	return r.Exists() && r.Status != RestoreStatusActive
}

// LeaseExpired reports whether the coordinator's lease ran out at nowMillis.
func (r RestoreOperation) LeaseExpired(nowMillis int64) bool {
	return r.Active() && nowMillis >= r.LeaseExpiresAtMillis
}

// Owns reports whether the given fencing token identifies the current owner of
// an active operation.
func (r RestoreOperation) Owns(id string, epoch uint64) bool {
	return r.Active() && r.ID == id && r.Epoch == epoch
}

// GatesCluster reports whether the operation keeps the cluster fenced: engines
// stopped and mutating requests rejected. That is the case while an active
// operation has not lifted the gate (phase before RESUMING) and after a failure
// that already overwrote partition data.
func (r RestoreOperation) GatesCluster() bool {
	if !r.Exists() || r.Phase.Index() >= RestorePhaseResuming.Index() {
		return false
	}
	switch r.Status {
	case RestoreStatusActive:
		return true
	case RestoreStatusFailed:
		return r.DataModified
	default:
		return false
	}
}

// RestoreAction is a restore state transition applied by the FSM.
type RestoreAction int

const (
	RestoreActionUnknown RestoreAction = iota
	RestoreActionAcquire
	RestoreActionUpdate
	RestoreActionComplete
	RestoreActionFail
	RestoreActionAbort
)

// RestoreChange is the FSM-level form of a restore command. NowMillis is the
// writer's clock: the transition never consults the wall clock so that every
// replica applies it identically.
type RestoreChange struct {
	Action              RestoreAction
	OperationID         string
	Epoch               uint64
	CoordinatorID       string
	Phase               RestorePhase
	TotalPartitions     uint32
	CompletedPartitions uint32
	Error               string
	Force               bool
	NowMillis           int64
	LeaseMillis         int64
}

// RestoreRejectedError is returned when a restore transition is refused. The
// cluster state is left untouched; Current describes the operation that
// blocked the change.
type RestoreRejectedError struct {
	Reason  string
	Current RestoreOperation
}

func (e *RestoreRejectedError) Error() string {
	if e.Current.Exists() {
		return fmt.Sprintf("restore change rejected: %s (current operation %s, epoch %d, status %s, phase %s)",
			e.Reason, e.Current.ID, e.Current.Epoch, e.Current.Status, e.Current.Phase)
	}
	return "restore change rejected: " + e.Reason
}

// ApplyRestoreChange applies a restore transition to the cluster state. It is
// deterministic and either mutates c or returns a *RestoreRejectedError with
// the state unchanged.
func (c *Cluster) ApplyRestoreChange(change RestoreChange) error {
	current := c.Restore
	reject := func(reason string) error {
		return &RestoreRejectedError{Reason: reason, Current: current}
	}
	switch change.Action {
	case RestoreActionAcquire:
		if change.OperationID == "" {
			return reject("operation id must not be empty")
		}
		if current.Active() && !current.LeaseExpired(change.NowMillis) {
			return reject(fmt.Sprintf("restore %s is in progress on coordinator %s", current.ID, current.CoordinatorID))
		}
		next := RestoreOperation{
			ID:                   change.OperationID,
			Epoch:                current.Epoch + 1,
			CoordinatorID:        change.CoordinatorID,
			Phase:                RestorePhasePending,
			Status:               RestoreStatusActive,
			Force:                change.Force,
			TotalPartitions:      change.TotalPartitions,
			StartedAtMillis:      change.NowMillis,
			UpdatedAtMillis:      change.NowMillis,
			LeaseExpiresAtMillis: change.NowMillis + change.LeaseMillis,
		}
		if current.GatesCluster() {
			// a previous attempt left partially restored data (or died mid-way):
			// keep the cluster gated until this attempt succeeds
			next.DataModified = current.DataModified
			next.PreviousOperationID = current.ID
		}
		c.Restore = next
		return nil
	case RestoreActionUpdate:
		if !current.Owns(change.OperationID, change.Epoch) {
			return reject("stale restore owner")
		}
		if !change.Phase.Valid() {
			return reject(fmt.Sprintf("unknown restore phase %q", change.Phase))
		}
		if change.Phase.Index() < current.Phase.Index() {
			return reject(fmt.Sprintf("restore phase cannot move backwards from %s to %s", current.Phase, change.Phase))
		}
		current.Phase = change.Phase
		if change.TotalPartitions > 0 {
			current.TotalPartitions = change.TotalPartitions
		}
		current.CompletedPartitions = change.CompletedPartitions
		current.UpdatedAtMillis = change.NowMillis
		if change.LeaseMillis > 0 {
			current.LeaseExpiresAtMillis = change.NowMillis + change.LeaseMillis
		}
		if change.Phase.ModifiesData() {
			current.DataModified = true
		}
		c.Restore = current
		return nil
	case RestoreActionComplete:
		if !current.Owns(change.OperationID, change.Epoch) {
			return reject("stale restore owner")
		}
		current.Status = RestoreStatusCompleted
		current.Phase = RestorePhaseDone
		current.CompletedPartitions = current.TotalPartitions
		current.UpdatedAtMillis = change.NowMillis
		current.FinishedAtMillis = change.NowMillis
		c.Restore = current
		return nil
	case RestoreActionFail:
		if !current.Owns(change.OperationID, change.Epoch) {
			return reject("stale restore owner")
		}
		current.Status = RestoreStatusFailed
		current.Error = change.Error
		current.UpdatedAtMillis = change.NowMillis
		current.FinishedAtMillis = change.NowMillis
		c.Restore = current
		return nil
	case RestoreActionAbort:
		if !current.Exists() || current.ID != change.OperationID {
			return reject(fmt.Sprintf("restore operation %s not found", change.OperationID))
		}
		if current.Status == RestoreStatusCompleted || current.Status == RestoreStatusAborted {
			return reject(fmt.Sprintf("restore operation %s is already %s", current.ID, current.Status))
		}
		current.Status = RestoreStatusAborted
		if change.Error != "" {
			current.Error = change.Error
		}
		current.UpdatedAtMillis = change.NowMillis
		current.FinishedAtMillis = change.NowMillis
		c.Restore = current
		return nil
	default:
		return reject(fmt.Sprintf("unknown restore action %d", change.Action))
	}
}

// LegacyRestoreOperationID identifies the operation synthesized from the
// "restoring" flag of a binary that predates restore operations.
const LegacyRestoreOperationID = "legacy-restore"

// ApplyLegacyRestoringFlag migrates the pre-operation "restoring" flag. A true
// flag means an earlier binary was interrupted mid-restore: the cluster stays
// gated through a synthetic FAILED operation that already modified data, so an
// operator has to retry (force=true) or abort explicitly. A false flag means
// that legacy restore finished; it completes the synthetic operation. The
// migration is deterministic and idempotent so log replay and snapshot restore
// converge on the same state.
func (c *Cluster) ApplyLegacyRestoringFlag(restoring bool, nowMillis int64) {
	current := c.Restore
	if restoring {
		if current.GatesCluster() {
			return // an operation already holds the cluster
		}
		c.Restore = RestoreOperation{
			ID:                  LegacyRestoreOperationID,
			Epoch:               current.Epoch + 1,
			CoordinatorID:       "legacy",
			Phase:               RestorePhaseLoading,
			Status:              RestoreStatusFailed,
			DataModified:        true,
			Error:               "restore interrupted by a binary that predates restore operations; retry the restore with force=true or abort the operation",
			StartedAtMillis:     nowMillis,
			UpdatedAtMillis:     nowMillis,
			FinishedAtMillis:    nowMillis,
			PreviousOperationID: current.ID,
		}
		return
	}
	if current.ID == LegacyRestoreOperationID && current.GatesCluster() {
		current.Status = RestoreStatusCompleted
		current.Phase = RestorePhaseDone
		current.Error = ""
		current.UpdatedAtMillis = nowMillis
		current.FinishedAtMillis = nowMillis
		c.Restore = current
	}
}

// RestoreInProgress reports whether the cluster is gated by a restore
// operation: engines are stopped and mutating requests are rejected.
func (c Cluster) RestoreInProgress() bool {
	return c.Restore.GatesCluster()
}
