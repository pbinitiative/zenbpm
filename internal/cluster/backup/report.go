package backup

import "github.com/pbinitiative/zenbpm/internal/cluster/state"

type PartitionRestoreResult struct {
	PartitionID uint32 `json:"partitionId"`
	LoadMillis  int64  `json:"loadMillis"`
}

type PointerConflict struct {
	Name           string  `json:"name"`
	CorrelationKey string  `json:"correlationKey"`
	WinnerKey      int64   `json:"winnerKey"`
	LoserKeys      []int64 `json:"loserKeys"`
}

// DefinitionSyncEntry reports one definition the reconciliation found missing
// on some partitions: where it was copied to, and where it could not be.
type DefinitionSyncEntry struct {
	Key          int64    `json:"key"`
	Type         string   `json:"type"` // "process" | "dmn"
	ToPartitions []uint32 `json:"toPartitions"`
	// Conflicts lists the partitions that refused the copy because they
	// already hold another definition at the same version. Their version
	// histories diverged before the backup; the restore leaves them as they
	// are rather than changing which definition is the latest.
	Conflicts []DefinitionSyncConflict `json:"conflicts,omitempty"`
}

type DefinitionSyncConflict struct {
	Partition uint32 `json:"partition"`
	Reason    string `json:"reason"`
}

// RestoreReport is returned to the operator after a cluster restore. The
// operation id identifies the durable restore record in the cluster state
// (see the restore status endpoint); Phase is the phase the restore ended in.
type RestoreReport struct {
	OperationID       string                   `json:"operationId"`
	Epoch             uint64                   `json:"epoch"`
	CoordinatorID     string                   `json:"coordinatorId"`
	Phase             state.RestorePhase       `json:"phase"`
	StartedAtMillis   int64                    `json:"startedAtMillis"`
	FinishedAtMillis  int64                    `json:"finishedAtMillis"`
	Partitions        []PartitionRestoreResult `json:"partitions"`
	PointersRebuilt   int                      `json:"pointersRebuilt"`
	PointerConflicts  []PointerConflict        `json:"pointerConflicts"`
	DefinitionsSynced []DefinitionSyncEntry    `json:"definitionsSynced"`
}
