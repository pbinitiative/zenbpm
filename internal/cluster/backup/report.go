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

type DefinitionSyncEntry struct {
	Key          int64    `json:"key"`
	Type         string   `json:"type"` // "process" | "dmn"
	ToPartitions []uint32 `json:"toPartitions"`
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
