package backup

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

// RestoreReport is returned to the operator after a cluster restore.
type RestoreReport struct {
	StartedAtMillis   int64                    `json:"startedAtMillis"`
	FinishedAtMillis  int64                    `json:"finishedAtMillis"`
	Partitions        []PartitionRestoreResult `json:"partitions"`
	PointersRebuilt   int                      `json:"pointersRebuilt"`
	PointerConflicts  []PointerConflict        `json:"pointerConflicts"`
	DefinitionsSynced []DefinitionSyncEntry    `json:"definitionsSynced"`
}
