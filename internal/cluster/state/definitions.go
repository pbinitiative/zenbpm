package state

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/pbinitiative/zenbpm/pkg/zenflake"
)

// ProcessDefinitionAllocation is the cluster-wide identity of one deployed
// BPMN process version: the key and numeric version every partition stores
// for the process, decided once through the main raft log before the
// deployment fans out.
// +k8s:deepcopy-gen=true
type ProcessDefinitionAllocation struct {
	Key     int64 `json:"key"`
	Version int32 `json:"version"`
	// Checksum is the hex MD5 of the raw BPMN bytes.
	Checksum          string `json:"checksum"`
	VersionTag        string `json:"versionTag,omitempty"`
	AllocatedAtMillis int64  `json:"allocatedAtMillis"`
}

// Exists reports whether the allocation records a version.
func (a ProcessDefinitionAllocation) Exists() bool {
	return a.Version > 0
}

// ProcessDefinitionVersions is the replicated allocation state of one BPMN
// process id. A deployment is deduplicated against the latest version of its
// process (re-deploying older content creates a new version), so the history
// of allocations is not needed to allocate; only the latest one is kept.
// Version tags are kept for the whole history because they stay unique
// across it.
// +k8s:deepcopy-gen=true
type ProcessDefinitionVersions struct {
	Latest ProcessDefinitionAllocation `json:"latest"`
	// VersionTags maps every version tag ever allocated for the process to
	// the allocation that carries it.
	VersionTags map[string]ProcessDefinitionAllocation `json:"versionTags,omitempty"`
}

// ProcessDefinitionAllocationRequest is the FSM-level form of a process
// definition allocation command.
type ProcessDefinitionAllocationRequest struct {
	ProcessID  string
	Checksum   string
	VersionTag string
	// Sequence is the raft log index of the command; it is folded into the
	// key of a new version.
	Sequence uint64
	// Observed is what the partitions hold of the process: its latest
	// version and every version carrying a version tag, empty when they hold
	// none. It seeds the allocation state with definitions deployed before
	// allocations were replicated; the FSM never moves behind it.
	Observed []ProcessDefinitionAllocation
	// RestoreID and RestoreEpoch identify the restore operation the cluster
	// state recorded when the partitions were observed (empty and zero when
	// the cluster was never restored). An observation that predates a
	// restore is refused: the restored partitions no longer hold what it saw.
	RestoreID    string
	RestoreEpoch uint64
	NowMillis    int64
}

// ObservedProcessDefinition is a definition of a process as a partition holds
// it, used to rebuild the allocation state from the partitions.
type ObservedProcessDefinition struct {
	ProcessID  string
	Key        int64
	Version    int32
	Checksum   string
	VersionTag string
}

// ProcessDefinitionAllocationRejectedError is returned when an allocation is
// refused; the cluster state is left untouched.
type ProcessDefinitionAllocationRejectedError struct {
	ProcessID string
	Reason    string
}

func (e *ProcessDefinitionAllocationRejectedError) Error() string {
	return fmt.Sprintf("process definition allocation for %q rejected: %s", e.ProcessID, e.Reason)
}

// AllocateProcessDefinition decides the (key, version) of a deployment. It is
// deterministic: every replica applying the same request to the same state
// ends up with the same allocation.
//
// The request's observed definitions first catch the recorded state up with
// the partitions: an observed version newer than the recorded latest becomes
// the latest, and every observed version tag is reserved for the version the
// partitions hold it on (the partitions are authoritative for what they
// hold). Then a checksum equal to the latest allocation returns that
// allocation with existing=true and changes nothing, so retries and
// concurrent identical deployments share one definition. Otherwise the next
// version is allocated under a key derived from the allocation clock and the
// sequence, unless the version tag is already taken: by a version of the
// same content, which is returned with existing=true so that a failed
// tagged deployment can be retried, or by a version of different content,
// which is rejected with a *ProcessDefinitionAllocationRejectedError.
func (c *Cluster) AllocateProcessDefinition(req ProcessDefinitionAllocationRequest) (allocation ProcessDefinitionAllocation, existing bool, err error) {
	if req.ProcessID == "" {
		return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "process id must not be empty"}
	}
	if req.Checksum == "" {
		return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "checksum must not be empty"}
	}
	if c.Restore.GatesCluster() {
		return ProcessDefinitionAllocation{}, false, &RestoreRejectedError{Reason: "process definitions cannot be allocated while a restore gates the cluster", Current: c.Restore}
	}
	if c.Restore.ID != req.RestoreID || c.Restore.Epoch != req.RestoreEpoch {
		return ProcessDefinitionAllocation{}, false, &RestoreRejectedError{Reason: "the deployment observed the partitions before a restore replaced them; deploy again", Current: c.Restore}
	}
	if req.NowMillis <= 0 {
		return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "timestamp must be positive"}
	}
	versions := c.ProcessDefinitions[req.ProcessID]
	for _, observed := range req.Observed {
		if observed.Key == 0 || observed.Checksum == "" || observed.Version <= 0 {
			return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "observed definitions must have a key, a version and a checksum"}
		}
	}
	versions.catchUp(req.Observed)
	if versions.Latest.Exists() && versions.Latest.Checksum == req.Checksum {
		c.setProcessDefinitionVersions(req.ProcessID, versions)
		return versions.Latest, true, nil
	}
	if req.VersionTag != "" {
		if taken, ok := versions.VersionTags[req.VersionTag]; ok {
			if taken.Checksum != req.Checksum {
				return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{
					ProcessID: req.ProcessID,
					Reason:    fmt.Sprintf("version tag %q is already used by version %d; even a failed deployment reserves its tag: retry the original content or use a new tag", req.VersionTag, taken.Version),
				}
			}
			// the same content under its own tag: the deployment of that
			// version is repeated (a retry after a partial failure)
			c.setProcessDefinitionVersions(req.ProcessID, versions)
			return taken, true, nil
		}
	}
	versions.Latest = ProcessDefinitionAllocation{
		Key:               c.nextProcessDefinitionKey(req.NowMillis, req.Sequence),
		Version:           versions.Latest.Version + 1,
		Checksum:          req.Checksum,
		VersionTag:        req.VersionTag,
		AllocatedAtMillis: req.NowMillis,
	}
	versions.recordVersionTag(versions.Latest)
	c.setProcessDefinitionVersions(req.ProcessID, versions)
	return versions.Latest, false, nil
}

// ResetProcessDefinitions replaces the allocation state of every process with
// what the partitions hold: the latest version of each process and the
// versions carrying a version tag. Processes absent from the definitions are
// forgotten. It is applied by a cluster restore while it reconciles the
// partitions it replaced, and only by the restore operation that owns the
// cluster: restoreID and restoreEpoch are its fencing token, and a reset by
// a superseded coordinator, or outside the reconciliation phase, is refused
// with a *RestoreRejectedError. It is deterministic whatever the order of
// the definitions. A definition without a process id, key, version or
// checksum is rejected and nothing changes.
func (c *Cluster) ResetProcessDefinitions(definitions []ObservedProcessDefinition, restoreID string, restoreEpoch uint64) error {
	if !c.Restore.Owns(restoreID, restoreEpoch) {
		return &RestoreRejectedError{Reason: "the process definition registry can only be reset by the restore operation that owns the cluster", Current: c.Restore}
	}
	if c.Restore.Phase != RestorePhaseReconciling {
		return &RestoreRejectedError{Reason: "the process definition registry can only be reset while the restore reconciles the partitions", Current: c.Restore}
	}
	for _, definition := range definitions {
		if definition.ProcessID == "" || definition.Key == 0 || definition.Version <= 0 || definition.Checksum == "" {
			return &ProcessDefinitionAllocationRejectedError{ProcessID: definition.ProcessID, Reason: "a definition of the reset must have a process id, a key, a version and a checksum"}
		}
	}
	byProcess := map[string][]ProcessDefinitionAllocation{}
	for _, definition := range definitions {
		byProcess[definition.ProcessID] = append(byProcess[definition.ProcessID], ProcessDefinitionAllocation{
			Key: definition.Key, Version: definition.Version, Checksum: definition.Checksum, VersionTag: definition.VersionTag,
		})
	}
	c.ProcessDefinitions = nil
	for processID, observed := range byProcess {
		var versions ProcessDefinitionVersions
		versions.catchUp(observed)
		c.setProcessDefinitionVersions(processID, versions)
	}
	return nil
}

// catchUp raises the recorded state to the observed definitions, which come
// from every partition of the cluster: the newest observed version becomes
// the latest when it is newer than the recorded one, and every observed
// version tag is reserved for the version the partitions hold it on,
// replacing a tag recorded for an allocation that may never have been
// deployed. A tag the partitions hold on several definitions (a history that
// diverged before allocations were replicated: each partition refuses a
// second definition under a tag, but two partitions may have accepted
// different ones) is reserved for the newest of them, by version then key,
// the definition the latest resolution prefers as well. A recorded latest
// version that no partition holds, while a partition holds another
// definition at that very version, was never deployed anywhere and could
// never be (the partitions refuse a second definition at a version): it is
// replaced by the observed one, so that an allocation made from a wrong
// observation does not block the process for good. The observations are
// visited in a fixed order so that every replica ends up with the same
// state.
func (v *ProcessDefinitionVersions) catchUp(observed []ProcessDefinitionAllocation) {
	ordered := slices.Clone(observed)
	slices.SortFunc(ordered, func(a, b ProcessDefinitionAllocation) int {
		if c := cmp.Compare(a.Version, b.Version); c != 0 {
			return c
		}
		return cmp.Compare(b.Key, a.Key) // highest key first, like the deployer
	})
	recordedLatestHeld := !v.Latest.Exists()
	observedTags := map[string]ProcessDefinitionAllocation{}
	for _, definition := range ordered {
		if definition.Version == v.Latest.Version && definition.Key == v.Latest.Key {
			recordedLatestHeld = true
		}
		if definition.VersionTag == "" {
			continue
		}
		if held, seen := observedTags[definition.VersionTag]; seen && !newerProcessDefinition(definition, held) {
			continue
		}
		observedTags[definition.VersionTag] = definition
		v.recordVersionTag(definition)
	}
	for _, definition := range ordered {
		if definition.Version == v.Latest.Version && !recordedLatestHeld {
			// the recorded allocation exists nowhere: its tag reservation goes
			// with it, or it would keep answering the content it was made for
			if v.Latest.VersionTag != "" && v.VersionTags[v.Latest.VersionTag] == v.Latest {
				delete(v.VersionTags, v.Latest.VersionTag)
			}
			v.Latest = definition
			recordedLatestHeld = true
		}
		if definition.Version > v.Latest.Version {
			v.Latest = definition
		}
	}
}

// newerProcessDefinition reports whether a is a newer definition than b: a
// higher version, or the higher key at the same version, the order the
// deployer and the latest resolution of catchUp use.
func newerProcessDefinition(a, b ProcessDefinitionAllocation) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.Key > b.Key
}

// nextProcessDefinitionKey builds the key of a new definition version. The
// millisecond comes from the replicated allocation clock, advanced past the
// writer's clock, so two allocations never share one; the sequence keeps the
// key apart from keys other generators built in the same millisecond.
func (c *Cluster) nextProcessDefinitionKey(nowMillis int64, sequence uint64) int64 {
	millis := max(nowMillis, c.ProcessDefinitionKeyClockMillis+1)
	c.ProcessDefinitionKeyClockMillis = millis
	return zenflake.GlobalKey(millis, int64(sequence&(1<<zenflake.StepBits-1))) // #nosec G115 -- masked to StepBits
}

func (c *Cluster) setProcessDefinitionVersions(processID string, versions ProcessDefinitionVersions) {
	if c.ProcessDefinitions == nil {
		c.ProcessDefinitions = map[string]ProcessDefinitionVersions{}
	}
	c.ProcessDefinitions[processID] = versions
}

func (v *ProcessDefinitionVersions) recordVersionTag(allocation ProcessDefinitionAllocation) {
	if allocation.VersionTag == "" {
		return
	}
	if v.VersionTags == nil {
		v.VersionTags = map[string]ProcessDefinitionAllocation{}
	}
	v.VersionTags[allocation.VersionTag] = allocation
}
