package state

import (
	"fmt"

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
// of confirmed allocations is not needed to allocate; only the latest one is
// kept. Allocations whose deployment was never confirmed stay until they are,
// so that a retry finds them whatever was allocated in between. Version tags
// are kept because they stay unique across the whole history.
// +k8s:deepcopy-gen=true
type ProcessDefinitionVersions struct {
	Latest ProcessDefinitionAllocation `json:"latest"`
	// VersionTags maps every version tag ever allocated for the process to
	// the version that carries it.
	VersionTags map[string]int32 `json:"versionTags,omitempty"`
	// Incomplete holds, by checksum, the allocations not yet confirmed to
	// have reached every partition.
	Incomplete map[string]ProcessDefinitionAllocation `json:"incomplete,omitempty"`
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
	// ObservedLatest is the latest definition of the process the requesting
	// node found on a partition, nil when the partition has none. It seeds
	// the allocation state with definitions deployed before allocations were
	// replicated; the FSM never moves behind it.
	ObservedLatest *ProcessDefinitionAllocation
	NowMillis      int64
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
// The request's observed latest definition first raises the recorded latest
// when it is newer (a partition that already holds versions the state does
// not know of). Then a checksum equal to the latest allocation, or to an
// allocation whose deployment was never confirmed, returns that allocation
// with existing=true and changes nothing, so retries and concurrent identical
// deployments share one definition. Otherwise the next version is allocated
// under a key derived from the allocation clock and the sequence, unless the
// version tag is already taken by another version of the process, which is
// rejected with a *ProcessDefinitionAllocationRejectedError. A new allocation
// counts as incomplete until ConfirmProcessDefinition is applied for it.
func (c *Cluster) AllocateProcessDefinition(req ProcessDefinitionAllocationRequest) (allocation ProcessDefinitionAllocation, existing bool, err error) {
	if req.ProcessID == "" {
		return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "process id must not be empty"}
	}
	if req.Checksum == "" {
		return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{ProcessID: req.ProcessID, Reason: "checksum must not be empty"}
	}
	versions := c.ProcessDefinitions[req.ProcessID]
	if observed := req.ObservedLatest; observed != nil && observed.Version > versions.Latest.Version {
		versions.Latest = *observed
		versions.recordVersionTag(observed.VersionTag, observed.Version)
	}
	if versions.Latest.Exists() && versions.Latest.Checksum == req.Checksum {
		c.setProcessDefinitionVersions(req.ProcessID, versions)
		return versions.Latest, true, nil
	}
	if incomplete, ok := versions.Incomplete[req.Checksum]; ok {
		c.setProcessDefinitionVersions(req.ProcessID, versions)
		return incomplete, true, nil
	}
	if req.VersionTag != "" {
		if taken, ok := versions.VersionTags[req.VersionTag]; ok {
			return ProcessDefinitionAllocation{}, false, &ProcessDefinitionAllocationRejectedError{
				ProcessID: req.ProcessID,
				Reason:    fmt.Sprintf("version tag %q is already used by version %d", req.VersionTag, taken),
			}
		}
	}
	versions.Latest = ProcessDefinitionAllocation{
		Key:               c.nextProcessDefinitionKey(req.NowMillis, req.Sequence),
		Version:           versions.Latest.Version + 1,
		Checksum:          req.Checksum,
		VersionTag:        req.VersionTag,
		AllocatedAtMillis: req.NowMillis,
	}
	versions.recordVersionTag(req.VersionTag, versions.Latest.Version)
	if versions.Incomplete == nil {
		versions.Incomplete = map[string]ProcessDefinitionAllocation{}
	}
	versions.Incomplete[req.Checksum] = versions.Latest
	c.setProcessDefinitionVersions(req.ProcessID, versions)
	return versions.Latest, false, nil
}

// ConfirmProcessDefinition records that the allocation with the given key
// reached every partition: a later deployment of the same content is then a
// new deployment, deduplicated against the latest version only. It returns
// the confirmed allocation and whether it was still recorded as incomplete;
// confirming an unknown or already confirmed allocation changes nothing.
func (c *Cluster) ConfirmProcessDefinition(processID string, key int64) (ProcessDefinitionAllocation, bool) {
	versions, ok := c.ProcessDefinitions[processID]
	if !ok {
		return ProcessDefinitionAllocation{}, false
	}
	for checksum, allocation := range versions.Incomplete {
		if allocation.Key != key {
			continue
		}
		delete(versions.Incomplete, checksum)
		if len(versions.Incomplete) == 0 {
			versions.Incomplete = nil
		}
		c.setProcessDefinitionVersions(processID, versions)
		return allocation, true
	}
	return ProcessDefinitionAllocation{}, false
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

func (v *ProcessDefinitionVersions) recordVersionTag(tag string, version int32) {
	if tag == "" {
		return
	}
	if v.VersionTags == nil {
		v.VersionTags = map[string]int32{}
	}
	v.VersionTags[tag] = version
}
