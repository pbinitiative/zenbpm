package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/safego"
	pb "google.golang.org/protobuf/proto"
)

type ClusterStateObserverFunc func(ctx context.Context)

// FSM is Finite State Machine of the system state
type FSM struct {
	store *Store
	// context used by the observer of a previous call
	previousChangeCtxCancel    context.CancelFunc
	clusterStateChangeObserver ClusterStateObserverFunc
}

// NewFSM returns a new FSM.
func NewFSM(s *Store) *FSM {
	return &FSM{store: s, clusterStateChangeObserver: func(ctx context.Context) {
		// wait for store to be open until we start sending change notifications
		if s.open.Load() {
			s.clusterStateChangeObserver(ctx)
		}
	}}
}

var _ raft.FSM = &FSM{}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var command proto.Command
	if err := pb.Unmarshal(l.Data, &command); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	var res interface{}
	switch command.GetType() {
	case proto.Command_TYPE_NODE_CHANGE:
		nodeChangeCommand := command.GetNodeChange()
		res = f.applyNodeChange(nodeChangeCommand)
	case proto.Command_TYPE_NODE_PARTITION_CHANGE:
		partitionChangeCommand := command.GetNodePartitionChange()
		res = f.applyPartitionChange(partitionChangeCommand)
	case proto.Command_TYPE_CLUSTER_MAINTENANCE_CHANGE:
		res = f.applyMaintenanceChange(command.GetClusterMaintenanceChange())
	case proto.Command_TYPE_PROCESS_DEFINITION_ALLOCATION:
		res = f.applyProcessDefinitionAllocation(command.GetProcessDefinitionAllocation(), l.Index)
	default:
		panic(fmt.Sprintf("unrecognized command type: %s", command.Type))
	}
	if f.store.clusterStateChangeObserver != nil {
		// cancel the context of previous goroutine to let it know that there is a newer change
		if f.previousChangeCtxCancel != nil {
			f.previousChangeCtxCancel()
		}
		ctx, cancel := context.WithCancel(context.Background())
		f.previousChangeCtxCancel = cancel
		safego.Go("cluster-state-change-observer", f.store.logger, func() {
			f.clusterStateChangeObserver(ctx)
		})
	}
	f.store.appliedTarget.Signal(l.Index)
	return res
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.store.stateMu.RLock()
	defer f.store.stateMu.RUnlock()

	return &fsmSnapshot{ClusterState: *f.store.state.DeepCopy()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	snapshot := legacyAwareSnapshot{}
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}
	clusterState := snapshot.ClusterState.Cluster
	if snapshot.ClusterState.Restoring {
		// snapshot written by a binary that predates restore operations while a
		// restore was in progress: keep the cluster gated
		clusterState.ApplyLegacyRestoringFlag(true, 0)
		f.store.logger.Warn("migrated legacy restoring flag from snapshot", "restore", clusterState.Restore.ID)
	}

	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	f.store.state = clusterState
	return nil
}

// legacyAwareSnapshot decodes snapshots of both shapes: the current one and
// the one written before restore operations existed, whose cluster state
// carried a plain "restoring" flag.
type legacyAwareSnapshot struct {
	ClusterState struct {
		state.Cluster
		Restoring bool `json:"restoring"`
	} `json:"clusterState"`
}

type FsmStore interface {
	LeaderID() (string, error)
	ClusterState() state.Cluster
}

func (f *FSM) applyNodeChange(nodeChangeCommand *proto.NodeChange) interface{} {
	changedState := FsmApplyNodeChange(f.store, nodeChangeCommand)
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	f.store.state = changedState
	return nil
}

func (f *FSM) applyPartitionChange(partitionChangeCommand *proto.NodePartitionChange) interface{} {
	changedState := FsmApplyPartitionChange(f.store, partitionChangeCommand)
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	f.store.state = changedState
	return nil
}

// RestoreApplyResult is what the FSM returns (through the raft ApplyFuture)
// for a restore command: the resulting operation, or the rejection that left
// the state untouched.
type RestoreApplyResult struct {
	Operation state.RestoreOperation
	Rejected  *state.RestoreRejectedError
}

func (f *FSM) applyMaintenanceChange(cmd *proto.ClusterMaintenanceChange) interface{} {
	change := cmd.GetRestore()
	if change == nil {
		// a "restoring" flag written by a binary that predates restore
		// operations: migrate it instead of dropping the safety gate
		//lint:ignore SA1019 the deprecated flag is read on purpose: it only exists to replay raft logs written by earlier binaries
		legacyRestoring := cmd.GetRestoring()
		f.store.stateMu.Lock()
		defer f.store.stateMu.Unlock()
		newState := *f.store.state.DeepCopy()
		newState.ApplyLegacyRestoringFlag(legacyRestoring, 0)
		f.store.state = newState
		f.store.logger.Warn("migrated legacy cluster restoring flag", "restoring", legacyRestoring, "restore", newState.Restore.ID)
		return RestoreApplyResult{Operation: newState.Restore}
	}
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	newState := *f.store.state.DeepCopy()
	if err := newState.ApplyRestoreChange(restoreChangeFromProto(change)); err != nil {
		var rejected *state.RestoreRejectedError
		if !errors.As(err, &rejected) {
			rejected = &state.RestoreRejectedError{Reason: err.Error(), Current: f.store.state.Restore}
		}
		return RestoreApplyResult{Operation: f.store.state.Restore, Rejected: rejected}
	}
	f.store.state = newState
	return RestoreApplyResult{Operation: newState.Restore}
}

// ProcessDefinitionAllocationResult is what the FSM returns (through the raft
// ApplyFuture) for a process definition allocation command. For an
// allocation it carries the allocation every partition must deploy and
// whether it already existed, or the rejection that left the state
// untouched; a reset carries no allocation. RestoreRejected reports a
// command fenced off by a cluster restore.
type ProcessDefinitionAllocationResult struct {
	Allocation      state.ProcessDefinitionAllocation
	Existing        bool
	Rejected        *state.ProcessDefinitionAllocationRejectedError
	RestoreRejected *state.RestoreRejectedError
}

// applyProcessDefinitionAllocation allocates or resets through the cluster
// state; the log index is the sequence a new version's key is derived from,
// so every replica builds the same key. Whatever its outcome, the command
// raises the protocol version the log requires of every member: a binary
// that does not know it cannot replay the log any more.
func (f *FSM) applyProcessDefinitionAllocation(cmd *proto.ProcessDefinitionAllocation, logIndex uint64) interface{} {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	f.store.state.MinProtocolVersion = max(f.store.state.MinProtocolVersion, state.ProtocolVersionProcessDefinitionAllocation)
	newState := *f.store.state.DeepCopy()
	var err error
	result := ProcessDefinitionAllocationResult{}
	switch cmd.GetAction() {
	case proto.ProcessDefinitionAllocation_ACTION_UNKNOWN, proto.ProcessDefinitionAllocation_ACTION_ALLOCATE:
		result.Allocation, result.Existing, err = newState.AllocateProcessDefinition(processDefinitionAllocationFromProto(cmd, logIndex))
	case proto.ProcessDefinitionAllocation_ACTION_RESET:
		err = newState.ResetProcessDefinitions(observedProcessDefinitionsFromProto(cmd.GetDefinitions()), cmd.GetRestoreOperationId(), cmd.GetRestoreEpoch())
	default:
		// a command written by a newer binary (or by an unreleased revision of this one): refuse it rather than guess
		err = &state.ProcessDefinitionAllocationRejectedError{
			ProcessID: cmd.GetProcessId(), Reason: fmt.Sprintf("unsupported allocation action %d", cmd.GetAction()),
		}
	}
	if err != nil {
		var restoreRejected *state.RestoreRejectedError
		if errors.As(err, &restoreRejected) {
			return ProcessDefinitionAllocationResult{RestoreRejected: restoreRejected}
		}
		var rejected *state.ProcessDefinitionAllocationRejectedError
		if !errors.As(err, &rejected) {
			rejected = &state.ProcessDefinitionAllocationRejectedError{ProcessID: cmd.GetProcessId(), Reason: err.Error()}
		}
		return ProcessDefinitionAllocationResult{Rejected: rejected}
	}
	f.store.state = newState
	return result
}

func processDefinitionAllocationFromProto(cmd *proto.ProcessDefinitionAllocation, logIndex uint64) state.ProcessDefinitionAllocationRequest {
	req := state.ProcessDefinitionAllocationRequest{
		ProcessID:    cmd.GetProcessId(),
		Checksum:     cmd.GetChecksum(),
		VersionTag:   cmd.GetVersionTag(),
		Sequence:     logIndex,
		RestoreID:    cmd.GetRestoreOperationId(),
		RestoreEpoch: cmd.GetRestoreEpoch(),
		NowMillis:    cmd.GetTimestampMillis(),
	}
	for _, observed := range cmd.GetObserved() {
		req.Observed = append(req.Observed, state.ProcessDefinitionAllocation{
			Key:        observed.GetKey(),
			Version:    observed.GetVersion(),
			Checksum:   observed.GetChecksum(),
			VersionTag: observed.GetVersionTag(),
		})
	}
	return req
}

func observedProcessDefinitionsFromProto(definitions []*proto.ObservedProcessDefinition) []state.ObservedProcessDefinition {
	observed := make([]state.ObservedProcessDefinition, 0, len(definitions))
	for _, definition := range definitions {
		observed = append(observed, state.ObservedProcessDefinition{
			ProcessID:  definition.GetProcessId(),
			Key:        definition.GetKey(),
			Version:    definition.GetVersion(),
			Checksum:   definition.GetChecksum(),
			VersionTag: definition.GetVersionTag(),
		})
	}
	return observed
}

func restoreChangeFromProto(change *proto.RestoreOperationChange) state.RestoreChange {
	return state.RestoreChange{
		Action:              restoreActionFromProto(change.GetAction()),
		OperationID:         change.GetOperationId(),
		Epoch:               change.GetEpoch(),
		CoordinatorID:       change.GetCoordinatorId(),
		Phase:               restorePhaseFromProto(change.GetPhase()),
		TotalPartitions:     change.GetTotalPartitions(),
		CompletedPartitions: change.GetCompletedPartitions(),
		Error:               change.GetError(),
		Force:               change.GetForce(),
		NowMillis:           change.GetTimestampMillis(),
		LeaseMillis:         change.GetLeaseMillis(),
	}
}

func restoreActionFromProto(action proto.RestoreOperationChange_Action) state.RestoreAction {
	switch action {
	case proto.RestoreOperationChange_RESTORE_ACTION_ACQUIRE:
		return state.RestoreActionAcquire
	case proto.RestoreOperationChange_RESTORE_ACTION_UPDATE:
		return state.RestoreActionUpdate
	case proto.RestoreOperationChange_RESTORE_ACTION_COMPLETE:
		return state.RestoreActionComplete
	case proto.RestoreOperationChange_RESTORE_ACTION_FAIL:
		return state.RestoreActionFail
	case proto.RestoreOperationChange_RESTORE_ACTION_ABORT:
		return state.RestoreActionAbort
	default:
		return state.RestoreActionUnknown
	}
}

var restorePhaseByProto = map[proto.RestorePhase]state.RestorePhase{
	proto.RestorePhase_RESTORE_PHASE_PENDING:     state.RestorePhasePending,
	proto.RestorePhase_RESTORE_PHASE_QUIESCING:   state.RestorePhaseQuiescing,
	proto.RestorePhase_RESTORE_PHASE_VALIDATING:  state.RestorePhaseValidating,
	proto.RestorePhase_RESTORE_PHASE_LOADING:     state.RestorePhaseLoading,
	proto.RestorePhase_RESTORE_PHASE_RECONCILING: state.RestorePhaseReconciling,
	proto.RestorePhase_RESTORE_PHASE_RESUMING:    state.RestorePhaseResuming,
	proto.RestorePhase_RESTORE_PHASE_DONE:        state.RestorePhaseDone,
}

// restorePhaseFromProto maps the wire enum to the state phase; unknown values
// map to an empty (invalid) phase that the FSM rejects.
func restorePhaseFromProto(phase proto.RestorePhase) state.RestorePhase {
	return restorePhaseByProto[phase]
}

func FsmApplyNodeChange(store FsmStore, nodeChangeCommand *proto.NodeChange) state.Cluster {
	currState := store.ClusterState()
	node, ok := currState.Nodes[nodeChangeCommand.GetNodeId()]
	// node is not yet present in the store
	role := state.RoleFollower
	leaderId, _ := store.LeaderID()
	if leaderId == nodeChangeCommand.GetNodeId() {
		role = state.RoleLeader
	}
	if !ok {
		// TODO: check state of the node it should be starting
		node = state.Node{
			Id:         nodeChangeCommand.GetNodeId(),
			Addr:       nodeChangeCommand.GetAddr(),
			State:      state.NodeState(nodeChangeCommand.GetState()),
			Partitions: map[uint32]state.NodePartition{},
		}
	}
	// A Shutdown → Started transition means the peer's heartbeat resumed.
	// shutdownNode cleared its partition roles to UNKNOWN; restore them here so
	// read selectors and leader routing pick the node back up: Leader where the
	// partition still records this node as its leader (a newer leader election
	// replaces both the record and the role in one apply), Follower otherwise.
	resuming := ok &&
		node.State == state.NodeStateShutdown &&
		nodeChangeCommand.GetState() == proto.NodeState_NODE_STATE_STARTED
	// if the leader has changed, change other nodes to be followers
	if leaderId == node.Id && node.Role < state.RoleLeader && role == state.RoleLeader {
		for k, n := range currState.Nodes {
			n.Role = state.RoleFollower
			currState.Nodes[k] = n
		}
	}
	node.Role = role
	if nodeChangeCommand.GetAddr() != "" {
		node.Addr = nodeChangeCommand.GetAddr()
	}
	if nodeChangeCommand.GetSuffrage() != proto.RaftSuffrage_RAFT_SUFFRAGE_UNKNOWN {
		switch nodeChangeCommand.GetSuffrage() {
		case proto.RaftSuffrage_RAFT_SUFFRAGE_VOTER:
			node.Suffrage = raft.Voter
		case proto.RaftSuffrage_RAFT_SUFFRAGE_NONVOTER:
			node.Suffrage = raft.Nonvoter
		}
	}
	if nodeChangeCommand.GetState() != proto.NodeState_NODE_STATE_UNKNOWN {
		node.State = state.NodeState(nodeChangeCommand.GetState())
	}
	// the protocol version is known for a running binary only: a node
	// recorded as shut down holds none, whatever an announcement in flight
	// says, so that a node that comes back announces again and one that
	// comes back with an older binary (which announces nothing) is not mistaken for the one that left
	if node.State == state.NodeStateShutdown {
		node.ProtocolVersion = 0
	} else if version := nodeChangeCommand.GetProtocolVersion(); version > 0 {
		node.ProtocolVersion = version
	}
	if resuming {
		for partitionId, np := range node.Partitions {
			np.Role = state.RoleFollower
			if p, ok := currState.Partitions[partitionId]; ok && p.LeaderId == node.Id {
				np.Role = state.RoleLeader
			}
			node.Partitions[partitionId] = np
		}
	}
	currState.Nodes[nodeChangeCommand.GetNodeId()] = node
	return currState
}

func FsmApplyPartitionChange(store FsmStore, partitionChangeCommand *proto.NodePartitionChange) state.Cluster {
	currState := store.ClusterState()
	node, ok := currState.Nodes[partitionChangeCommand.GetNodeId()]
	// node is not yet present in the store
	if !ok {
		node = state.Node{
			Id:         partitionChangeCommand.GetNodeId(),
			Partitions: make(map[uint32]state.NodePartition),
		}
	}
	if partitionChangeCommand.GetState() == proto.NodePartitionState_NODE_PARTITION_STATE_LEAVING {
		delete(node.Partitions, partitionChangeCommand.GetPartitionId())
		currState.Nodes[partitionChangeCommand.GetNodeId()] = node
		return currState
	}
	node.Partitions[partitionChangeCommand.GetPartitionId()] = state.NodePartition{
		Id:    partitionChangeCommand.GetPartitionId(),
		State: state.NodePartitionState(partitionChangeCommand.GetState()),
		Role:  state.Role(partitionChangeCommand.GetRole()),
	}
	if partitionChangeCommand.GetRole() == proto.Role_ROLE_TYPE_LEADER {
		partitionID := partitionChangeCommand.GetPartitionId()
		if previous, exists := currState.Partitions[partitionID]; exists && previous.LeaderId != partitionChangeCommand.GetNodeId() {
			if previousNode, nodeExists := currState.Nodes[previous.LeaderId]; nodeExists {
				if previousPartition, partitionExists := previousNode.Partitions[partitionID]; partitionExists {
					previousPartition.Role = state.RoleFollower
					previousNode.Partitions[partitionID] = previousPartition
					currState.Nodes[previous.LeaderId] = previousNode
				}
			}
		}
		currState.Partitions[partitionChangeCommand.GetPartitionId()] = state.Partition{
			Id:       partitionChangeCommand.GetPartitionId(),
			LeaderId: partitionChangeCommand.GetNodeId(),
		}
	}
	currState.Nodes[partitionChangeCommand.GetNodeId()] = node
	return currState
}
