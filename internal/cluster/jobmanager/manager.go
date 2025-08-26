package jobmanager

import (
	"context"
	"fmt"
	"maps"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"google.golang.org/grpc"
)

var (
	NodeIsNotALeader error = fmt.Errorf("Node is not a leader")
)

type Store interface {
	ClusterState() store.ClusterState
	NodeID() string
}

type ClientID string
type JobType string

type NodeId string

// JobManager handles job distribution in the system.
// When external application makes a call to the public API, the API registers
// the clientId and jobType.
// JobManager serves as a client subscribing to the partition leader nodes for required jobTypes
// and at the same time as a server distributing created jobs by the engine among subscribers.
type JobManager struct {
	ctx    context.Context
	client *jobClient
	server *jobServer
	store  Store

	currentPartitionRoles map[uint32]string

	// server needs its own context because we might cancel it on leader changes
	serverCtx    context.Context
	serverCancel context.CancelFunc
	loader       JobLoader
	completer    JobCompleter
}

type Job struct {
	Key         int64
	InstanceKey int64
	Variables   []byte
	Type        JobType
	State       int64
	ElementID   string
	CreatedAt   int64
	ClientID    ClientID
}

func New(
	ctx context.Context,
	store Store,
	clientManager *client.ClientManager,
	loader JobLoader,
	completer JobCompleter,
) *JobManager {
	return &JobManager{
		ctx:       ctx,
		client:    newJobClient(ctx, NodeId(store.NodeID()), store, clientManager),
		store:     store,
		loader:    loader,
		completer: completer,
	}
}

func (m *JobManager) Start() {
	state := m.store.ClusterState()
	isLeader := false
	for _, partition := range state.Partitions {
		if partition.LeaderId == m.store.NodeID() {
			isLeader = true
			break
		}
	}
	if isLeader {
		m.serverCtx, m.serverCancel = context.WithCancel(m.ctx)
		m.server = newJobServer(NodeId(m.store.NodeID()), m.loader, m.completer)
		m.server.startServer(m.serverCtx)
	}
	m.client.startClient()
}

func (m *JobManager) AddClient(ctx context.Context, clientId ClientID, clientRcv chan Job) error {
	return m.client.addClient(ctx, clientId, clientRcv)
}

func (m *JobManager) RemoveClient(ctx context.Context, clientId ClientID) {
	m.client.removeClient(ctx, clientId)
}

func (m *JobManager) AddClientJobSub(ctx context.Context, clientId ClientID, jobType JobType) {
	m.client.addJobSub(ctx, clientId, jobType)
}

func (m *JobManager) RemoveClientJobSub(ctx context.Context, clientId ClientID, jobType JobType) {
	m.client.removeJobSub(ctx, clientId, jobType)
}

func (m *JobManager) AddNodeSubscription(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	if m.server == nil {
		return NodeIsNotALeader
	}
	return m.server.addNodeSubscription(stream)
}

// CompleteJobReq is called by a client to request job completion
func (m *JobManager) CompleteJobReq(ctx context.Context, clientId ClientID, jobKey int64, variables map[string]any) error {
	return m.client.completeJob(ctx, clientId, jobKey, variables)
}

// CompleteJob is called by internal GRPC server to finish job completion
func (m *JobManager) CompleteJob(ctx context.Context, clientId ClientID, jobKey int64, variables map[string]any) error {
	if m.server == nil {
		return NodeIsNotALeader
	}
	return m.server.completeJob(ctx, clientId, jobKey, variables)
}

// FailJobReq is called by a client to request job failure
func (m *JobManager) FailJobReq(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	return m.client.failJob(ctx, clientID, jobKey, message, errorCode, variables)
}

// FailJob is called by internal GRPC server to fail job with optional error code which triggers BPMN error execution
func (m *JobManager) FailJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	if m.server == nil {
		return NodeIsNotALeader
	}
	return m.server.failJob(ctx, clientID, jobKey, message, errorCode, variables)
}

func (m *JobManager) OnClusterStateChange(ctx context.Context) {
	state := m.store.ClusterState()
	newPartitionLeaders := map[uint32]string{}
	for id, partition := range state.Partitions {
		newPartitionLeaders[id] = partition.LeaderId
	}
	if maps.Equal(m.currentPartitionRoles, newPartitionLeaders) {
		return
	}
	m.OnPartitionRoleChange(ctx)
	m.currentPartitionRoles = newPartitionLeaders
}

// OnPartitionRoleChange is a callback function called when cluster state changes its partition leaders
func (m *JobManager) OnPartitionRoleChange(ctx context.Context) {
	state := m.store.ClusterState()
	isLeader := false
	leaders := []string{}
	for _, partition := range state.Partitions {
		if partition.LeaderId == m.store.NodeID() {
			isLeader = true
		}
		leaders = append(leaders, partition.LeaderId)
	}
	// if we have to start the server
	if isLeader && m.serverCtx == nil {
		m.serverCtx, m.serverCancel = context.WithCancel(m.ctx)
		m.server = newJobServer(NodeId(m.store.NodeID()), m.loader, m.completer)
		m.server.startServer(m.serverCtx)
	}
	// if we have to stop the server
	if !isLeader && m.serverCtx != nil {
		m.serverCancel()
		m.serverCtx = nil
		m.server = nil
	}
	m.client.updateNodeSubs(ctx, leaders)
}

// OnJobRejected is a server callback function called when client rejects job
func (m *JobManager) OnJobRejected(ctx context.Context, jobKey int64) error {
	if m.server == nil {
		return NodeIsNotALeader
	}
	m.server.onJobRejected(ctx, jobKey)
	return nil
}
