package jobmanager

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"google.golang.org/grpc"
)

type Store interface {
	ClusterState() store.ClusterState
	NodeID() string
}

type ClientId string
type JobType string

type NodeId string
type nodeSubs struct {
	nodeId   string
	jobTypes []JobType
}

type nodeStream struct {
	stream      grpc.BidiStreamingClient[proto.SubscribeJobRequest, proto.InternalJob]
	nodeId      string
	partitionId uint32
	jobTypes    []JobType
}

// JobManager handles job distribution in the system.
// When external application makes a call to the public API, the API registers
// the clientId and jobType.
// JobManager serves as a client subscribing to the partition leader nodes for required jobTypes
// and at the same time as a server distributing created jobs by the engine among subscribers.
type JobManager struct {
	client *jobClient
	server *jobServer
	store  Store
}

type Job struct {
	Key         int64
	InstanceKey int64
	Variables   []byte
	Type        JobType
	State       int64
	ElementId   string
	CreatedAt   int64
}

func NewJobManager(
	ctx context.Context,
	store Store,
	clientManager *client.ClientManager,
	loader JobLoader,
	completer JobCompleter,
) *JobManager {
	return &JobManager{
		client: newJobClient(ctx, store, clientManager),
		server: newJobServer(ctx, NodeId(store.NodeID()), loader, completer),
		store:  store,
	}
}

func (m *JobManager) Start() {
	m.server.startServer()
	m.client.startClient()
}

func (m *JobManager) AddJob(ctx context.Context, job runtime.Job) {
}

func (m *JobManager) AddClient(ctx context.Context, clientId ClientId) error {
	return m.client.addClient(ctx, clientId)
}

func (m *JobManager) RemoveClient(ctx context.Context, clientId ClientId) {
	m.client.removeClient(ctx, clientId)
}

func (m *JobManager) AddClientJobSub(ctx context.Context, clientId ClientId, jobType JobType) {
	m.client.addJobSub(ctx, clientId, jobType)
}

func (m *JobManager) RemoveClientJobSub(ctx context.Context, clientId ClientId, jobType JobType) {
	m.client.removeJobSub(ctx, clientId, jobType)
}

// OnPartitionRoleChange is a callback function called when cluster state changes its partition leaders
func (m *JobManager) OnPartitionRoleChange(ctx context.Context) {
	// TODO: there should be a mechanism that handles changes in cluster state
	clusterState := m.store.ClusterState()
}

// OnJobRejected is a server callback function called when client rejects job
func (m *JobManager) OnJobRejected(ctx context.Context, jobKey int64) {
	// TODO: there should be a mechanism that handles changes in cluster state
	m.server.onJobRejected(ctx, jobKey)
}
