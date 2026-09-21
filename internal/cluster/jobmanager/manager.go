package jobmanager

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"google.golang.org/grpc"
)

var (
	NodeIsNotALeader error = fmt.Errorf("Node is not a leader")
)

type Store interface {
	ClusterState() state.Cluster
	NodeID() string
}

type ClientID string
type JobType string

type NodeId string

type partitionRoleState struct {
	nodeId string
	state  state.NodePartitionState
}

// JobManager handles job distribution in the system.
// When external application makes a call to the public API, the API registers
// the clientId and jobType.
// JobManager serves as a client subscribing to the partition leader nodes for required jobTypes
// and at the same time as a server distributing created jobs by the engine among subscribers.
type JobManager struct {
	ctx    context.Context
	client *jobClient
	// server is set while this node leads at least one partition and nil
	// otherwise. Readers on the request path never take roleChangeMu, so the
	// pointer is swapped atomically rather than guarded by it.
	server       atomic.Pointer[jobServer]
	store        Store
	logger       hclog.Logger
	roleChangeMu *sync.Mutex

	currentPartitionRoles map[uint32]partitionRoleState

	// server needs its own context because we might cancel it on leader changes
	serverCtx    context.Context
	serverCancel context.CancelFunc
	loader       JobLoader
	completer    JobCompleter
	limits       LockLimits
	// started guards cluster state driven reconciliation until the job server and
	// client are running.
	started atomic.Bool
}

// Option configures a JobManager.
type Option func(*JobManager)

// LockLimitsFromConfig converts the configuration section into the limits the
// job server applies. A field left at zero takes the engine default: a
// configuration read from a file or the environment never has one, its
// validation rejects zero, but one built as a struct literal, as tests do,
// leaves out what it does not care about, and a zero active-job cap would
// stop every delivery.
func LockLimitsFromConfig(conf config.JobManager) LockLimits {
	return LockLimits{
		DefaultLockDuration:  DurationFromMillis(conf.DefaultLockDurationMs),
		MaxLockDuration:      DurationFromMillis(conf.MaxLockDurationMs),
		DefaultMaxActiveJobs: conf.DefaultMaxActiveJobs,
		MaxActiveJobsCap:     conf.MaxActiveJobsCap,
	}.withDefaults()
}

// withDefaults returns the limits with every zero field replaced by its default.
func (l LockLimits) withDefaults() LockLimits {
	defaults := DefaultLockLimits()
	if l.DefaultLockDuration <= 0 {
		l.DefaultLockDuration = defaults.DefaultLockDuration
	}
	if l.MaxLockDuration <= 0 {
		l.MaxLockDuration = defaults.MaxLockDuration
	}
	if l.DefaultMaxActiveJobs <= 0 {
		l.DefaultMaxActiveJobs = defaults.DefaultMaxActiveJobs
	}
	if l.MaxActiveJobsCap <= 0 {
		l.MaxActiveJobsCap = defaults.MaxActiveJobsCap
	}
	return l
}

// WithLockLimits sets the defaults and caps applied to job stream subscriptions.
func WithLockLimits(limits LockLimits) Option {
	return func(m *JobManager) {
		m.limits = limits
	}
}

type Job struct {
	Key            int64
	InstanceKey    int64
	InputVariables []byte
	Type           JobType
	State          int64
	ElementID      string
	CreatedAt      int64
	ElementType    string
	ClientID       ClientID
	// LockUntil is the unix millisecond on the leader's clock at which the
	// lock of this delivery lapses.
	LockUntil int64
}

func New(
	ctx context.Context,
	store Store,
	clientManager *client.ClientManager,
	loader JobLoader,
	completer JobCompleter,
	opts ...Option,
) *JobManager {
	manager := &JobManager{
		ctx:          ctx,
		client:       newJobClient(ctx, NodeId(store.NodeID()), store, clientManager),
		store:        store,
		loader:       loader,
		completer:    completer,
		limits:       DefaultLockLimits(),
		roleChangeMu: &sync.Mutex{},
		logger:       hclog.Default().Named("job-manger"),
	}
	for _, opt := range opts {
		opt(manager)
	}
	return manager
}

func (m *JobManager) Start() {
	err := registerMetrics()
	if err != nil {
		hclog.Default().Error("Failed to register metrics", "err", err)
	}
	m.serverCtx, m.serverCancel = context.WithCancel(m.ctx)
	server := newJobServer(NodeId(m.store.NodeID()), m.loader, m.completer, m.limits)
	server.startServer(m.serverCtx)
	m.server.Store(server)
	m.client.startClient()
	m.started.Store(true)
}

func (m *JobManager) AddClient(ctx context.Context, clientId ClientID, clientRcv chan Job) error {
	return m.client.addClient(ctx, clientId, clientRcv)
}

func (m *JobManager) RemoveClient(ctx context.Context, clientId ClientID) {
	m.client.removeClient(ctx, clientId)
}

// AddClientJobSub subscribes the client to a job type. Zero values in settings
// mean the engine's defaults.
func (m *JobManager) AddClientJobSub(ctx context.Context, clientID ClientID, jobType JobType, settings SubscriptionSettings) error {
	return m.client.addJobSub(ctx, clientID, jobType, settings)
}

func (m *JobManager) RemoveClientJobSub(ctx context.Context, clientID ClientID, jobType JobType) error {
	return m.client.removeJobSub(ctx, clientID, jobType)
}

func (m *JobManager) AddNodeSubscription(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	server := m.server.Load()
	if server == nil {
		return NodeIsNotALeader
	}
	return server.addNodeSubscription(stream)
}

// CompleteJobReq is called by a client to request job completion
func (m *JobManager) CompleteJobReq(ctx context.Context, clientId ClientID, jobKey int64, variables map[string]any) error {
	return m.client.completeJob(ctx, clientId, jobKey, variables)
}

// CompleteJob is called by internal GRPC server to finish job completion
func (m *JobManager) CompleteJob(ctx context.Context, clientId ClientID, jobKey int64, variables map[string]any) error {
	server := m.server.Load()
	if server == nil {
		return NodeIsNotALeader
	}
	return server.completeJob(ctx, clientId, jobKey, variables)
}

// ExtendJobLockReq is called by a client to move the lock deadline of a job it
// holds to now plus duration (zero: the subscription's lock duration). It
// returns the new deadline on the leader's clock.
func (m *JobManager) ExtendJobLockReq(ctx context.Context, clientID ClientID, jobKey int64, duration time.Duration) (time.Time, error) {
	return m.client.extendLock(ctx, clientID, jobKey, duration)
}

// ExtendJobLock is called by the internal GRPC server on the partition leader
// to move the lock deadline of a distributed job. The extension is an
// in-memory operation, so the context is not needed.
func (m *JobManager) ExtendJobLock(_ context.Context, clientID ClientID, jobKey int64, duration time.Duration) (time.Time, error) {
	server := m.server.Load()
	if server == nil {
		return time.Time{}, NodeIsNotALeader
	}
	return server.extendLock(clientID, jobKey, duration)
}

// FailJobReq is called by a client to request job failure
func (m *JobManager) FailJobReq(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	return m.client.failJob(ctx, clientID, jobKey, message, errorCode, variables)
}

// FailJob is called by internal GRPC server to fail job with optional error code which triggers BPMN error execution
func (m *JobManager) FailJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	server := m.server.Load()
	if server == nil {
		return NodeIsNotALeader
	}
	return server.failJob(ctx, clientID, jobKey, message, errorCode, variables)
}

func (m *JobManager) OnClusterStateChange(_ context.Context) {
	state := m.store.ClusterState()
	newPartitionLeaders := map[uint32]partitionRoleState{}
	for id, partition := range state.Partitions {
		leaderNode := state.Nodes[partition.LeaderId]
		partitionLeaderState := leaderNode.Partitions[partition.Id]
		newPartitionLeaders[id] = partitionRoleState{
			nodeId: partition.LeaderId,
			state:  partitionLeaderState.State,
		}
	}
	if maps.Equal(m.currentPartitionRoles, newPartitionLeaders) {
		return
	}
	m.currentPartitionRoles = newPartitionLeaders
	// Partition leaders are registered in the cluster state asynchronously, so a
	// partition can become available after the manager has been started. Without
	// this reconciliation the node would never open a job stream to that
	// partition leader and no job would ever be distributed to the workers.
	if m.started.Load() {
		m.client.reconcileNodeSubscriptions()
	}
}

// OnPartitionRoleChange is a callback function called when cluster state changes its partition leaders
func (m *JobManager) OnPartitionRoleChange(_ context.Context) {
	m.roleChangeMu.Lock()
	defer m.roleChangeMu.Unlock()
	s := m.store.ClusterState()
	isLeader := false
	for _, partition := range s.Partitions {
		partitionLeader := partition.LeaderId
		partitionNode, ok := s.Nodes[partitionLeader]
		if !ok {
			continue
		}
		// if partition is not initialized yet skip it for now
		if partitionNode.Partitions[partition.Id].State != state.NodePartitionStateInitialized {
			continue
		}
		if partition.LeaderId == m.store.NodeID() {
			isLeader = true
		}
	}
	// if we have to start the server
	if isLeader && m.serverCtx == nil {
		m.serverCtx, m.serverCancel = context.WithCancel(m.ctx)
		server := newJobServer(NodeId(m.store.NodeID()), m.loader, m.completer, m.limits)
		server.startServer(m.serverCtx)
		m.server.Store(server)
	}
	// if we have to stop the server
	if !isLeader && m.serverCtx != nil {
		m.logger.Info("Stopping server...lost leader status")
		// unpublish first: a node stream reconnecting on the close message
		// the stopping server sends must not be handed to that server
		m.server.Store(nil)
		m.serverCancel()
		m.serverCtx = nil
	}
	m.client.reconcileNodeSubscriptions()
}

// OnJobRejected is a server callback function called when client rejects job
func (m *JobManager) OnJobRejected(ctx context.Context, jobKey int64) error {
	server := m.server.Load()
	if server == nil {
		return NodeIsNotALeader
	}
	server.onJobRejected(ctx, jobKey)
	return nil
}
