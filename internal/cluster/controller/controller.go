package controller

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/pbinitiative/zenbpm/pkg/script/feel"
	"github.com/pbinitiative/zenbpm/pkg/script/js"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	rstore "github.com/rqlite/rqlite/v10/store"
	"github.com/rqlite/rqlite/v10/tcp"
)

type Controller struct {
	// partitions contains a map of partition nodes on this zen node
	// one zen node will be always working with maximum of one partition node per partition
	partitions              map[uint32]*partition.ZenPartitionNode
	partitionsMu            sync.RWMutex
	store                   ControlledStore
	client                  *client.ClientManager
	Config                  config.Cluster
	persistenceConfig       config.Persistence
	cdcConfig               config.CDC
	mux                     *tcp.Mux
	logger                  hclog.Logger
	clusterChangesMu        sync.RWMutex
	handleClusterChanges    bool
	clusterStateChangeHooks []func(context.Context)
	shutdownCh              chan struct{}
	shutdownOnce            sync.Once
	backgroundWg            sync.WaitGroup
	partitionOps            sync.Map
	retryMu                 sync.Mutex
	retryStopped            bool
	retryScheduled          map[uint32]bool
	retryAttempts           map[uint32]uint
	initializationFailures  map[uint32]uint
	lifecycleCtx            context.Context
	lifecycleCancel         context.CancelFunc
}

const (
	defaultRetryDelay         = 5 * time.Second
	maxRetryDelay             = 15 * time.Minute
	maxInitializationAttempts = 8
)

func NewController(mux *tcp.Mux, conf config.Cluster) (*Controller, error) {
	if conf.PartitionRetryDelay <= 0 {
		conf.PartitionRetryDelay = defaultRetryDelay
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	c := Controller{
		Config:                  conf,
		mux:                     mux,
		partitions:              make(map[uint32]*partition.ZenPartitionNode),
		logger:                  hclog.Default().Named("zen-controller"),
		partitionsMu:            sync.RWMutex{},
		clusterStateChangeHooks: []func(context.Context){},
		shutdownCh:              make(chan struct{}),
		retryScheduled:          make(map[uint32]bool),
		retryAttempts:           make(map[uint32]uint),
		initializationFailures:  make(map[uint32]uint),
		lifecycleCtx:            lifecycleCtx,
		lifecycleCancel:         lifecycleCancel,
	}
	return &c, nil
}

type ControlledStore interface {
	ID() string
	Addr() string
	IsLeader() bool
	Role() proto.Role
	ClusterState() state.Cluster
	WritePartitionChange(change *proto.NodePartitionChange) error
}

// Start will start a controller instance on a given store
func (c *Controller) Start(s ControlledStore, clientMgr *client.ClientManager) error {
	c.store = s
	c.client = clientMgr
	persistenceConfig := c.Config.Persistence

	if c.Config.Persistence.RqLite == nil {
		defaultConfig := partition.GetRqLiteDefaultConfig(c.store.ID(), c.store.Addr(), c.store.ID(), c.Config.Raft.JoinAddresses, c.Config.Raft.BootstrapExpect)
		persistenceConfig.RqLite = &defaultConfig
	}
	if err := c.Config.ValidateCDC(); err != nil {
		return fmt.Errorf("failed to start controller, CDC output validation failed: %w", err)
	}
	cdcConfig := config.CDC{}
	if c.Config.CDC.Enabled {
		cdcConfig = c.Config.CDC
	}
	err := persistenceConfig.RqLite.Validate()
	if err != nil {
		return fmt.Errorf("failed to start controller, rqLite config validation failed: %w", err)
	}
	if cdcConfig.Output != "" && persistenceConfig.RqLite.RaftNonVoter {
		return errors.New("failed to start controller, rqLite config validation failed: CDC cannot be enabled on non-voting nodes")
	}
	c.persistenceConfig = persistenceConfig
	c.cdcConfig = cdcConfig
	c.clusterChangesMu.Lock()
	c.handleClusterChanges = true
	c.clusterChangesMu.Unlock()
	// TODO: start engines for assigned partitions
	c.ClusterStateChangeNotification(c.lifecycleCtx)
	return nil
}

// ClusterStateChangeNotification is called in a goroutine from FSM when changes are applied to the state
func (c *Controller) ClusterStateChangeNotification(ctx context.Context) {
	// Synchronize notifications with Stop so shutdown can wait for in-flight
	// notifications and prevent a later one from starting another partition.
	c.clusterChangesMu.RLock()
	defer c.clusterChangesMu.RUnlock()

	if !c.handleClusterChanges {
		return
	}
	c.logger.Debug("Received cluster state change notification")
	if c.store.IsLeader() {
		c.performLeaderOperations(ctx)
	}
	c.performMemberOperations(ctx)
	for _, hook := range c.clusterStateChangeHooks {
		hook(ctx)
	}
}

func (c *Controller) AddClusterStateChangeHook(f func(context.Context)) {
	c.clusterStateChangeHooks = append(c.clusterStateChangeHooks, f)
}

func (c *Controller) performLeaderOperations(ctx context.Context) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping leader operation checks due to expired context")
		return
	}
	c.logger.Debug("Performing leader operations")
	cs := c.store.ClusterState()

	// verify that the partition count in the cluster is same as desired one.
	// if its not the leader starts to create partitions one by one (each new partition needs to report its leader into the state)
	currentPartitionCount := len(cs.Partitions)
	if int(cs.Config.DesiredPartitions) > currentPartitionCount {
		c.assignNewPartition(ctx, currentPartitionCount+1)
	}
	// check if there is node with no partitions and if there is assign it to partition with least nodes // TODO
	cs = c.store.ClusterState()
	for _, node := range cs.Nodes {
		if len(node.Partitions) == 0 {
			c.assignPartition(ctx, 1, node.Id)
		}
	}
	// TODO:
	// if node is leader he needs to:
	//  - verify that the partitions are spread across the cluster in the desired manner (we dont have spread logic yet)
}

// assignPartition will send a message to store that indicates that a node should start the joining process into a partition cluster
func (c *Controller) assignPartition(ctx context.Context, partitionId uint32, nodeId string) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping assigning of a partition due to expired context")
		return
	}
	c.logger.Info(fmt.Sprintf("Assigning partition %d to %s", partitionId, nodeId))
	err := c.store.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      new(nodeId),
		PartitionId: new(partitionId),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_JOINING.Enum(),
		Role:        proto.Role_ROLE_TYPE_UNKNOWN.Enum(),
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to assignPartition: %s", err))
	}
	if c.logger.IsDebug() {
		c.logger.Debug(fmt.Sprintf("Assigned partition %d to %s", partitionId, nodeId))
	}
}

// assignNewPartition will send a message to store that indicates that a node should start the joining process into a partition cluster
func (c *Controller) assignNewPartition(ctx context.Context, newPartitionId int) {
	cs := c.store.ClusterState()
	c.logger.Debug(fmt.Sprintf("%+v", cs))
	partitionCandidate, err := cs.GetLeastStressedNode()
	if err != nil {
		// we have empty nodes
		return
	}

	// check if partition is not in the process of being created on one of the nodes
	if cs.AnyNodeHasPartition(newPartitionId) {
		c.logger.Debug(fmt.Sprintf("Partition %d already assigned skipping assignment.", newPartitionId))
		return
	}

	if ctx.Err() != nil {
		c.logger.Debug("Skipping assigning of a new partition due to expired context")
		return
	}
	c.assignPartition(ctx, uint32(newPartitionId), partitionCandidate.Id)
}

func (c *Controller) performMemberOperations(ctx context.Context) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping member operation checks due to expired context")
		return
	}
	cs := c.store.ClusterState()
	currentNode, err := cs.GetNode(c.store.ID())
	if err != nil {
		c.logger.Error("Controller encountered a node not yet registered in the cluster.")
		return
	}
	for partitionId, partition := range currentNode.Partitions {
		partitionOp := c.partitionOperationMutex(partitionId)
		partitionOp.Lock()
		c.logger.Debug(fmt.Sprintf("Handling partition %d state %s", partitionId, partition.State))
		switch partition.State {
		case state.NodePartitionStateError:
			c.handlePartitionStateError(partitionId)
		case state.NodePartitionStateJoining:
			c.handlePartitionStateJoining(ctx, partitionId)
		case state.NodePartitionStateInitializing:
			c.partitionsMu.RLock()
			_, running := c.partitions[partitionId]
			c.partitionsMu.RUnlock()
			if running {
				c.handlePartitionStateInitializing(partitionId)
			} else {
				// partition node is not running locally (node restart or a failed
				// start), the join has to be performed again
				c.handlePartitionStateJoining(ctx, partitionId)
			}
		case state.NodePartitionStateInitialized:
			c.handlePartitionStateInitialized(ctx, partitionId)
		case state.NodePartitionStateLeaving:
			c.handlePartitionStateLeaving(ctx, partitionId)
		default:
			c.logger.Error("encountered unexpected partition state, skipping partition",
				"partitionId", partitionId, "state", partition.State)
		}
		partitionOp.Unlock()
	}
	// TODO:
	// regardless of the leader/follower status node needs to:
	//  - check its state in partitions to:
	//    - see if it has newly assigned partition that it needs to join
	//    - see if it has lost some assigned partition that it needs to leave
	//  - check if it is leader of any partition that does not have engine running yet and start it
	//  - check if it lost its leadership of any partition and needs to stop the engine (this should be preceded by previous error logs from the engine not being able to store changes)
}

func (c *Controller) handlePartitionStateJoining(ctx context.Context, partitionID uint32) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping handlePartitionStateJoining due to expired context")
		return
	}
	// check if partition is already assigned and skip
	if node, err := c.store.ClusterState().GetNode(c.store.ID()); err == nil {
		if partition, ok := node.Partitions[partitionID]; ok {
			// if we dont have running partition node we need to initialize it again
			c.partitionsMu.RLock()
			_, runningOk := c.partitions[partitionID]
			c.partitionsMu.RUnlock()
			if runningOk && (partition.State == state.NodePartitionStateInitializing ||
				partition.State == state.NodePartitionStateInitialized) {
				return
			}
		}
	}
	// change the state
	if err := c.reportPartitionState(partitionID, proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING, proto.Role_ROLE_TYPE_UNKNOWN); err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to change partition %d node state to INITIALIZING: %s", partitionID, err))
		c.schedulePartitionRetry(partitionID, "partition-initializing-state-retry")
		return
	}
	partitionConf := c.persistenceConfig
	rqLiteConf := *partitionConf.RqLite
	partitionConf.RqLite = &rqLiteConf
	partitionConf.RqLite.NodeID = fmt.Sprintf("zen-%s-partition-%d", c.store.ID(), partitionID)
	partitionConf.RqLite.DataPath = filepath.Join(c.Config.Raft.Dir, fmt.Sprintf("partition-%d", partitionID))
	partitionNode, err := partition.StartZenPartitionNode(c.lifecycleCtx, c.mux, partitionConf, c.cdcConfig, c.client, partitionID, partition.PartitionChangesCallbacks{
		AddNewNode: func(s raft.Server) error {
			return c.partitionAddNewNode(s, partitionID)
		},
		ShutdownNode: func(s raft.ServerID) error {
			return c.partitionShutdownNode(s, partitionID)
		},
		LeaderChange: func(s raft.ServerID) error {
			return c.partitionLeaderChange(s, partitionID)
		},
		RemoveNode: func(id string) error {
			return c.partitionRemoveNode(id, partitionID)
		},
		ResumeNode: func(id string) error {
			return c.partitionResumeNode(id, partitionID)
		},
	},
		c.store.ClusterState,
	)
	if err != nil {
		c.logger.Error(fmt.Sprintf("Failed to start partition %d node: %s", partitionID, err))
		c.schedulePartitionRetry(partitionID, "partition-join-retry")
		return
	}
	c.partitionsMu.Lock()
	c.partitions[partitionID] = partitionNode
	c.partitionsMu.Unlock()

	c.handlePartitionStateInitializing(partitionID)
}

func (c *Controller) handlePartitionStateInitializing(partitionID uint32) {
	if c.lifecycleCtx.Err() != nil {
		return
	}
	c.partitionsMu.RLock()
	partitionNode, ok := c.partitions[partitionID]
	c.partitionsMu.RUnlock()
	if !ok {
		c.schedulePartitionRetry(partitionID, "partition-join-retry")
		return
	}
	_, err := c.waitForPartitionLeader(c.lifecycleCtx, partitionNode, time.Minute)
	if errors.Is(err, rstore.ErrWaitForLeaderTimeout) {
		c.logger.Info(fmt.Sprintf("Timeout waiting for leader of partition %d.", partitionID))
		c.schedulePartitionRetry(partitionID, "partition-leader-retry")
		return
	}
	if err != nil {
		c.logger.Debug(fmt.Sprintf("Stopped waiting for leader of partition %d: %s", partitionID, err))
		return
	}
	if c.lifecycleCtx.Err() != nil {
		c.logger.Debug(fmt.Sprintf("Skipping handlePartitionStateInitializing due to expired context: %s", c.lifecycleCtx.Err()))
		return
	}

	isLeader := partitionNode.IsLeader(c.lifecycleCtx)
	if partitionNode.FeelRuntime == nil && isLeader {
		c.partitionsMu.Lock()
		partitionNode.FeelRuntime = feel.NewFeelinRuntime(c.Config.Script.Feel.MaxVmPoolSize, c.Config.Script.Feel.MinVmPoolSize)
		c.partitionsMu.Unlock()
	}

	if partitionNode.JsRuntime == nil && isLeader {
		c.partitionsMu.Lock()
		partitionNode.JsRuntime = js.NewJsRuntime(c.Config.Script.Js.MaxVmPoolSize, c.Config.Script.Js.MinVmPoolSize)
		c.partitionsMu.Unlock()
	}

	if isLeader && partitionNode.Engine == nil {
		if err := c.startPartitionEngine(c.lifecycleCtx, partitionID, partitionNode); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to initialize engine for partition %d, keeping partition in state %s", partitionID, state.NodePartitionStateInitializing), "err", err)
			c.recordInitializationFailure(partitionID, proto.Role_ROLE_TYPE_LEADER)
			return
		}
	}
	if currentIsLeader := partitionNode.IsLeader(c.lifecycleCtx); currentIsLeader != isLeader {
		if !currentIsLeader {
			c.partitionsMu.Lock()
			engine := partitionNode.Engine
			partitionNode.Engine = nil
			c.partitionsMu.Unlock()
			if engine != nil {
				engine.Stop()
			}
		}
		c.schedulePartitionRetry(partitionID, "partition-role-change-retry")
		return
	}
	if !isLeader && !partitionLeaderInitialized(c.store.ClusterState(), partitionID) {
		c.schedulePartitionPoll(partitionID, "partition-leader-initialization-poll")
		return
	}
	if !isLeader {
		ready, schemaErr := partitionNode.DB.SchemaReady(c.lifecycleCtx)
		if schemaErr != nil || !ready {
			c.logger.Debug("Follower schema is not ready", "partitionId", partitionID, "err", schemaErr)
			c.schedulePartitionPoll(partitionID, "partition-follower-schema-poll")
			return
		}
	}
	role := proto.Role_ROLE_TYPE_FOLLOWER
	if isLeader {
		role = proto.Role_ROLE_TYPE_LEADER
	}
	if err := c.reportPartitionState(partitionID, proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED, role); err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to update state of the partition to INITIALIZED %d: %s", partitionID, err))
		c.schedulePartitionRetry(partitionID, "partition-initialized-state-retry")
		return
	}
	c.resetPartitionRetry(partitionID)
}

func (c *Controller) handlePartitionStateError(partitionID uint32) {
	c.retryMu.Lock()
	failedInThisProcess := c.initializationFailures[partitionID] > 0
	c.retryMu.Unlock()
	if failedInThisProcess {
		return
	}
	if err := c.reportPartitionState(partitionID, proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING, proto.Role_ROLE_TYPE_UNKNOWN); err != nil {
		c.logger.Warn("Failed to retry errored partition after restart", "partitionId", partitionID, "err", err)
		c.schedulePartitionRetry(partitionID, "partition-error-recovery-retry")
	}
}

func (c *Controller) handlePartitionStateInitialized(ctx context.Context, partitionID uint32) {
	c.partitionsMu.RLock()
	partitionNode, ok := c.partitions[partitionID]
	c.partitionsMu.RUnlock()
	if !ok {
		// we restarted the node and it needs to re-initialize its partition state
		c.handlePartitionStateJoining(ctx, partitionID)
		return
	}
	isLeader := partitionNode.IsLeader(ctx)
	if !isLeader && partitionNode.Engine != nil {
		c.partitionsMu.Lock()
		engine := partitionNode.Engine
		partitionNode.Engine = nil
		c.partitionsMu.Unlock()
		engine.Stop()
		c.logger.Info("Stopped engine after partition leadership loss", "partitionId", partitionID)
		return
	}
	if isLeader && partitionNode.Engine == nil {
		if err := c.reportPartitionState(partitionID, proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING, proto.Role_ROLE_TYPE_LEADER); err != nil {
			c.logger.Warn("Failed to reinitialize newly elected partition leader", "partitionId", partitionID, "err", err)
			c.schedulePartitionRetry(partitionID, "partition-leader-engine-retry")
		}
	}
}

// startPartitionEngine runs the database schema migrations and starts the BPMN
// engine for the partition. The partition must not be reported as INITIALIZED
// (and the node must not report readiness) until this completes successfully.
func (c *Controller) startPartitionEngine(ctx context.Context, partitionID uint32, partitionNode *partition.ZenPartitionNode) error {
	engine, err := c.createEngine(ctx, partitionNode.DB, partitionNode.FeelRuntime, partitionNode.JsRuntime)
	if err != nil {
		return fmt.Errorf("failed to create engine for partition %d: %w", partitionID, err)
	}
	if err := engine.Start(ctx); err != nil {
		engine.Stop()
		return fmt.Errorf("failed to start engine for partition %d: %w", partitionID, err)
	}
	c.partitionsMu.Lock()
	partitionNode.Engine = engine
	c.partitionsMu.Unlock()
	c.logger.Info(fmt.Sprintf("Started engine for partition %d", partitionID))
	return nil
}

func partitionLeaderInitialized(clusterState state.Cluster, partitionID uint32) bool {
	return clusterState.PartitionLeaderInitialized(partitionID)
}

func (c *Controller) schedulePartitionRetry(partitionID uint32, name string) {
	c.schedulePartitionRetryAfter(partitionID, name, true)
}

// schedulePartitionPoll retries a healthy wait condition at a fixed cadence.
// Waiting for the leader to initialize or for replicated schema state to arrive
// must not push a follower into the exponential failure backoff.
func (c *Controller) schedulePartitionPoll(partitionID uint32, name string) {
	c.schedulePartitionRetryAfter(partitionID, name, false)
}

func (c *Controller) schedulePartitionRetryAfter(partitionID uint32, name string, backoff bool) {
	c.retryMu.Lock()
	if c.retryStopped || c.lifecycleCtx.Err() != nil || c.retryScheduled[partitionID] {
		c.retryMu.Unlock()
		return
	}
	c.retryScheduled[partitionID] = true
	delay := c.Config.PartitionRetryDelay
	if backoff {
		c.retryAttempts[partitionID]++
		attempt := c.retryAttempts[partitionID]
		for i := uint(1); i < attempt && delay < maxRetryDelay; i++ {
			delay *= 2
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}
		}
	} else {
		delete(c.retryAttempts, partitionID)
	}
	c.backgroundWg.Add(1)
	c.retryMu.Unlock()

	safego.Go(name, c.logger, func() {
		defer c.backgroundWg.Done()
		retryTimer := time.NewTimer(delay)
		defer retryTimer.Stop()
		select {
		case <-retryTimer.C:
		case <-c.shutdownCh:
			return
		}
		c.retryMu.Lock()
		c.retryScheduled[partitionID] = false
		c.retryMu.Unlock()
		c.ClusterStateChangeNotification(c.lifecycleCtx)
	})
}

func (c *Controller) resetPartitionRetry(partitionID uint32) {
	c.retryMu.Lock()
	delete(c.retryAttempts, partitionID)
	delete(c.initializationFailures, partitionID)
	c.retryMu.Unlock()
}

func (c *Controller) recordInitializationFailure(partitionID uint32, role proto.Role) {
	c.retryMu.Lock()
	c.initializationFailures[partitionID]++
	failures := c.initializationFailures[partitionID]
	c.retryMu.Unlock()
	if failures < maxInitializationAttempts {
		c.schedulePartitionRetry(partitionID, "partition-initializing-retry")
		return
	}
	if err := c.reportPartitionState(partitionID, proto.NodePartitionState_NODE_PARTITION_STATE_ERROR, role); err != nil {
		c.logger.Warn("Failed to mark persistently broken partition as ERROR", "partitionId", partitionID, "err", err)
		c.schedulePartitionRetry(partitionID, "partition-error-state-retry")
	}
}

func (c *Controller) partitionOperationMutex(partitionID uint32) *sync.Mutex {
	mutex, _ := c.partitionOps.LoadOrStore(partitionID, &sync.Mutex{})
	return mutex.(*sync.Mutex)
}

func (c *Controller) reportPartitionState(partitionID uint32, partitionState proto.NodePartitionState, role proto.Role) error {
	leaderClient, err := c.client.ClusterLeader()
	if err != nil {
		return fmt.Errorf("failed to get cluster leader client: %w", err)
	}
	ctxClient, cancel := context.WithTimeout(c.lifecycleCtx, 5*time.Second)
	defer cancel()
	_, err = leaderClient.NodeCommand(ctxClient, &proto.Command{
		Type: proto.Command_TYPE_NODE_PARTITION_CHANGE.Enum(),
		Request: &proto.Command_NodePartitionChange{NodePartitionChange: &proto.NodePartitionChange{
			NodeId: new(c.store.ID()), PartitionId: new(partitionID), State: partitionState.Enum(), Role: role.Enum(),
		}},
	})
	if err != nil {
		return fmt.Errorf("failed to report partition state: %w", err)
	}
	return nil
}

func (c *Controller) waitForPartitionLeader(ctx context.Context, partitionNode *partition.ZenPartitionNode, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", rstore.ErrWaitForLeaderTimeout
		}
		wait := min(remaining, time.Second)
		leaderID, err := partitionNode.WaitForLeader(wait)
		if err == nil {
			return leaderID, nil
		}
		if !errors.Is(err, rstore.ErrWaitForLeaderTimeout) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-c.lifecycleCtx.Done():
			return "", c.lifecycleCtx.Err()
		default:
		}
	}
}

func (c *Controller) createEngine(ctx context.Context, db *partition.DB, feelRuntime script.FeelRuntime, jsRuntime script.JsRuntime) (*bpmn.Engine, error) {
	err := db.RunMigrations(ctx)
	if err != nil {
		c.logger.Error("Failed to execute migrations", "err", err)
		return nil, err
	}

	c.logger.Info(fmt.Sprintf("Engine created for partition %d", db.Partition))
	return new(bpmn.NewEngine(
		bpmn.EngineWithStorageAndFeel(db, feelRuntime),
		bpmn.EngineWithJs(jsRuntime),
		bpmn.EngineWithMaxProcessInstanceNestingDepth(c.Config.Engine.MaxProcessInstanceNestingDepth),
		bpmn.EngineWithMaxProcessInstanceFlowNodeCount(c.Config.Engine.MaxProcessInstanceFlowNodeCount),
		bpmn.EngineWithDefinitionSubscriptionRecoveryFilter(func(definition bpmnruntime.ProcessDefinition) bool {
			return db.Partition == definitionSubscriptionPartition(c.store.ClusterState(), definition.BpmnProcessId)
		}),
	)), nil
}

func definitionSubscriptionPartition(clusterState state.Cluster, processId string) uint32 {
	partitionIds := make([]uint32, 0, len(clusterState.Partitions))
	for partitionId := range clusterState.Partitions {
		partitionIds = append(partitionIds, partitionId)
	}
	slices.Sort(partitionIds)
	if len(partitionIds) == 0 {
		return 0
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(processId))
	return partitionIds[int(hash.Sum32()%uint32(len(partitionIds)))]
}

func (c *Controller) handlePartitionStateLeaving(ctx context.Context, partitionId uint32) {
	c.partitionsMu.Lock()
	toLeave, ok := c.partitions[partitionId]
	if ok {
		delete(c.partitions, partitionId)
	}
	c.partitionsMu.Unlock()
	if !ok {
		c.logger.Warn(fmt.Sprintf("Failed to find partition to leave: %d", partitionId))
		return
	}
	if err := toLeave.Stop(); err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to stop partition %d: %s", partitionId, err))
	}
	// TODO: verify that partition leader removes the node from the state after it gets removed from the cluster
}

func parsePartitionServerID(serverID raft.ServerID) (nodeID string, partitionID uint32, err error) {
	s := string(serverID)
	const prefix = "zen-"
	const sep = "-partition-"

	if !strings.HasPrefix(s, prefix) {
		return "", 0, fmt.Errorf("invalid partition server ID %q: missing %q prefix", s, prefix)
	}

	sepIdx := strings.LastIndex(s, sep)
	if sepIdx < 0 {
		return "", 0, fmt.Errorf("invalid partition server ID %q: missing %q separator", s, sep)
	}

	nodeID = s[len(prefix):sepIdx]
	if nodeID == "" {
		return "", 0, fmt.Errorf("invalid partition server ID %q: empty node ID", s)
	}

	partNum, err := strconv.ParseUint(s[sepIdx+len(sep):], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("invalid partition server ID %q: %w", s, err)
	}

	return nodeID, uint32(partNum), nil
}

func (c *Controller) partitionResumeNode(id string, partitionId uint32) error {
	nodeId, _, err := parsePartitionServerID(raft.ServerID(id))
	if err != nil {
		c.logger.Warn(fmt.Sprintf("partition %d: resume-node observation had unparseable server ID %q: %s", partitionId, id, err))
		return nil
	}
	c.logger.Info(fmt.Sprintf("Partition %d: node %s resumed", partitionId, nodeId))
	// TODO(phase 4): re-mark node partition as active after heartbeat resumed.
	return nil
}

func (c *Controller) partitionRemoveNode(id string, partitionId uint32) error {
	nodeId, _, err := parsePartitionServerID(raft.ServerID(id))
	if err != nil {
		c.logger.Warn(fmt.Sprintf("partition %d: remove-node observation had unparseable server ID %q: %s", partitionId, id, err))
		return nil
	}
	c.logger.Info(fmt.Sprintf("Partition %d: node %s removed (reap timeout)", partitionId, nodeId))
	// TODO(phase 4): write NodePartitionChange{State=LEAVING} to base cluster leader.
	return nil
}

func (c *Controller) partitionLeaderChange(s raft.ServerID, partitionId uint32) error {
	// Raft emits a leader observation with an empty ID when the partition
	// becomes leaderless. There is nothing to resolve or report in that case.
	if s == "" {
		c.logger.Debug("Partition is currently leaderless", "partitionId", partitionId)
		return nil
	}
	var nodeID string
	for id := range c.store.ClusterState().Nodes {
		if fmt.Sprintf("zen-%s-partition-%d", id, partitionId) == string(s) {
			nodeID = id
			break
		}
	}
	if nodeID == "" {
		return fmt.Errorf("failed to resolve partition node %q in cluster state", s)
	}
	if nodeID != c.store.ID() {
		return nil
	}
	err := c.reportPartitionState(partitionId, proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING, proto.Role_ROLE_TYPE_LEADER)
	if err != nil {
		c.schedulePartitionRetry(partitionId, "partition-leader-change-retry")
		return fmt.Errorf("failed to publish new leader of partition %d: %w", partitionId, err)
	}
	return nil
}

func (c *Controller) partitionShutdownNode(s raft.ServerID, partitionId uint32) error {
	nodeId, _, err := parsePartitionServerID(s)
	if err != nil {
		c.logger.Warn(fmt.Sprintf("partition %d: shutdown-node observation had unparseable server ID %q: %s", partitionId, s, err))
		return nil
	}
	c.logger.Info(fmt.Sprintf("Partition %d: node %s shutdown detected", partitionId, nodeId))
	// TODO(phase 4): mark partition slot as unavailable, trigger reassignment if quorum affected.
	return nil
}

func (c *Controller) partitionAddNewNode(s raft.Server, partitionId uint32) error {
	nodeId, _, err := parsePartitionServerID(s.ID)
	if err != nil {
		// Log and continue — the observer goroutine discards errors and there's no upstream retry path.
		c.logger.Warn(fmt.Sprintf("partition %d: add-node observation had unparseable server ID %q: %s", partitionId, s.ID, err))
		return nil
	}
	c.logger.Info(fmt.Sprintf("Partition %d: node %s joined", partitionId, nodeId))
	// TODO(phase 4): propagate NodePartitionChange{State=JOINING} to base cluster leader.
	return nil
}

// GetPartitions returns a snapshot of partition nodes hosted on this zen node.
func (c *Controller) GetPartitions() map[uint32]*partition.ZenPartitionNode {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	out := make(map[uint32]*partition.ZenPartitionNode, len(c.partitions))
	for id, p := range c.partitions {
		out[id] = p
	}
	return out
}

func (c *Controller) Stop() error {
	c.lifecycleCancel()
	c.shutdownOnce.Do(func() {
		close(c.shutdownCh)
	})
	// The store keeps dispatching cluster state change notifications until it is
	// closed. Wait for any in-flight notification and prevent subsequent ones
	// from starting another partition after the map has been drained.
	c.clusterChangesMu.Lock()
	c.handleClusterChanges = false

	// Take the partitions out of the map under the lock and stop them outside of
	// both controller locks. The handlers can no longer find them and partition
	// Stop is idempotent.
	c.partitionsMu.Lock()
	partitions := make([]*partition.ZenPartitionNode, 0, len(c.partitions))
	for partitionID, partitionNode := range c.partitions {
		partitions = append(partitions, partitionNode)
		delete(c.partitions, partitionID)
	}
	c.partitionsMu.Unlock()
	c.clusterChangesMu.Unlock()

	// Retry registration checks retryStopped and increments the WaitGroup while
	// holding retryMu. Setting the flag under the same mutex ensures no Add can
	// race with Wait, including retries requested by partition observers.
	c.retryMu.Lock()
	c.retryStopped = true
	c.retryMu.Unlock()
	c.backgroundWg.Wait()

	var joinErr error
	for _, partitionNode := range partitions {
		err := partitionNode.Stop()
		if err != nil {
			joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop partition %d: %w", partitionNode.PartitionId, err))
		}
	}
	return joinErr
}

// NotifyShutdown notifies the cluster leader / store that the node is shutting down
func (c *Controller) NotifyShutdown() error {
	// TODO: call zen cluster api on a leader to notify about node shutdown
	return nil
}

func (c *Controller) IsPartitionLeader(ctx context.Context, partition uint32) bool {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	p, ok := c.partitions[partition]
	if !ok {
		return false
	}
	return p.IsLeader(ctx)
}

func (c *Controller) IsAnyPartitionLeader(ctx context.Context) bool {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	for _, partitionNode := range c.partitions {
		if partitionNode.IsLeader(ctx) {
			return true
		}
	}
	return false
}

func (c *Controller) PartitionEngine(ctx context.Context, partition uint32) *bpmn.Engine {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	partitionNode, ok := c.partitions[partition]
	if !ok || !partitionNode.IsLeader(ctx) || !localPartitionInitialized(c.store.ClusterState(), c.store.ID(), partition) {
		return nil
	}
	return partitionNode.Engine
}

func (c *Controller) Engines(ctx context.Context) map[uint32]*bpmn.Engine {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	res := make(map[uint32]*bpmn.Engine, 0)
	clusterState := c.store.ClusterState()
	for partition, partitionNode := range c.partitions {
		if partitionNode.Engine != nil && partitionNode.IsLeader(ctx) && localPartitionInitialized(clusterState, c.store.ID(), partition) {
			res[partition] = partitionNode.Engine
		}
	}
	return res
}

func (c *Controller) AllPartitionLeaderDBs(ctx context.Context) []*partition.DB {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	clusterState := c.store.ClusterState()
	leaderQueries := make([]*partition.DB, 0)
	for partitionID, partitionNode := range c.partitions {
		if !partitionNode.IsLeader(ctx) || !localPartitionInitialized(clusterState, c.store.ID(), partitionID) {
			continue
		}
		leaderQueries = append(leaderQueries, partitionNode.DB)
	}
	return leaderQueries
}

func (c *Controller) PartitionQueries(ctx context.Context, partitionId uint32) *sql.Queries {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	partitionNode, ok := c.partitions[partitionId]
	if !ok || !localPartitionInitialized(c.store.ClusterState(), c.store.ID(), partitionId) {
		return nil
	}
	return partitionNode.DB.Queries
}

func (c *Controller) GetPartition(ctx context.Context, partitionId uint32) *partition.ZenPartitionNode {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	partitionNode := c.partitions[partitionId]
	return partitionNode
}

// GetReadOnlyDB returns a database object preferably on partition where node is a follower
// mostly used for resources spread across all partitions
func (c *Controller) GetReadOnlyDB(ctx context.Context) (*partition.DB, error) {
	c.partitionsMu.RLock()
	defer c.partitionsMu.RUnlock()
	clusterState := c.store.ClusterState()
	for partitionID, node := range c.partitions {
		if localPartitionInitialized(clusterState, c.store.ID(), partitionID) && !node.IsLeader(ctx) {
			return node.DB, nil
		}
	}
	for partitionID, node := range c.partitions {
		if localPartitionInitialized(clusterState, c.store.ID(), partitionID) {
			return node.DB, nil
		}
	}
	return nil, fmt.Errorf("no partition available to get read only database")
}

func localPartitionInitialized(clusterState state.Cluster, nodeID string, partitionID uint32) bool {
	node, err := clusterState.GetNode(nodeID)
	if err != nil {
		return false
	}
	partitionState, ok := node.Partitions[partitionID]
	return ok && partitionState.State == state.NodePartitionStateInitialized
}
