package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	rqproto "github.com/rqlite/rqlite/v8/command/proto"
	rstore "github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
)

type controller struct {
	// partitions contains a map of partition nodes on this zen node
	// one zen node will be always working with maximum of one partition node per partition
	partitions           map[uint32]*ZenPartitionNode
	partitionsMu         sync.RWMutex
	store                ControlledStore
	client               *client.ClientManager
	config               config.Cluster
	persistenceConfig    config.Persistence
	mux                  *tcp.Mux
	logger               hclog.Logger
	handleClusterChanges bool
	jobManager           *jobmanager.JobManager
}

func NewController(mux *tcp.Mux, conf config.Cluster) (*controller, error) {
	c := controller{
		config:       conf,
		mux:          mux,
		partitions:   make(map[uint32]*ZenPartitionNode),
		logger:       hclog.Default().Named("zen-controller"),
		partitionsMu: sync.RWMutex{},
	}
	return &c, nil
}

type ControlledStore interface {
	ID() string
	Addr() string
	IsLeader() bool
	Role() proto.Role
	ClusterState() store.ClusterState
	WritePartitionChange(change *proto.NodePartitionChange) error
}

// Start will start a controller instance on a given store
func (c *controller) Start(s ControlledStore, clientMgr *client.ClientManager) error {
	c.store = s
	c.client = clientMgr
	persistenceConfig := c.config.Persistence

	if c.config.Persistence.RqLite == nil {
		defaultConfig := GetRqLiteDefaultConfig(c.store.ID(), c.store.Addr(), c.store.ID(), c.config.Raft.JoinAddresses)
		persistenceConfig.RqLite = &defaultConfig
	}
	err := persistenceConfig.RqLite.Validate()
	if err != nil {
		return fmt.Errorf("failed to start controller, rqLite config validation failed: %w", err)
	}
	c.persistenceConfig = persistenceConfig
	c.handleClusterChanges = true
	// TODO: start engines for assigned partitions
	c.ClusterStateChangeNotification(context.Background())
	return nil
}

// ClusterStateChangeNotification is called in a goroutine from FSM when changes are applied to the state
func (c *controller) ClusterStateChangeNotification(ctx context.Context) {
	if !c.handleClusterChanges {
		return
	}
	c.logger.Debug("Received cluster state change notification")
	if c.store.IsLeader() {
		c.performLeaderOperations(ctx)
	}
	c.performMemberOperations(ctx)
	if c.jobManager == nil {
		return
	}
	c.jobManager.OnPartitionRoleChange(ctx)
}

func (c *controller) SetJobManager(jm *jobmanager.JobManager) {
	c.jobManager = jm
}

func (c *controller) performLeaderOperations(ctx context.Context) {
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
func (c *controller) assignPartition(ctx context.Context, partitionId uint32, nodeId string) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping assigning of a partition due to expired context")
		return
	}
	c.logger.Info(fmt.Sprintf("Assigning partition %d to %s", partitionId, nodeId))
	err := c.store.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      nodeId,
		PartitionId: partitionId,
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_JOINING,
		Role:        proto.Role_ROLE_TYPE_UNKNOWN,
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to assignPartition: %s", err))
	}
	if c.logger.IsDebug() {
		c.logger.Debug(fmt.Sprintf("Assigned partition %d to %s", partitionId, nodeId))
	}
}

// assignNewPartition will send a message to store that indicates that a node should start the joining process into a partition cluster
func (c *controller) assignNewPartition(ctx context.Context, newPartitionId int) {
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

func (c *controller) performMemberOperations(ctx context.Context) {
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
	leaderClient, err := c.client.ClusterLeader()
	if err != nil {
		c.logger.Error("Failed to get cluster leader client.")
		return
	}
	for partitionId, partition := range currentNode.Partitions {
		c.logger.Debug(fmt.Sprintf("Handling partition %d state %s", partitionId, partition.State))
		switch partition.State {
		case store.NodePartitionStateError:
		case store.NodePartitionStateJoining:
			// this node needs to start the joining partition group logic
			c.partitionsMu.Lock()
			c.handlePartitionStateJoining(ctx, partitionId, leaderClient)
			c.partitionsMu.Unlock()
		case store.NodePartitionStateInitializing:
			// we are currently in the process of joining partition group
			c.partitionsMu.RLock()
			c.handlePartitionStateInitializing(ctx, partitionId, leaderClient)
			c.partitionsMu.RUnlock()
		case store.NodePartitionStateInitialized:
			// we are successfully joined in partition group
			c.partitionsMu.Lock()
			c.handlePartitionStateInitialized(ctx, partitionId, leaderClient)
			c.partitionsMu.Unlock()
		case store.NodePartitionStateLeaving:
			// we need to leave partition group
			c.partitionsMu.Lock()
			c.handlePartitionStateLeaving(ctx, partitionId)
			c.partitionsMu.Unlock()
		default:
			panic(fmt.Sprintf("unexpected store.NodePartitionState: %#v", partition.State))
		}
	}
	// TODO:
	// regardless of the leader/follower status node needs to:
	//  - check its state in partitions to:
	//    - see if it has newly assigned partition that it needs to join
	//    - see if it has lost some assigned partition that it needs to leave
	//  - check if it is leader of any partition that does not have engine running yet and start it
	//  - check if it lost its leadership of any partition and needs to stop the engine (this should be preceded by previous error logs from the engine not being able to store changes)
}

func (c *controller) handlePartitionStateJoining(ctx context.Context, partitionId uint32, leaderClient zenproto.ZenServiceClient) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping handlePartitionStateJoining due to expired context")
		return
	}
	// check if partition is already assigned and skip
	if node, err := c.store.ClusterState().GetNode(c.store.ID()); err == nil {
		if partition, ok := node.Partitions[partitionId]; ok {
			// if we dont have running partition node we need to initialize it again
			_, runningOk := c.partitions[partitionId]
			if runningOk && (partition.State == store.NodePartitionStateInitializing ||
				partition.State == store.NodePartitionStateInitialized) {
				return
			}
		}
	}
	// change the state
	ctxClient, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := leaderClient.NodeCommand(ctxClient, &proto.Command{
		Type: proto.Command_TYPE_NODE_PARTITION_CHANGE,
		Request: &proto.Command_NodePartitionChange{
			NodePartitionChange: &proto.NodePartitionChange{
				NodeId:      c.store.ID(),
				PartitionId: partitionId,
				State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING,
				Role:        c.store.Role(),
			},
		},
	})
	if err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to change partition %d node state to INITIALIZING: %s", partitionId, err))
	}
	partitionConf := c.persistenceConfig
	partitionConf.RqLite.NodeID = fmt.Sprintf("zen-%s-partition-%d", c.store.ID(), partitionId)
	partitionConf.RqLite.DataPath = filepath.Join(c.config.Raft.Dir, fmt.Sprintf("partition-%d", partitionId))
	partitionNode, err := StartZenPartitionNode(context.Background(), c.mux, c.persistenceConfig, partitionId, PartitionChangesCallbacks{
		addNewNode: func(s raft.Server) error {
			return c.partitionAddNewNode(s, partitionId)
		},
		shutdownNode: func(s raft.ServerID) error {
			return c.partitionShutdownNode(s, partitionId)
		},
		leaderChange: func(s raft.ServerID) error {
			return c.partitionLeaderChange(s, partitionId)
		},
		removeNode: func(id string) error {
			return c.partitionRemoveNode(id, partitionId)
		},
		resumeNode: func(id string) error {
			return c.partitionResumeNode(id, partitionId)
		},
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("Failed to start partition %d node: %s", partitionId, err))
		_ = partitionNode.Stop()
		return
	}
	c.partitions[partitionId] = partitionNode

	c.handlePartitionStateInitializing(ctx, partitionId, leaderClient)
}

func (c *controller) handlePartitionStateInitializing(ctx context.Context, partitionId uint32, leaderClient zenproto.ZenServiceClient) {
	partitionNode, ok := c.partitions[partitionId]
	if !ok {
		// partition is still running joining operation
		return
	}
	_, err := partitionNode.WaitForLeader(1 * time.Minute)
	if errors.Is(err, rstore.ErrWaitForLeaderTimeout) {
		c.logger.Info(fmt.Sprintf("Timeout waiting for leader of partition %d.", partitionId))
		return
	}
	if ctx.Err() != nil {
		c.logger.Debug(fmt.Sprintf("Skipping handlePartitionStateInitializing due to expired context: %s", ctx.Err()))
		return
	}

	if partitionNode.engine == nil && partitionNode.IsLeader(ctx) {
		engine, err := c.createEngine(ctx, c.partitions[partitionId].rqliteDB)
		if err != nil {
			c.logger.Error("failed to create engine for partition %d: %s", partitionId, err.Error())
			// TODO: do something when this fails
		}
		partitionNode.engine = engine
		err = partitionNode.engine.Start()
		if err != nil {
			c.logger.Error("failed to start engine for partition %d: %s", partitionId, err.Error())
			// TODO: do something when this fails
		}
		c.logger.Info(fmt.Sprintf("Started engine for partition %d", partitionId))
	}
	partitionChangeCmd := &proto.Command_NodePartitionChange{
		NodePartitionChange: &proto.NodePartitionChange{
			NodeId:      c.store.ID(),
			PartitionId: partitionId,
			State:       proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED,
			Role:        partitionNode.Role(),
		},
	}
	ctxClient, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = leaderClient.NodeCommand(ctxClient, &proto.Command{
		Type:    proto.Command_TYPE_NODE_PARTITION_CHANGE,
		Request: partitionChangeCmd,
	})
	if err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to update state of the partition to INITIALIZED %d: %s", partitionId, err))
		return
	}
}

func (c *controller) handlePartitionStateInitialized(ctx context.Context, partitionId uint32, leaderClient zenproto.ZenServiceClient) {
	if _, ok := c.partitions[partitionId]; !ok {
		// we restarted the node and it needs to re-initialize its partition state
		c.handlePartitionStateJoining(ctx, partitionId, leaderClient)
		return
	}
}

func (c *controller) createEngine(ctx context.Context, db *RqLiteDB) (*bpmn.Engine, error) {
	// TODO: add check for migrations and apply only missing
	migrations, err := sql.GetMigrations()
	if err != nil {
		c.logger.Error("Failed to read migrations: %w", err)
		return nil, fmt.Errorf("failed to read migrations: %w", err)
	}
	statements := make([]*rqproto.Statement, len(migrations))
	for i, migration := range migrations {
		statements[i] = &rqproto.Statement{
			Sql: migration.SQL,
		}
	}
	_, err = db.executeStatements(ctx, statements)
	if err != nil {
		c.logger.Error("Failed to execute migrations: %w", err)
		return nil, fmt.Errorf("failed to execute migrations: %w", err)
	}
	engine := bpmn.NewEngine(bpmn.EngineWithStorage(db))
	return &engine, nil
}

func (c *controller) handlePartitionStateLeaving(ctx context.Context, partitionId uint32) {
	toLeave, ok := c.partitions[partitionId]
	if !ok {
		c.logger.Warn(fmt.Sprintf("Failed to find partition to leave: %d", partitionId))
		return
	}
	err := toLeave.Stop()
	if err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to stop partition %d.", partitionId))
		return
	}
	delete(c.partitions, partitionId)
	// TODO: verify that partition leader removes the node from the state after it gets removed from the cluster
}

func (c *controller) partitionResumeNode(id string, partitionId uint32) error {
	panic("unimplemented")
}

func (c *controller) partitionRemoveNode(id string, partitionId uint32) error {
	panic("unimplemented")
}

func (c *controller) partitionLeaderChange(s raft.ServerID, partitionId uint32) error {
	panic("unimplemented")
}

func (c *controller) partitionShutdownNode(s raft.ServerID, partitionId uint32) error {
	panic("unimplemented")
}

func (c *controller) partitionAddNewNode(s raft.Server, partitionId uint32) error {
	panic("unimplemented")
}

func (c *controller) Stop() error {
	var joinErr error
	for _, partition := range c.partitions {
		err := partition.Stop()
		if err != nil {
			joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop partition %d: %w", partition.partitionId, err))
		}
	}
	return joinErr
}

// NotifyShutdown notifies the cluster leader / store that the node is shutting down
func (c *controller) NotifyShutdown() error {
	// TODO: call zen cluster api on a leader to notify about node shutdown
	return nil
}

func (c *controller) IsPartitionLeader(ctx context.Context, partition uint32) bool {
	p, ok := c.partitions[partition]
	if !ok {
		return false
	}
	return p.IsLeader(ctx)
}

func (c *controller) IsAnyPartitionLeader(ctx context.Context) bool {
	for partitionId := range c.partitions {
		if c.IsPartitionLeader(ctx, partitionId) {
			return true
		}
	}
	return false
}

func (c *controller) PartitionEngine(ctx context.Context, partition uint32) *bpmn.Engine {
	partitionNode, ok := c.partitions[partition]
	if !ok {
		return nil
	}
	return partitionNode.engine
}

func (c *controller) Engines(ctx context.Context) map[uint32]*bpmn.Engine {
	res := make(map[uint32]*bpmn.Engine, 0)
	for partition, partitionNode := range c.partitions {
		if partitionNode.engine != nil {
			res[partition] = partitionNode.engine
		}
	}
	return res
}

func (c *controller) AllPartitionLeaderDBs(ctx context.Context) []*RqLiteDB {
	leaderQueries := make([]*RqLiteDB, 0)
	for _, partition := range c.partitions {
		if !partition.IsLeader(ctx) {
			continue
		}
		leaderQueries = append(leaderQueries, partition.rqliteDB)
	}
	return leaderQueries
}

func (c *controller) PartitionQueries(ctx context.Context, partitionId uint32) *sql.Queries {
	partitionNode, ok := c.partitions[partitionId]
	if !ok {
		return nil
	}
	return partitionNode.rqliteDB.queries
}
