package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	rstore "github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
)

type controller struct {
	// partitions contains a map of partition nodes on this zen node
	// one zen node will be always working with maximum of one partition node per partition
	partitions   map[uint32]*ZenPartitionNode
	partitionsMu sync.RWMutex
	store        ControlledStore
	client       *client.ClientManager
	config       config.Cluster
	rqLiteConfig config.RqLite
	mux          *tcp.Mux
	logger       hclog.Logger
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
	rqLiteConfig := c.config.RqLite

	if c.config.RqLite == nil {
		defaultConfig := GetRqLiteDefaultConfig(c.store.ID(), c.store.Addr(), c.store.ID(), c.config.Raft.JoinAddresses)
		rqLiteConfig = &defaultConfig
	}
	err := rqLiteConfig.Validate()
	if err != nil {
		return fmt.Errorf("failed to start controller, rqLite config validation failed: %w", err)
	}
	c.rqLiteConfig = *rqLiteConfig
	// TODO: wait until we catch up with raft log and verify that we have all the engines/partitions running
	go c.monitor()
	return nil
}

// ClusterStateChangeNotification is called in a goroutine from FSM when changes are applied to the state
func (c *controller) ClusterStateChangeNotification(ctx context.Context) {
	if c.store.IsLeader() {
		c.performLeaderOperations(ctx)
	}
	c.performMemberOperations(ctx)
}

func (c *controller) performLeaderOperations(ctx context.Context) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping leader operation checks due to expired context")
		return
	}
	cs := c.store.ClusterState()

	// verify that the partition count in the cluster is same as desired one.
	// if its not the leader starts to create partitions one by one (each new partition needs to report its leader into the state)
	currentPartitionCount := len(cs.Partitions)
	if int(cs.Config.DesiredPartitions) > currentPartitionCount {
		c.assignNewPartition(ctx, currentPartitionCount+1)
	}
	// TODO:
	// if node is leader he needs to:
	//  - verify that the partitions are spread across the cluster in the desired manner (we dont have spread logic yet)
}

// assignNewPartition will send a message to store that indicates that a node should start the joining process into a partition cluster
func (c *controller) assignNewPartition(ctx context.Context, newPartitionId int) {
	cs := c.store.ClusterState()
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
		c.logger.Debug("Skipping assigning of a new partition due expired context")
		return
	}
	c.logger.Info(fmt.Sprintf("Assigning new partition %d to %s", newPartitionId, partitionCandidate.Id))
	err = c.store.WritePartitionChange(&proto.NodePartitionChange{
		NodeId:      partitionCandidate.Id,
		PartitionId: uint32(newPartitionId),
		State:       proto.NodePartitionState_NODE_PARTITION_STATE_JOINING,
		Role:        proto.Role_ROLE_TYPE_UNKNOWN,
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to assignNewPartition: %s", err))
	}
}

func (c *controller) performMemberOperations(ctx context.Context) {
	if ctx.Err() != nil {
		c.logger.Debug("Skipping member operation checks due to context being cancelled")
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
		switch partition.State {
		case store.NodePartitionStateError:
		case store.NodePartitionStateJoining:
			// this node needs to start the joining partition group logic
			c.handlePartitionStateJoining(ctx, partitionId, leaderClient)
		case store.NodePartitionStateInitializing:
			// we are currently in the process of joining partition group
			c.handlePartitionStateInitializing(ctx, partitionId, leaderClient)
		case store.NodePartitionStateInitialized:
			// we are successfully joined in partition group
		case store.NodePartitionStateLeaving:
			// we need to leave partition group
			c.handlePartitionStateLeaving(ctx, partitionId)
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
		c.logger.Debug("Skipping handlePartitionStateJoining due to context being cancelled")
		return
	}
	c.partitionsMu.Lock()
	// check if partition is already assigned and skip
	if node, err := c.store.ClusterState().GetNode(c.store.ID()); err == nil {
		if partition, ok := node.Partitions[partitionId]; ok {
			if partition.State == store.NodePartitionStateInitializing ||
				partition.State == store.NodePartitionStateInitialized {
				c.partitionsMu.Unlock()
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
	partitionConf := c.rqLiteConfig
	partitionConf.NodeID = fmt.Sprintf("zen-%s-partition-%d", c.store.ID(), partitionId)
	partitionConf.DataPath = path.Join(c.config.Raft.Dir, fmt.Sprintf("partition-%d", partitionId))
	partitionNode, err := StartZenPartitionNode(context.Background(), c.mux, &partitionConf, partitionId, PartitionChangesCallbacks{
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
		c.partitionsMu.Unlock()
		_ = partitionNode.Stop()
		return
	}
	c.partitions[partitionId] = partitionNode
	c.partitionsMu.Unlock()

	c.handlePartitionStateInitializing(ctx, partitionId, leaderClient)
}

func (c *controller) handlePartitionStateInitializing(ctx context.Context, partitionId uint32, leaderClient zenproto.ZenServiceClient) {
	c.partitionsMu.RLock()
	defer func() {
		c.partitionsMu.RUnlock()
	}()
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
		c.logger.Debug(fmt.Sprintf("Skipping handlePartitionStateInitializing due to context being cancelled: %s", ctx.Err()))
		return
	}

	if partitionNode.engine == nil && partitionNode.IsLeader(ctx) {
		engine := bpmn.NewEngine(bpmn.EngineWithStorage(c.partitions[partitionId].rqliteDB))
		partitionNode.engine = &engine
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

// monitor runs the main controller loop that monitors and performs changes on the cluster
func (c *controller) monitor() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		// c.ClusterStateChangeNotification(context.Background())
		state, _ := json.MarshalIndent(c.store.ClusterState(), "", " ")
		fmt.Println(string(state))
	}
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
