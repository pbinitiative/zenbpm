package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/rqlite/v8/rsync"
	"github.com/rqlite/rqlite/v8/tcp"
	pb "google.golang.org/protobuf/proto"
)

const (
	observerChanLen     = 100
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second
	leaderWaitDelay     = 100 * time.Millisecond
)

type Store struct {
	cfg Config

	open *atomic.Bool

	notifyMu        sync.Mutex
	bootstrapExpect int
	bootstrapped    bool
	notifyingNodes  map[string]raft.Server

	// mutext used by fsm to lock state changes
	stateMu sync.Mutex
	boltDB  *raftboltdb.BoltStore

	layer  *tcp.Layer
	raftTn *raft.NetworkTransport

	raftID    string // Node ID.
	raftDir   string
	raft      *raft.Raft // The consensus mechanism
	boltStore *raftboltdb.BoltStore
	logger    hclog.Logger

	appliedTarget *rsync.ReadyTarget[uint64]

	// Raft changes observer
	observer      *raft.Observer
	observerChan  chan raft.Observation
	observerClose chan struct{}
	observerDone  chan struct{}

	state                      state.Cluster
	clusterStateChangeObserver ClusterStateObserverFunc
}

type Config struct {
	NodeId              string
	RetainSnapshotCount int
	RaftTimeout         time.Duration
	RaftDir             string

	// Node-reaping configuration
	ReapTimeout         time.Duration
	ReapReadOnlyTimeout time.Duration

	// Time after which the node becomes marked as shut down when it stops sending heartbeats
	// must be lower than reap timeout if set
	NodeHearbeatShutdownTimeout time.Duration

	BootstrapExpect int
}

// DefaultConfig provides default store configuration based on cluster configuration.
func DefaultConfig(c config.Cluster) Config {
	conf := Config{
		RetainSnapshotCount:         2,
		RaftDir:                     c.Raft.Dir,
		ReapTimeout:                 0,
		RaftTimeout:                 5 * time.Second,
		ReapReadOnlyTimeout:         0,
		NodeId:                      c.NodeId,
		NodeHearbeatShutdownTimeout: 2 * time.Second,
		BootstrapExpect:             c.Raft.BootstrapExpect,
	}
	if c.Raft.Dir == "" {
		conf.RaftDir = "zenbpm_raft"
	}

	if c.NodeId == "" {
		conf.NodeId = random.String()
	}
	return conf
}

// New returns a new Store.
// The store is in closed state and needs to be opened by calling Open before usage.
func New(layer *tcp.Layer, stateObserverFn ClusterStateObserverFunc, c Config) *Store {
	s := &Store{
		cfg:             c,
		stateMu:         sync.Mutex{},
		boltDB:          &raftboltdb.BoltStore{},
		raft:            &raft.Raft{},
		logger:          hclog.Default().Named("zenbpm-store"),
		open:            &atomic.Bool{},
		raftID:          c.NodeId,
		layer:           layer,
		raftDir:         c.RaftDir,
		bootstrapExpect: c.BootstrapExpect,
		notifyingNodes:  map[string]raft.Server{},
		notifyMu:        sync.Mutex{},
		bootstrapped:    false,
		state: state.Cluster{
			Config: state.ClusterConfig{
				DesiredPartitions: 1, // TODO: hardcoded partition number for now
			},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
		raftTn:                     &raft.NetworkTransport{},
		boltStore:                  &raftboltdb.BoltStore{},
		appliedTarget:              rsync.NewReadyTarget[uint64](),
		observer:                   &raft.Observer{},
		observerChan:               make(chan raft.Observation),
		observerClose:              make(chan struct{}),
		observerDone:               make(chan struct{}),
		clusterStateChangeObserver: stateObserverFn,
	}

	return s
}

func (s *Store) ClusterState() state.Cluster {
	return *s.state.DeepCopy()
}

func (s *Store) Role() proto.Role {
	if s.IsLeader() {
		return proto.Role_ROLE_TYPE_LEADER
	}
	return proto.Role_ROLE_TYPE_FOLLOWER
}

func (s *Store) NodeID() string {
	return s.cfg.NodeId
}

func (s *Store) WriteNodeChange(change *proto.NodeChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_NODE_CHANGE.Enum(),
		Request: &proto.Command_NodeChange{
			NodeChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal NodeChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.cfg.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply NodeChange message to raft log: %w", f.Error())
	}
	return nil
}

func (s *Store) WritePartitionChange(change *proto.NodePartitionChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_NODE_PARTITION_CHANGE.Enum(),
		Request: &proto.Command_NodePartitionChange{
			NodePartitionChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal NodePartitionChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.cfg.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply NodePartitionChange message to raft log: %w", f.Error())
	}
	return nil
}

// Nodes returns the slice of nodes in the cluster, sorted by ID ascending.
func (s *Store) Nodes() ([]state.Node, error) {
	if !s.open.Load() {
		return nil, zenerr.ErrNotOpen
	}

	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil, fmt.Errorf("failed to get raft configuration for nodes: %w", f.Error())
	}

	rs := f.Configuration().Servers
	nodes := make([]state.Node, len(rs))
	for i := range rs {
		nodes[i] = state.Node{
			Id:       string(rs[i].ID),
			Addr:     string(rs[i].Address),
			Suffrage: rs[i].Suffrage,
		}
		node, err := s.state.GetNode(string(rs[i].ID))
		if err != nil {
			if errors.Is(err, zenerr.ErrNodeNotFound) {
				// TODO: decide if we need full node info here or just raft.Server
				continue
			}
			return nil, fmt.Errorf("failed to retrieve node info from state: %w", err)
		}
		nodes[i].State = node.State
		nodes[i].Partitions = node.Partitions
		nodes[i].Role = node.Role
	}

	sort.Sort(state.Nodes(nodes))
	return nodes, nil
}

func (s *Store) observe() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case o := <-s.observerChan:
				switch signal := o.Data.(type) {
				case raft.ResumedHeartbeatObservation:
					if err := s.resumeNode(signal.PeerID); err == nil {
						s.logger.Info(fmt.Sprintf("node %s was recovered in the state", signal.PeerID))
					}
				case raft.FailedHeartbeatObservation:

					nodes, err := s.Nodes()
					if err != nil {
						s.logger.Error(fmt.Sprintf("failed to get nodes configuration during reap check: %s", err.Error()))
					}
					servers := state.Nodes(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)

					isReadOnly, found := servers.IsReadOnly(id)
					if !found {
						s.logger.Error(fmt.Sprintf("node %s (failing heartbeat) is not present in configuration", id))
						break
					}

					if s.cfg.NodeHearbeatShutdownTimeout > 0 && dur > s.cfg.NodeHearbeatShutdownTimeout {
						if err = s.shutdownNode(signal.PeerID); err == nil {
							s.logger.Info(fmt.Sprintf("node %s was shutdown in the state", signal.PeerID))
						}
					}

					if (isReadOnly && s.cfg.ReapReadOnlyTimeout > 0 && dur > s.cfg.ReapReadOnlyTimeout) ||
						(!isReadOnly && s.cfg.ReapTimeout > 0 && dur > s.cfg.ReapTimeout) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := s.remove(id); err != nil {
							s.logger.Error(fmt.Sprintf("failed to reap %s %s: %s", pn, id, err.Error()))
						} else {
							s.logger.Info(fmt.Sprintf("successfully reaped %s %s", pn, id))
						}
					}
				case raft.LeaderObservation:
					isLeader := signal.LeaderID == raft.ServerID(s.raftID)
					s.selfLeaderChange(isLeader)
					if isLeader {
						s.logger.Info(fmt.Sprintf("this node (ID=%s) is now Leader", s.raftID))
					} else {
						if signal.LeaderID == "" {
							s.logger.Warn("Leader is now unknown")
						} else {
							s.logger.Info(fmt.Sprintf("node %s is now Leader", signal.LeaderID))
						}
					}
				case raft.PeerObservation:
					// PeerObservation is invoked only when the raft replication goroutine is started/stoped
					var err error
					if signal.Removed {
						if err = s.shutdownNode(signal.Peer.ID); err == nil {
							s.logger.Info(fmt.Sprintf("node %s was shutdown in the state", signal.Peer.ID))
						}
					} else {
						if err = s.addNewNode(signal.Peer); err == nil {
							s.logger.Debug(fmt.Sprintf("node %s was updated in the state", signal.Peer.ID))
						}
					}
					if err != nil {
						s.logger.Error(fmt.Sprintf("failed to update peer observation: %s", err))
					}
				}
			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

// addNewNode is called when leader observes that a node has been added
func (s *Store) addNewNode(node raft.Server) error {
	if !s.IsLeader() {
		return nil
	}
	nodeChange := &proto.NodeChange{
		NodeId: ptr.To(string(node.ID)),
		Addr:   ptr.To(string(node.Address)),
		State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}
	switch node.Suffrage {
	case raft.Voter:
		nodeChange.Suffrage = proto.RaftSuffrage_RAFT_SUFFRAGE_VOTER.Enum()
	case raft.Nonvoter:
		nodeChange.Suffrage = proto.RaftSuffrage_RAFT_SUFFRAGE_NONVOTER.Enum()
	}
	err := s.WriteNodeChange(nodeChange)
	if err != nil {
		return fmt.Errorf("failed to write NodeChange update for %s: %w", node.ID, err)
	}
	return nil
}

// shutdownNode is called when leader observes a node change that signals that a node was removed
func (s *Store) shutdownNode(nodeId raft.ServerID) error {
	if !s.IsLeader() {
		return nil
	}
	nodeChange := &proto.NodeChange{
		NodeId: ptr.To(string(nodeId)),
		State:  proto.NodeState_NODE_STATE_SHUTDOWN.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}
	err := s.WriteNodeChange(nodeChange)
	if err != nil {
		return fmt.Errorf("failed to write shutdown NodeChange for %s: %w", nodeId, err)
	}
	return nil
}

// resumeNode is called when leader observes that a node resumed its heartbeat
func (s *Store) resumeNode(nodeId raft.ServerID) error {
	// skip if node is not a leader
	if !s.IsLeader() {
		return nil
	}
	nodeChange := &proto.NodeChange{
		NodeId: ptr.To(string(nodeId)),
		State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:   proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
	}
	err := s.WriteNodeChange(nodeChange)
	if err != nil {
		return fmt.Errorf("failed to write shutdown NodeChange for %s: %w", nodeId, err)
	}
	return nil
}

// remove removes the node, with the given ID, from the cluster.
func (s *Store) remove(id string) error {
	// TODO: should we completely remove reaped nodes from the state?
	if err := s.shutdownNode(raft.ServerID(id)); err != nil {
		return fmt.Errorf("failed to shutdown node %s: %w", id, err)
	}
	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil && f.Error() == raft.ErrNotLeader {
		return zenerr.ErrNotLeader
	}
	if f.Error() != nil {
		return fmt.Errorf("failed to remove node %s: %w", id, f.Error())
	}
	return nil
}

// selfLeaderChange is called when this node detects that its leadership
// status has changed.
func (s *Store) selfLeaderChange(leader bool) error {
	if !leader {
		s.logger.Info("different node became leader, not performing leadership change")
		return nil
	}

	s.logger.Info("this node is now leader")
	err := s.WriteNodeChange(&proto.NodeChange{
		NodeId:   ptr.To(s.raftID),
		Addr:     ptr.To(s.Addr()),
		State:    proto.NodeState_NODE_STATE_STARTED.Enum(),
		Role:     proto.Role_ROLE_TYPE_LEADER.Enum(),
		Suffrage: proto.RaftSuffrage_RAFT_SUFFRAGE_VOTER.Enum(),
	})
	if err != nil {
		return fmt.Errorf("failed to send NodeChange - leadership change message: %w", err)
	}
	return nil
}

// PartitionLeaderWithID is used to return the current leader address and ID of the partition leader.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Store) PartitionLeaderWithID(partition uint32) (string, string) {
	if !s.open.Load() {
		return "", ""
	}
	partitionInfo, ok := s.state.Partitions[partition]
	if !ok {
		return "", ""
	}
	partitionLeader, ok := s.state.Nodes[partitionInfo.LeaderId]
	if !ok {
		return "", ""
	}
	return partitionLeader.Addr, partitionInfo.LeaderId
}
