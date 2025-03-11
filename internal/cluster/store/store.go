package store

import (
	"errors"
	"expvar"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
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

const (
	numSnapshots              = "num_snapshots"
	numSnapshotsFailed        = "num_snapshots_failed"
	numBackups                = "num_backups"
	numRestores               = "num_restores"
	numRestoresFailed         = "num_restores_failed"
	numJoins                  = "num_joins"
	numIgnoredJoins           = "num_ignored_joins"
	numRemovedBeforeJoins     = "num_removed_before_joins"
	snapshotCreateDuration    = "snapshot_create_duration"
	numSnapshotPersists       = "num_snapshot_persists"
	numSnapshotPersistsFailed = "num_snapshot_persists_failed"
	snapshotPersistDuration   = "snapshot_persist_duration"
	leaderChangesObserved     = "leader_changes_observed"
	leaderChangesDropped      = "leader_changes_dropped"
	failedHeartbeatObserved   = "failed_heartbeat_observed"
	nodesReapedOK             = "nodes_reaped_ok"
	nodesReapedFailed         = "nodes_reaped_failed"
)

var (
	// stats captures stats for the Store.
	stats *expvar.Map
)

func init() {
	stats = expvar.NewMap("zenbpm-store")
	ResetStats()
}

func ResetStats() {
	stats.Init()
	stats.Add(numSnapshots, 0)
	stats.Add(numSnapshotsFailed, 0)
	stats.Add(numBackups, 0)
	stats.Add(numRestores, 0)
	stats.Add(numRestoresFailed, 0)
	stats.Add(numJoins, 0)
	stats.Add(numIgnoredJoins, 0)
	stats.Add(numRemovedBeforeJoins, 0)
	stats.Add(snapshotCreateDuration, 0)
	stats.Add(numSnapshotPersists, 0)
	stats.Add(numSnapshotPersistsFailed, 0)
	stats.Add(snapshotPersistDuration, 0)
	stats.Add(leaderChangesObserved, 0)
	stats.Add(leaderChangesDropped, 0)
	stats.Add(failedHeartbeatObserved, 0)
	stats.Add(nodesReapedOK, 0)
	stats.Add(nodesReapedFailed, 0)
}

type Store struct {
	config Config

	open *atomic.Bool

	mu     sync.Mutex
	boltDB *raftboltdb.BoltStore

	layer  *tcp.Layer
	raftTn *raft.NetworkTransport

	raftID    string // Node ID.
	raftDir   string
	raft      *raft.Raft // The consensus mechanism
	boltStore *raftboltdb.BoltStore
	logger    hclog.Logger

	// Raft changes observer
	observer      *raft.Observer
	observerChan  chan raft.Observation
	observerClose chan struct{}
	observerDone  chan struct{}

	state ClusterState
}

type Config struct {
	NodeId              string
	RetainSnapshotCount int
	RaftTimeout         time.Duration
	RaftDir             string

	// Node-reaping configuration
	ReapTimeout         time.Duration
	ReapReadOnlyTimeout time.Duration
}

// DefaultConfig provides default store configuration based on cluster configuration.
func DefaultConfig(c config.Cluster) Config {
	conf := Config{
		RetainSnapshotCount: 2,
		RaftTimeout:         10 * time.Second,
		RaftDir:             c.RaftDir,
		ReapTimeout:         0,
		ReapReadOnlyTimeout: 0,
		NodeId:              c.NodeId,
	}
	return conf
}

// New returns a new Store.
// The store is in closed state and needs to be opened by calling Open before usage.
func New(layer *tcp.Layer, c Config) *Store {
	s := &Store{
		config:  c,
		mu:      sync.Mutex{},
		boltDB:  &raftboltdb.BoltStore{},
		raft:    &raft.Raft{},
		logger:  hclog.Default().Named("zenbpm-store"),
		open:    &atomic.Bool{},
		raftID:  c.NodeId,
		layer:   layer,
		raftDir: c.RaftDir,
		state: ClusterState{
			Partitions: map[uint32]Partition{},
			Nodes:      map[string]Node{},
		},
	}

	if c.RaftDir == "" {
		s.raftDir = "zenbpm_raft"
	}

	if c.NodeId == "" {
		s.raftID = random.String()
	}
	ResetStats()
	return s
}

// Open opens the store and configures underlying raft communication and storage
func (s *Store) Open() (retErr error) {
	defer func() {
		if retErr == nil {
			s.open.Store(true)
		}
	}()

	if s.open.Load() {
		return ErrAlreadyOpen
	}
	s.logger.Info("opening store with node ID %s, listening on %s", s.raftID, s.layer.Addr().String())

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Create Raft-compatible network layer.
	s.raftTn = raft.NewNetworkTransport(NewTransport(s.layer), connectionPoolCount, connectionTimeout, nil)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.config.RaftDir, s.config.RetainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(s.config.RaftDir, "raft.db"),
	})
	if err != nil {
		return fmt.Errorf("new bbolt store: %s", err)
	}
	s.boltStore = boltDB
	logStore = s.boltStore
	stableStore = s.boltStore

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, NewFSM(s), logStore, stableStore, snapshots, s.raftTn)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	s.observerChan = make(chan raft.Observation, observerChanLen)
	blocking := true
	s.observer = raft.NewObserver(s.observerChan, blocking, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		// _, isPeerChange := o.Data.(raft.PeerObservation)
		return isLeaderChange || isFailedHeartBeat // || isPeerChange
	})
	s.raft.RegisterObserver(s.observer)

	s.observerClose, s.observerDone = s.observe()
	return nil
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(jr *zproto.JoinRequest) error {
	if !s.open.Load() {
		return ErrNotOpen
	}

	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	id := jr.Id
	addr := jr.Address
	voter := jr.Voter

	// Confirm that this node can resolve the remote address. This can happen due
	// to incomplete DNS records across the underlying infrastructure. If it can't
	// then don't consider this join attempt successful -- so the joining node
	// will presumably try again.
	if _, err := resolvableAddress(addr); err != nil {
		return fmt.Errorf("failed to resolve %s: %w", addr, err)
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Info("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			// However, if *both* the ID and the address are the same, then no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
				stats.Add(numIgnoredJoins, 1)
				s.logger.Info("node %s at %s already member of cluster, ignoring join request", id, addr)
				return nil
			}

			if err := s.remove(id); err != nil {
				s.logger.Error("failed to remove node %s: %v", id, err)
				return err
			}
			stats.Add(numRemovedBeforeJoins, 1)
			s.logger.Info("removed node %s prior to rejoin with changed ID or address", id)
		}
	}

	var f raft.IndexFuture
	if voter {
		f = s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	} else {
		f = s.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	}
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}

	stats.Add(numJoins, 1)
	s.logger.Info("node with ID %s, at %s, joined successfully as voter: %s", id, addr, voter)
	return nil
}

// Bootstrap executes a cluster bootstrap on this node, using the given nodes.
func (s *Store) Bootstrap(nodes ...*Node) error {
	raftServers := make([]raft.Server, len(nodes))
	for i := range nodes {
		raftServers[i] = raft.Server{
			ID:      raft.ServerID(nodes[i].Id),
			Address: raft.ServerAddress(nodes[i].Addr),
		}
	}
	fut := s.raft.BootstrapCluster(raft.Configuration{
		Servers: raftServers,
	})
	return fut.Error()
}

// Stepdown forces this node to relinquish leadership to another node in
// the cluster. If this node is not the leader, and 'wait' is true, an error
// will be returned.
func (s *Store) Stepdown(wait bool) error {
	if !s.open.Load() {
		return ErrNotOpen
	}
	f := s.raft.LeadershipTransfer()
	if !wait {
		return nil
	}
	return f.Error()
}

// IsLeader is used to determine if the current node is cluster leader
func (s *Store) IsLeader() bool {
	if !s.open.Load() {
		return false
	}
	return s.raft.State() == raft.Leader
}

// HasLeader returns true if the cluster has a leader, false otherwise.
func (s *Store) HasLeader() bool {
	if !s.open.Load() {
		return false
	}
	return s.raft.Leader() != ""
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
	var leaderAddr string
	check := func() bool {
		var chkErr error
		leaderAddr, chkErr = s.LeaderAddr()
		return chkErr == nil && leaderAddr != ""
	}
	err := rsync.NewPollTrue(check, leaderWaitDelay, timeout).Run("leader")
	if err != nil {
		return "", ErrWaitForLeaderTimeout
	}
	return leaderAddr, err
}

// VerifyLeader checks that the current node is the Raft leader.
func (s *Store) VerifyLeader() (retErr error) {
	if !s.open.Load() {
		return ErrNotOpen
	}
	future := s.raft.VerifyLeader()
	if err := future.Error(); err != nil {
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			return ErrNotLeader
		}
		return fmt.Errorf("failed to verify leader: %s", err.Error())
	}
	return nil
}

// IsVoter returns true if the current node is a voter in the cluster. If there
// is no reference to the current node in the current cluster configuration then
// false will also be returned.
func (s *Store) IsVoter() (bool, error) {
	if !s.open.Load() {
		return false, ErrNotOpen
	}
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false, err
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == raft.ServerID(s.raftID) {
			return srv.Suffrage == raft.Voter, nil
		}
	}
	return false, nil
}

// RaftClusterState defines the possible Raft states the current node can be in
type RaftClusterState int

// Represents the Raft cluster states
const (
	Leader RaftClusterState = iota
	Follower
	Candidate
	Shutdown
	Unknown
)

// State returns the current node's Raft state
func (s *Store) State() RaftClusterState {
	if !s.open.Load() {
		return Unknown
	}
	state := s.raft.State()
	switch state {
	case raft.Leader:
		return Leader
	case raft.Candidate:
		return Candidate
	case raft.Follower:
		return Follower
	case raft.Shutdown:
		return Shutdown
	default:
		return Unknown
	}
}

// Addr returns the address of the store.
func (s *Store) Addr() string {
	if !s.open.Load() {
		return ""
	}
	return string(s.raftTn.LocalAddr())
}

// ID returns the Raft ID of the store.
func (s *Store) ID() string {
	return s.raftID
}

// LeaderAddr returns the address of the current leader. Returns a
// blank string if there is no leader or if the Store is not open.
func (s *Store) LeaderAddr() (string, error) {
	if !s.open.Load() {
		return "", nil
	}
	addr, _ := s.raft.LeaderWithID()
	return string(addr), nil
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	if !s.open.Load() {
		return "", nil
	}
	_, id := s.raft.LeaderWithID()
	return string(id), nil
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Store) LeaderWithID() (string, string) {
	if !s.open.Load() {
		return "", ""
	}
	addr, id := s.raft.LeaderWithID()
	return string(addr), string(id)
}

// HasLeaderID returns true if the cluster has a leader ID, false otherwise.
func (s *Store) HasLeaderID() bool {
	if !s.open.Load() {
		return false
	}
	_, id := s.raft.LeaderWithID()
	return id != ""
}

// CommitIndex returns the Raft commit index.
func (s *Store) CommitIndex() (uint64, error) {
	if !s.open.Load() {
		return 0, ErrNotOpen
	}
	return s.raft.CommitIndex(), nil
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
func (s *Store) Close(wait bool) (retErr error) {
	defer func() {
		if retErr == nil {
			s.logger.Info("store closed with node ID %s, listening on %s", s.raftID, s.layer.Addr().String())
			s.open.Store(false)
		}
	}()
	if !s.open.Load() {
		// Protect against closing already-closed resource, such as channels.
		return nil
	}

	close(s.observerClose)
	<-s.observerDone

	f := s.raft.Shutdown()
	if wait {
		if f.Error() != nil {
			return f.Error()
		}
	}

	if err := s.boltStore.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Store) WriteNodeChange(change *proto.NodeChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_NODE_CHANGE,
		Request: &proto.Command_NodeChange{
			NodeChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal NodeChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.config.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply NodeChange message to raft log: %w", f.Error())
	}
	return nil
}

func (s *Store) WritePartitionChange(change *proto.NodePartitionChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_NODE_PARTITION_CHANGE,
		Request: &proto.Command_NodePartitionChange{
			NodePartitionChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal NodePartitionChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.config.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply NodePartitionChange message to raft log: %w", f.Error())
	}
	return nil
}

// Nodes returns the slice of nodes in the cluster, sorted by ID ascending.
func (s *Store) Nodes() ([]Node, error) {
	if !s.open.Load() {
		return nil, ErrNotOpen
	}

	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil, fmt.Errorf("failed to get raft configuration for nodes: %w", f.Error())
	}

	rs := f.Configuration().Servers
	nodes := make([]Node, len(rs))
	for i := range rs {
		nodes[i] = Node{
			Id:       string(rs[i].ID),
			Addr:     string(rs[i].Address),
			Suffrage: rs[i].Suffrage,
		}
		node, err := s.state.GetNode(string(rs[i].ID))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				// TODO: decide if we need full node info here or just raft.Server
				continue
			}
			return nil, fmt.Errorf("failed to retrieve node info from state: %w", err)
		}
		nodes[i].State = node.State
		nodes[i].Partitions = node.Partitions
		nodes[i].Role = node.Role
	}

	sort.Sort(Nodes(nodes))
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
				case raft.FailedHeartbeatObservation:
					stats.Add(failedHeartbeatObserved, 1)

					nodes, err := s.Nodes()
					if err != nil {
						s.logger.Error("failed to get nodes configuration during reap check: %s", err.Error())
					}
					servers := Nodes(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)

					isReadOnly, found := servers.IsReadOnly(id)
					if !found {
						s.logger.Error("node %s (failing heartbeat) is not present in configuration", id)
						break
					}

					if (isReadOnly && s.config.ReapReadOnlyTimeout > 0 && dur > s.config.ReapReadOnlyTimeout) ||
						(!isReadOnly && s.config.ReapTimeout > 0 && dur > s.config.ReapTimeout) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := s.remove(id); err != nil {
							stats.Add(nodesReapedFailed, 1)
							s.logger.Error("failed to reap %s %s: %s", pn, id, err.Error())
						} else {
							stats.Add(nodesReapedOK, 1)
							s.logger.Info("successfully reaped %s %s", pn, id)
						}
					}
				case raft.LeaderObservation:
					s.selfLeaderChange(signal.LeaderID == raft.ServerID(s.raftID))
					if signal.LeaderID == raft.ServerID(s.raftID) {
						s.logger.Info("this node (ID=%s) is now Leader", s.raftID)
					} else {
						if signal.LeaderID == "" {
							s.logger.Warn("Leader is now unknown")
						} else {
							s.logger.Info("node %s is now Leader", signal.LeaderID)
						}
					}
				}
			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

// remove removes the node, with the given ID, from the cluster.
func (s *Store) remove(id string) error {
	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil && f.Error() == raft.ErrNotLeader {
		return ErrNotLeader
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
	nodeChange := proto.Command{
		Type: proto.Command_TYPE_NODE_CHANGE,
		Request: &proto.Command_NodeChange{
			NodeChange: &proto.NodeChange{
				NodeId:       s.raftID,
				PrivGrpcAddr: "",
				State:        proto.NodeState_NODE_STATE_STARTED,
				Role:         proto.Role_ROLE_TYPE_FOLLOWER,
			},
		},
	}
	b, err := pb.Marshal(&nodeChange)
	if err != nil {
		return fmt.Errorf("failed to marshal NodeChange - leadership change message: %w", err)
	}
	f := s.raft.Apply(b, s.config.RaftTimeout)
	if f.Error() != nil {
		return fmt.Errorf("failed to send NodeChange - leadership change message: %w", err)
	}
	return nil
}

func resolvableAddress(addr string) (string, error) {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Just try the given address directly.
		h = addr
	}
	_, err = net.LookupHost(h)
	return h, err
}
