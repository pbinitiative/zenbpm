package store

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/rqlite/rqlite/v8/rsync"
)

// Open opens the store and configures underlying raft communication and storage
func (s *Store) Open() (retErr error) {
	defer func() {
		if retErr == nil {
			s.open.Store(true)
		}
	}()

	if s.open.Load() {
		return zenerr.ErrAlreadyOpen
	}
	s.logger.Info(fmt.Sprintf("opening store with node ID %s, listening on %s", s.raftID, s.layer.Addr().String()))

	// Setup Raft configuration.
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(s.raftID)

	// Create Raft-compatible network layer.
	s.raftTn = raft.NewNetworkTransport(NewTransport(s.layer), connectionPoolCount, connectionTimeout, nil)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.cfg.RaftDir, s.cfg.RetainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(s.cfg.RaftDir, "raft.db"),
	})
	if err != nil {
		return fmt.Errorf("new bbolt store: %s", err)
	}
	s.boltStore = boltDB
	logStore = s.boltStore
	stableStore = s.boltStore

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(cfg, NewFSM(s), logStore, stableStore, snapshots, s.raftTn)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	s.observerChan = make(chan raft.Observation, observerChanLen)
	blocking := true
	s.observer = raft.NewObserver(s.observerChan, blocking, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		_, isPeerChange := o.Data.(raft.PeerObservation)
		return isLeaderChange || isFailedHeartBeat || isPeerChange
	})
	s.raft.RegisterObserver(s.observer)

	s.observerClose, s.observerDone = s.observe()
	return nil
}

// WaitForAllApplied waits for all Raft log entries to be applied to the
// underlying database.
func (s *Store) WaitForAllApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	return s.WaitForAppliedIndex(s.raft.LastIndex(), timeout)
}

// WaitForAppliedIndex blocks until a given log index has been applied,
// or the timeout expires.
func (s *Store) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	ch := s.appliedTarget.Subscribe(idx)
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for index %d to be applied", idx)
	}
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(jr *zproto.JoinRequest) error {
	if !s.open.Load() {
		return zenerr.ErrNotOpen
	}

	if s.raft.State() != raft.Leader {
		return zenerr.ErrNotLeader
	}

	id := jr.GetId()
	addr := jr.GetAddress()
	voter := jr.GetVoter()

	// Confirm that this node can resolve the remote address. This can happen due
	// to incomplete DNS records across the underlying infrastructure. If it can't
	// then don't consider this join attempt successful -- so the joining node
	// will presumably try again.
	if _, err := resolvableAddress(addr); err != nil {
		return fmt.Errorf("failed to resolve %s: %w", addr, err)
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Info(fmt.Sprintf("failed to get raft configuration: %v", err))
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			// However, if *both* the ID and the address are the same, then no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
				s.logger.Info(fmt.Sprintf("node %s at %s already member of cluster, ignoring join request", id, addr))
				return nil
			}

			if err := s.remove(id); err != nil {
				s.logger.Error(fmt.Sprintf("failed to remove node %s: %v", id, err))
				return err
			}
			s.logger.Info(fmt.Sprintf("removed node %s prior to rejoin with changed ID or address", id))
		}
	}

	var f raft.IndexFuture
	if voter {
		f = s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	} else {
		f = s.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	}
	if e := f.(raft.Future); e.Error() != nil {
		if errors.Is(e.Error(), raft.ErrNotLeader) {
			return zenerr.ErrNotLeader
		}
		return e.Error()
	}

	s.logger.Info(fmt.Sprintf("node with ID %s, at %s, joined successfully as voter: %t", id, addr, voter))
	return nil
}

// Bootstrap executes a cluster bootstrap on this node, using the given nodes.
func (s *Store) Bootstrap(nodes ...*state.Node) error {
	if !s.open.Load() {
		return zenerr.ErrNotOpen
	}
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
	if fut.Error() != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", fut.Error())
	}
	return nil
}

// Notify notifies this Store that a node is ready for bootstrapping at the
// given address. Once the number of known nodes reaches the expected level
// bootstrapping will be attempted using this Store. "Expected level" includes
// this node, so this node must self-notify to ensure the cluster bootstraps
// with the *advertised Raft address* which the Store doesn't know about.
//
// Notifying is idempotent. A node may repeatedly notify the Store without issue.
func (s *Store) Notify(nr *zproto.NotifyRequest) error {
	if !s.open.Load() {
		return zenerr.ErrNotOpen
	}

	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()

	if s.bootstrapExpect == 0 || s.bootstrapped || s.HasLeader() {
		// There is no reason this node will bootstrap.
		//
		// - Read-only nodes require that BootstrapExpect is set to 0, so this
		// block ensures that notifying a read-only node will not cause a bootstrap.
		// - If the node is already bootstrapped, then there is nothing to do.
		// - If the node already has a leader, then no bootstrapping is required.
		return nil
	}

	if _, ok := s.notifyingNodes[nr.GetId()]; ok {
		return nil
	}

	// Confirm that this node can resolve the remote address. This can happen due
	// to incomplete DNS records across the underlying infrastructure. If it can't
	// then don't consider this Notify attempt successful -- so the notifying node
	// will presumably try again.
	if addr, err := resolvableAddress(nr.GetAddress()); err != nil {
		return fmt.Errorf("failed to resolve %s: %w", addr, err)
	}

	s.notifyingNodes[nr.GetId()] = raft.Server{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(nr.GetId()),
		Address:  raft.ServerAddress(nr.GetAddress()),
	}
	if len(s.notifyingNodes) < s.bootstrapExpect {
		return nil
	}
	s.logger.Info(fmt.Sprintf("reached expected bootstrap count of %d, starting cluster bootstrap", s.bootstrapExpect))

	raftServers := make([]raft.Server, 0, len(s.notifyingNodes))
	for _, n := range s.notifyingNodes {
		raftServers = append(raftServers, n)
	}
	bf := s.raft.BootstrapCluster(raft.Configuration{
		Servers: raftServers,
	})
	if bf.Error() != nil {
		s.logger.Error(fmt.Sprintf("cluster bootstrap failed: %s", bf.Error()))
	} else {
		s.logger.Info(fmt.Sprintf("cluster bootstrap successful, servers: %s", raftServers))
	}
	s.bootstrapped = true
	return nil
}

// Stepdown forces this node to relinquish leadership to another node in
// the cluster. If this node is not the leader, and 'wait' is true, an error
// will be returned.
func (s *Store) Stepdown(wait bool) error {
	if !s.open.Load() {
		return zenerr.ErrNotOpen
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
		return "", zenerr.ErrWaitForLeaderTimeout
	}
	return leaderAddr, err
}

// VerifyLeader checks that the current node is the Raft leader.
func (s *Store) VerifyLeader() (retErr error) {
	if !s.open.Load() {
		return zenerr.ErrNotOpen
	}
	future := s.raft.VerifyLeader()
	if err := future.Error(); err != nil {
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			return zenerr.ErrNotLeader
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
		return false, zenerr.ErrNotOpen
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
		return 0, zenerr.ErrNotOpen
	}
	return s.raft.CommitIndex(), nil
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
// Before calling Close the caller should already Notify leader that it is shutting down.
func (s *Store) Close(wait bool) (retErr error) {
	// if s.IsLeader() {
	// 	node, err := s.state.GetNode(s.ID())
	// 	if err != nil {
	// 		s.logger.Warn(fmt.Sprintf("failed to retrieve node from state: %s", err))
	// 	}
	// 	s.WriteNodeChange(&proto.NodeChange{
	// 		NodeId:   node.Id,
	// 		Addr:     node.Addr,
	// 		Suffrage: proto.RaftSuffrage(node.Suffrage + 1),
	// 		State:    proto.NodeState(node.State),
	// 		Role:     proto.Role(node.Role),
	// 	})
	// 	err = s.Stepdown(true)
	// 	if err != nil {
	// 		s.logger.Warn(fmt.Sprintf("failed to stepdown as a leader: %s", err))
	// 	}
	// }

	defer func() {
		if retErr == nil {
			s.logger.Info(fmt.Sprintf("store closed with node ID %s, listening on %s", s.raftID, s.layer.Addr().String()))
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
			fmt.Println("shutdown", f.Error())
			return f.Error()
		}
	}

	if err := s.boltStore.Close(); err != nil {
		return err
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
