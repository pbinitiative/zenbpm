package controller

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/rqlite/rqlite/v10/tcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerStopDisablesClusterStateChangeNotifications(t *testing.T) {
	tStore := &countingControllerTestStore{
		ControllerTestStore: &ControllerTestStore{
			id: "test-node-1",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{},
				Nodes:      map[string]state.Node{},
			},
		},
	}
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.store = tStore
	controller.logger = hclog.NewNullLogger()
	controller.handleClusterChanges = true

	require.NoError(t, controller.Stop())
	controller.ClusterStateChangeNotification(t.Context())

	assert.Zero(t, tStore.isLeaderCalls.Load())
}

func TestControllerStopWaitsForInFlightClusterStateChangeNotification(t *testing.T) {
	notificationStarted := make(chan struct{})
	releaseNotification := make(chan struct{})
	tStore := &countingControllerTestStore{
		ControllerTestStore: &ControllerTestStore{
			id: "test-node-1",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{},
				Nodes:      map[string]state.Node{},
			},
		},
		isLeaderFn: func(call int32) bool {
			if call == 1 {
				close(notificationStarted)
				<-releaseNotification
			}
			return false
		},
	}
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.store = tStore
	controller.logger = hclog.NewNullLogger()
	controller.handleClusterChanges = true

	notificationDone := make(chan struct{})
	go func() {
		defer close(notificationDone)
		controller.ClusterStateChangeNotification(t.Context())
	}()
	<-notificationStarted

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- controller.Stop()
	}()

	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before the in-flight notification completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseNotification)
	require.NoError(t, <-stopDone)
	<-notificationDone

	controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, int32(1), tStore.isLeaderCalls.Load())
}

func TestControllerStopCancelsPendingPartitionJoinRetry(t *testing.T) {
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.handleClusterChanges = true

	controller.handlePartitionStateInitializing(1)

	startedAt := time.Now()
	require.NoError(t, controller.Stop())
	assert.Less(t, time.Since(startedAt), time.Second)
}

func TestControllerDeduplicatesPartitionRetries(t *testing.T) {
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.retryDelay = time.Hour

	controller.schedulePartitionRetry(1, "first-retry")
	controller.schedulePartitionRetry(1, "duplicate-retry")

	controller.retryMu.Lock()
	assert.True(t, controller.retryScheduled[1])
	assert.Equal(t, uint(1), controller.retryAttempts[1])
	controller.retryMu.Unlock()
	require.NoError(t, controller.Stop())
}

func TestControllerRetrySurvivesSourceContextCancellation(t *testing.T) {
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.retryDelay = 10 * time.Millisecond
	controller.logger = hclog.NewNullLogger()
	controller.handleClusterChanges = true
	controller.store = &ControllerTestStore{
		id: "test-node-1",
		clusterState: state.Cluster{Nodes: map[string]state.Node{
			"test-node-1": {Id: "test-node-1", Partitions: map[uint32]state.NodePartition{}},
		}},
	}
	controller.client = client.NewClientManager(controller.store.(*ControllerTestStore))

	sourceCtx, cancel := context.WithCancel(t.Context())
	cancel()
	controller.schedulePartitionRetry(1, "context-independent-retry")

	require.Eventually(t, func() bool {
		controller.retryMu.Lock()
		defer controller.retryMu.Unlock()
		return !controller.retryScheduled[1]
	}, time.Second, 5*time.Millisecond)
	require.Error(t, sourceCtx.Err())
	require.NoError(t, controller.Stop())
}

func TestPartitionLeaderInitialized(t *testing.T) {
	tests := []struct {
		name         string
		clusterState state.Cluster
		initialized  bool
	}{
		{
			name:         "partition is missing",
			clusterState: state.Cluster{},
		},
		{
			name: "leader is missing",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "leader"}},
			},
		},
		{
			name: "leader partition is initializing",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "leader"}},
				Nodes: map[string]state.Node{
					"leader": {
						Id: "leader",
						Partitions: map[uint32]state.NodePartition{
							1: {Id: 1, State: state.NodePartitionStateInitializing, Role: state.RoleLeader},
						},
					},
				},
			},
		},
		{
			name: "leader partition is initialized",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "leader"}},
				Nodes: map[string]state.Node{
					"leader": {
						Id: "leader",
						Partitions: map[uint32]state.NodePartition{
							1: {Id: 1, State: state.NodePartitionStateInitialized, Role: state.RoleLeader},
						},
					},
				},
			},
			initialized: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.initialized, partitionLeaderInitialized(tt.clusterState, 1))
		})
	}
}

func TestControllerCanStartNewPartitions(t *testing.T) {
	tStore, clientMgr, mux := setupControllerTestCluster(t)

	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id,
		Addr:   tStore.addr,
		Adv:    tStore.addr,
		Raft: config.ClusterRaft{
			Dir:                    t.TempDir(),
			JoinAttempts:           2,
			JoinInterval:           100 * time.Millisecond,
			JoinAddresses:          []string{tStore.addr},
			BootstrapExpect:        1,
			BootstrapExpectTimeout: 1 * time.Second,
		},
	})
	assert.NoError(t, err)

	err = controller.Start(tStore, clientMgr)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})

	// add node to the cluster state
	tStore.setNode(state.Node{
		Id:         tStore.id,
		Addr:       tStore.addr,
		Suffrage:   raft.Voter,
		State:      state.NodeStateStarted,
		Role:       state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	})

	controller.ClusterStateChangeNotification(t.Context())
	// verify that controller updated state so that new partition needs to be created by a node
	s := controller.store.ClusterState()
	assert.Equal(t, state.NodePartitionStateInitialized, s.Nodes[tStore.id].Partitions[1].State)

	// verify that partition was started
	testPoll(t, func() bool {
		s := controller.store.ClusterState()
		if tStore.id == s.Partitions[1].LeaderId &&
			state.NodePartitionStateInitialized == s.Nodes[tStore.id].Partitions[1].State {
			return true
		}
		return false
	}, 100*time.Millisecond, 5*time.Second, "Failed to verify that partition was started. State was: %s", controller.store.ClusterState().Nodes[tStore.id].Partitions[1].State)

	// update desired partition count
	tStore.setDesiredPartitions(2)

	controller.ClusterStateChangeNotification(t.Context())
	// verify that new partition was created
	testPoll(t, func() bool {
		s := controller.store.ClusterState()
		if tStore.id == s.Partitions[2].LeaderId &&
			state.NodePartitionStateInitialized == s.Nodes[tStore.id].Partitions[2].State {
			return true
		}
		return false
	}, 100*time.Millisecond, 10*time.Second, "Failed to verify that second partition was started. State was: %s", controller.store.ClusterState().Nodes[tStore.id].Partitions[2].State)
}

func TestControllerDoesNotMarkPartitionInitializedWhenMigrationsFail(t *testing.T) {
	tStore, clientMgr, mux := setupControllerTestCluster(t)

	migrationDir := writeBrokenMigrationFixture(t)
	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id,
		Addr:   tStore.addr,
		Adv:    tStore.addr,
		Raft: config.ClusterRaft{
			Dir:                    t.TempDir(),
			JoinAttempts:           2,
			JoinInterval:           100 * time.Millisecond,
			JoinAddresses:          []string{tStore.addr},
			BootstrapExpect:        1,
			BootstrapExpectTimeout: 1 * time.Second,
		},
		Persistence: config.Persistence{
			Migration: config.Migration{Dir: migrationDir},
		},
	})
	require.NoError(t, err)
	controller.retryDelay = 25 * time.Millisecond

	require.NoError(t, controller.Start(tStore, clientMgr))
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})

	tStore.setNode(state.Node{
		Id:         tStore.id,
		Addr:       tStore.addr,
		Suffrage:   raft.Voter,
		State:      state.NodeStateStarted,
		Role:       state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	})

	controller.ClusterStateChangeNotification(t.Context())

	s := controller.store.ClusterState()
	require.Equal(t, state.NodePartitionStateInitializing, s.Nodes[tStore.id].Partitions[1].State)
	assert.Never(t, func() bool {
		s := controller.store.ClusterState()
		return s.Nodes[tStore.id].Partitions[1].State == state.NodePartitionStateInitialized
	}, 100*time.Millisecond, 10*time.Millisecond, "partition must not be reported as initialized when schema migrations fail")

	repairBrokenMigrationFixture(t, migrationDir)
	require.Eventually(t, func() bool {
		s := controller.store.ClusterState()
		return s.Nodes[tStore.id].Partitions[1].State == state.NodePartitionStateInitialized
	}, 5*time.Second, 20*time.Millisecond, "partition was not initialized after repairing the migration")
}

func TestControllerRejoinsPartitionStuckInInitializing(t *testing.T) {
	tStore, clientMgr, mux := setupControllerTestCluster(t)

	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id,
		Addr:   tStore.addr,
		Adv:    tStore.addr,
		Raft: config.ClusterRaft{
			Dir:                    t.TempDir(),
			JoinAttempts:           2,
			JoinInterval:           100 * time.Millisecond,
			JoinAddresses:          []string{tStore.addr},
			BootstrapExpect:        1,
			BootstrapExpectTimeout: 1 * time.Second,
		},
	})
	require.NoError(t, err)
	controller.retryDelay = 25 * time.Millisecond

	require.NoError(t, controller.Start(tStore, clientMgr))
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})

	// simulate a restart after INITIALIZING was persisted but the partition node
	// is not running locally
	tStore.setNode(state.Node{
		Id:       tStore.id,
		Addr:     tStore.addr,
		Suffrage: raft.Voter,
		State:    state.NodeStateStarted,
		Role:     state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{
			1: {Id: 1, State: state.NodePartitionStateInitializing},
		},
	})

	controller.ClusterStateChangeNotification(t.Context())

	require.Eventually(t, func() bool {
		return controller.store.ClusterState().Nodes[tStore.id].Partitions[1].State == state.NodePartitionStateInitialized
	}, 10*time.Second, 20*time.Millisecond, "partition stuck in INITIALIZING was never re-joined")
}

func TestControllerMarksPersistentlyBrokenPartitionAsError(t *testing.T) {
	tStore, clientMgr, mux := setupControllerTestCluster(t)
	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id, Addr: tStore.addr, Adv: tStore.addr,
		Raft: config.ClusterRaft{
			Dir: t.TempDir(), JoinAttempts: 2, JoinInterval: 100 * time.Millisecond,
			JoinAddresses: []string{tStore.addr}, BootstrapExpect: 1, BootstrapExpectTimeout: time.Second,
		},
		Persistence: config.Persistence{Migration: config.Migration{Dir: writeBrokenMigrationFixture(t)}},
	})
	require.NoError(t, err)
	controller.retryDelay = time.Millisecond
	require.NoError(t, controller.Start(tStore, clientMgr))
	t.Cleanup(func() { assert.NoError(t, controller.Stop()) })
	tStore.setNode(state.Node{
		Id: tStore.id, Addr: tStore.addr, Suffrage: raft.Voter,
		State: state.NodeStateStarted, Role: state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	})

	controller.ClusterStateChangeNotification(t.Context())

	require.Eventually(t, func() bool {
		return controller.store.ClusterState().Nodes[tStore.id].Partitions[1].State == state.NodePartitionStateError
	}, 5*time.Second, 10*time.Millisecond)
}

type countingControllerTestStore struct {
	*ControllerTestStore
	isLeaderCalls atomic.Int32
	isLeaderFn    func(call int32) bool
}

func (c *countingControllerTestStore) IsLeader() bool {
	call := c.isLeaderCalls.Add(1)
	if c.isLeaderFn != nil {
		return c.isLeaderFn(call)
	}
	return c.ControllerTestStore.IsLeader()
}

type ControllerTestStore struct {
	mu           sync.RWMutex
	id           string
	addr         string
	clusterState state.Cluster
	leader       bool
}

// Addr implements ControlledStore.
func (c *ControllerTestStore) Addr() string {
	return c.addr
}

// ClusterState implements ControlledStore.
func (c *ControllerTestStore) ClusterState() state.Cluster {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.clusterState.DeepCopy()
}

// ID implements ControlledStore.
func (c *ControllerTestStore) ID() string {
	return c.id
}

// IsLeader implements ControlledStore.
func (c *ControllerTestStore) IsLeader() bool {
	return c.leader
}

// LeaderWithID implements client.ClientStore.
func (c *ControllerTestStore) LeaderWithID() (string, string) {
	return c.addr, c.id
}

// PartitionLeaderWithID implements client.ClientStore.
func (c *ControllerTestStore) PartitionLeaderWithID(partition uint32) (string, string) {
	return c.addr, c.id
}

// Role implements ControlledStore.
func (c *ControllerTestStore) Role() proto.Role {
	if c.leader == true {
		return proto.Role_ROLE_TYPE_LEADER
	} else {
		return proto.Role_ROLE_TYPE_FOLLOWER
	}
}

// LeaderID implements store.FsmStore.
func (c *ControllerTestStore) LeaderID() (string, error) {
	return c.id, nil
}

// Join implements server.StoreService.
func (c *ControllerTestStore) Join(jr *zenproto.JoinRequest) error {
	panic("unexpected call to Join")
}

// Notify implements server.StoreService.
func (c *ControllerTestStore) Notify(nr *zenproto.NotifyRequest) error {
	panic("unexpected call to Notify")
}

// WriteNodeChange implements server.StoreService.
func (c *ControllerTestStore) WriteNodeChange(change *proto.NodeChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tStore := controllerTestFsmStore{clusterState: *c.clusterState.DeepCopy(), leaderID: c.id}
	c.clusterState = store.FsmApplyNodeChange(tStore, change)
	return nil
}

// WritePartitionChange implements ControlledStore.
func (c *ControllerTestStore) WritePartitionChange(change *proto.NodePartitionChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tStore := controllerTestFsmStore{clusterState: *c.clusterState.DeepCopy(), leaderID: c.id}
	c.clusterState = store.FsmApplyPartitionChange(tStore, change)
	return nil
}

type controllerTestFsmStore struct {
	clusterState state.Cluster
	leaderID     string
}

func (s controllerTestFsmStore) ClusterState() state.Cluster {
	return s.clusterState
}

func (s controllerTestFsmStore) LeaderID() (string, error) {
	return s.leaderID, nil
}

func (c *ControllerTestStore) setNode(node state.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusterState.Nodes[node.Id] = node
}

func (c *ControllerTestStore) setDesiredPartitions(desired uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusterState.Config.DesiredPartitions = desired
}

func testPoll(t *testing.T, f func() bool, checkPeriod time.Duration, timeout time.Duration, msgAndArgs ...any) {
	t.Helper()
	tck := time.NewTicker(checkPeriod)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if f() {
				return
			}
		case <-tmr.C:
			if len(msgAndArgs) > 0 {
				t.Fatalf(msgAndArgs[0].(string), msgAndArgs[1:]...)
			} else {
				t.Fatalf("timeout expired: %s", t.Name())
			}
		}
	}
}

// setupControllerTestCluster starts a node mux and a ControllerTestStore
// backed test server, returning the store and a client manager wired to it.
func setupControllerTestCluster(t *testing.T) (*ControllerTestStore, *client.ClientManager, *tcp.Mux) {
	t.Helper()
	mux, ln, err := network.NewNodeMux("")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ln.Close())
	})

	addr := ln.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	tStore := &ControllerTestStore{
		id:   "test-node-1",
		addr: fmt.Sprintf("127.0.0.1:%s", port),
		clusterState: state.Cluster{
			Config: state.ClusterConfig{
				DesiredPartitions: 1,
			},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
		leader: true,
	}
	srvLn := network.NewZenBpmClusterListener(mux)
	srv := server.New(srvLn, tStore, nil, nil)
	require.NoError(t, srv.Open())

	clientMgr := client.NewClientManager(tStore)
	return tStore, clientMgr, mux
}

// writeBrokenMigrationFixture copies the production migrations and appends a
// broken final migration, so that RunMigrations fails after creating the full
// schema needed by Engine.Start.
func writeBrokenMigrationFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	sourceDir := filepath.Join(filepath.Dir(currentFile), "..", "..", "sql", "migrations")
	entries, err := os.ReadDir(sourceDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(sourceDir, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, entry.Name()), content, 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "9999_broken.up.sql"), []byte("THIS IS NOT VALID SQL;"), 0o600))
	return dir
}

func repairBrokenMigrationFixture(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "9999_broken.up.sql"), []byte("SELECT 1;"), 0o600))
}
