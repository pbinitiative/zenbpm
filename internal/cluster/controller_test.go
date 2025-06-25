package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestControllerCanStartNewPartitions(t *testing.T) {
	mux, ln, err := network.NewNodeMux("")
	assert.NoError(t, err)
	go func() {
		err = mux.Serve()
		assert.NoError(t, err)
	}()

	addr := ln.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	assert.NoError(t, err)

	tStore := &controllerTestStore{
		id:   "test-node-1",
		addr: fmt.Sprintf("127.0.0.1:%s", port),
		clusterState: store.ClusterState{
			Config: store.ClusterConfig{
				DesiredPartitions: 1,
			},
			Partitions: map[uint32]store.Partition{},
			Nodes:      map[string]store.Node{},
		},
		leader: true,
	}
	srvLn := network.NewZenBpmClusterListener(mux)
	srv := server.New(srvLn, tStore, nil)
	err = srv.Open()
	assert.NoError(t, err)

	clientMgr := client.NewClientManager(tStore)
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
	defer controller.Stop()

	// add node to the cluster state
	tStore.clusterState.Nodes[tStore.id] = store.Node{
		Id:         tStore.id,
		Addr:       tStore.addr,
		Suffrage:   raft.Voter,
		State:      store.NodeStateStarted,
		Role:       store.RoleLeader,
		Partitions: map[uint32]store.NodePartition{},
	}

	controller.ClusterStateChangeNotification(t.Context())
	// verify that controller updated state so that new partition needs to be created by a node
	state := controller.store.ClusterState()
	assert.Equal(t, store.NodePartitionStateInitialized, state.Nodes[tStore.id].Partitions[1].State)

	// verify that partition was started
	testPoll(t, func() bool {
		s := controller.store.ClusterState()
		if tStore.id == s.Partitions[1].LeaderId &&
			store.NodePartitionStateInitialized == s.Nodes[tStore.id].Partitions[1].State {
			return true
		}
		return false
	}, 100*time.Millisecond, 5*time.Second, "Failed to verify that partition was started. State was: %s", controller.store.ClusterState().Nodes[tStore.id].Partitions[1].State)

	// update desired partition count
	tStore.clusterState.Config.DesiredPartitions = 2

	controller.ClusterStateChangeNotification(t.Context())
	// verify that new partition was created
	testPoll(t, func() bool {
		s := controller.store.ClusterState()
		if tStore.id == s.Partitions[2].LeaderId &&
			store.NodePartitionStateInitialized == s.Nodes[tStore.id].Partitions[2].State {
			return true
		}
		return false
	}, 100*time.Millisecond, 10*time.Second, "Failed to verify that second partition was started. State was: %s", controller.store.ClusterState().Nodes[tStore.id].Partitions[2].State)
}

type controllerTestStore struct {
	id           string
	addr         string
	clusterState store.ClusterState
	leader       bool
}

// Addr implements ControlledStore.
func (c *controllerTestStore) Addr() string {
	return c.addr
}

// ClusterState implements ControlledStore.
func (c *controllerTestStore) ClusterState() store.ClusterState {
	return c.clusterState
}

// ID implements ControlledStore.
func (c *controllerTestStore) ID() string {
	return c.id
}

// IsLeader implements ControlledStore.
func (c *controllerTestStore) IsLeader() bool {
	return c.leader
}

// LeaderWithID implements client.ClientStore.
func (c *controllerTestStore) LeaderWithID() (string, string) {
	return c.addr, c.id
}

// PartitionLeaderWithID implements client.ClientStore.
func (c *controllerTestStore) PartitionLeaderWithID(partition uint32) (string, string) {
	return c.addr, c.id
}

// Role implements ControlledStore.
func (c *controllerTestStore) Role() proto.Role {
	if c.leader == true {
		return proto.Role_ROLE_TYPE_LEADER
	} else {
		return proto.Role_ROLE_TYPE_FOLLOWER
	}
}

// LeaderID implements store.FsmStore.
func (c *controllerTestStore) LeaderID() (string, error) {
	return c.id, nil
}

// Join implements server.StoreService.
func (c *controllerTestStore) Join(jr *zenproto.JoinRequest) error {
	panic("unexpected call to Join")
}

// Notify implements server.StoreService.
func (c *controllerTestStore) Notify(nr *zenproto.NotifyRequest) error {
	panic("unexpected call to Notify")
}

// WriteNodeChange implements server.StoreService.
func (c *controllerTestStore) WriteNodeChange(change *proto.NodeChange) error {
	c.clusterState = store.FsmApplyNodeChange(c, change)
	return nil
}

// WritePartitionChange implements ControlledStore.
func (c *controllerTestStore) WritePartitionChange(change *proto.NodePartitionChange) error {
	c.clusterState = store.FsmApplyPartitionChange(c, change)
	return nil
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
