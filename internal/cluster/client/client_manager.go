package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"google.golang.org/grpc"
)

const (
	dialTimeout = 5 * time.Second
)

type ClientManager struct {
	// mu is a mutext for protecting activeClients
	mu sync.RWMutex
	// TODO: clear map of clients after its node is shut down
	activeClients map[string]proto.ZenServiceClient

	store *store.Store
}

func New(store *store.Store) *ClientManager {
	return &ClientManager{
		activeClients: map[string]proto.ZenServiceClient{},
		store:         store,
		mu:            sync.RWMutex{},
	}
}

func (c *ClientManager) ClusterLeader() (proto.ZenServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	leaderAddr, leaderId := c.store.LeaderWithID()
	var err error
	if leaderAddr == "" {
		return nil, fmt.Errorf("failed to get leader address for cluster client: no leader available")
	}
	client, ok := c.activeClients[leaderId]
	if !ok {
		client, err = c.newClient(leaderId, leaderAddr)
	}
	return client, err
}

func (c *ClientManager) PartitionLeader(partition uint32) (proto.ZenServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	leaderAddr, leaderId := c.store.PartitionLeaderWithID(partition)
	var err error
	if leaderAddr == "" {
		return nil, fmt.Errorf("failed to get leader address for cluster client: no leader available")
	}
	client, ok := c.activeClients[leaderId]
	if !ok {
		client, err = c.newClient(leaderId, leaderAddr)
	}
	return client, err
}

func (c *ClientManager) newClient(nodeId string, nodeAddr string) (proto.ZenServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dialer := network.NewZenBpmClusterDialer()
	grpcClient, err := grpc.NewClient(nodeAddr, grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return dialer.Dial(s, dialTimeout)
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to create new GRPC client")
	}
	zsc := proto.NewZenServiceClient(grpcClient)
	c.activeClients[nodeId] = zsc
	return zsc, nil
}
