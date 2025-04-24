package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dialTimeout = 5 * time.Second
)

type clientData struct {
	conn *grpc.ClientConn
	c    proto.ZenServiceClient
}

type ClientManager struct {
	// mu is a mutext for protecting activeClients
	mu sync.RWMutex

	activeClients map[string]clientData

	store *store.Store
}

func NewClientManager(store *store.Store) *ClientManager {
	return &ClientManager{
		activeClients: map[string]clientData{},
		store:         store,
		mu:            sync.RWMutex{},
	}
}

func (c *ClientManager) Close() error {
	var joinErr error
	for _, cd := range c.activeClients {
		err := cd.conn.Close()
		if err != nil {
			joinErr = errors.Join(joinErr, fmt.Errorf("failed to close client %s: %w", cd.conn.Target(), err))
		}
	}
	return joinErr
}

func (c *ClientManager) ClusterLeader() (proto.ZenServiceClient, error) {
	leaderAddr, _ := c.store.LeaderWithID()
	var err error
	if leaderAddr == "" {
		return nil, fmt.Errorf("failed to get leader address for cluster client: no leader available")
	}
	c.mu.RLock()
	client, ok := c.activeClients[leaderAddr]
	c.mu.RUnlock()
	if !ok {
		return c.newClient(leaderAddr)
	}
	return client.c, err
}

func (c *ClientManager) PartitionLeader(partition uint32) (proto.ZenServiceClient, error) {
	leaderAddr, _ := c.store.PartitionLeaderWithID(partition)
	var err error
	if leaderAddr == "" {
		return nil, fmt.Errorf("failed to get leader address for cluster client: no leader available")
	}
	c.mu.RLock()
	client, ok := c.activeClients[leaderAddr]
	c.mu.RUnlock()
	if !ok {
		return c.newClient(leaderAddr)
	}
	return client.c, err
}

func (c *ClientManager) For(targetAddr string) (proto.ZenServiceClient, error) {
	c.mu.RLock()
	client, ok := c.activeClients[targetAddr]
	c.mu.RUnlock()
	if !ok {
		return c.newClient(targetAddr)
	}
	return client.c, nil
}

func (c *ClientManager) newClient(nodeAddr string) (proto.ZenServiceClient, error) {
	dialer := network.NewZenBpmClusterDialer()
	grpcClient, err := grpc.NewClient(nodeAddr,
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return dialer.Dial(s, dialTimeout)
		}),
		// TODO: add credentials at later stage
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new GRPC client: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeClients[nodeAddr] = clientData{
		conn: grpcClient,
		c:    proto.NewZenServiceClient(grpcClient),
	}
	return c.activeClients[nodeAddr].c, nil
}
