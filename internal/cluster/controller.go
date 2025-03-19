package cluster

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/rqlite/rqlite/v8/tcp"
)

type controller struct {
	partitions   []*ZenPartitionNode
	store        *store.Store
	config       config.Cluster
	rqLiteConfig config.RqLite
	mux          *tcp.Mux
}

func NewController(s *store.Store, mux *tcp.Mux, conf config.Cluster) (*controller, error) {
	c := controller{
		store:      s,
		config:     conf,
		mux:        mux,
		partitions: []*ZenPartitionNode{},
	}
	return &c, nil
}

func (c *controller) Start() error {
	rqLiteConfig := c.config.RqLite

	if c.config.RqLite == nil {
		defaultConfig := GetDefaultConfig(c.store.ID(), c.store.Addr(), c.store.ID(), []string{})
		rqLiteConfig = &defaultConfig
	}
	err := rqLiteConfig.Validate()
	if err != nil {
		return fmt.Errorf("failed to start controller, rqLite config validation failed: %w", err)
	}
	c.rqLiteConfig = *rqLiteConfig
	// TODO: run in goroutine once engine is created here and server consumes ZenNode
	c.monitor()
	// go c.monitor()
	return nil
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
	// TODO: actually implement controller loop instead of just manually creating partitions
	c.startPartition(context.TODO(), 1)
	// for {
	// }
}

func (c *controller) startPartition(ctx context.Context, partitionId uint32) error {
	rqLiteConfig := c.config.RqLite

	if c.config.RqLite == nil {
		defaultConfig := GetDefaultConfig(c.store.ID(), c.store.Addr(), c.store.ID(), []string{})
		rqLiteConfig = &defaultConfig
	}
	err := rqLiteConfig.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate rqLite configuration: %w", err)
	}
	partitionConf := *rqLiteConfig
	partitionConf.NodeID = fmt.Sprintf("zen-%s-partition-%d", c.store.ID(), partitionId)
	partitionConf.DataPath = path.Join(c.config.RaftDir, fmt.Sprintf("partition-%d", partitionId))
	partition, err := StartZenPartitionNode(ctx, c.mux, &partitionConf, partitionId)
	if err != nil {
		return fmt.Errorf("failed to start zen partition %d: %w", partitionId, err)
	}
	c.partitions = append(c.partitions, partition)
	return nil
}

// NotifyShutdown notifies the cluster leader / store that the node is shutting down
func (c *controller) NotifyShutdown() error {
	// TODO: call zen cluster api on a leader to notify about node shutdown
	return nil
}
