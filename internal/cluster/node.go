package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/tcp"
)

// ZenNode is the main node of the zen cluster.
// It serves as a controller of the underlying RqLite clusters as partitions of the main cluster.
//
//	     ┌─────────────────┐   ┌─────────────────┐
//	     │Zen cluster 1(L) │   │Zen cluster 1(F) │
//	     │ ┌────────────┐  ◄───┼ ┌────────────┐  │
//	     │ │Partition 1 │  ┼───► │Partition 1 │  │
//	     │ │RqLite 1 (L)┼──┼───┼─►RqLite 1 (F)│  │
//	┌────┼ │            ◄──┼───┼─┼            │  ┼──┐
//	│┌───► └────────────┘  │   │ └────────────┘  ◄─┐│
//	││   └───────┬───▲─────┘   └───▲───┬─────────┘ ││
//	││   ┌───────▼───┼─────┐   ┌───┴───▼─────────┐ ││
//	││   │Zen cluster 1(F) │   │Zen cluster 1(F) │ ││
//	││   │ ┌────────────┐  │   │ ┌────────────┐  │ ││
//	││   │ │Partition 2 │  │   │ │Partition 2 │  │ ││
//	││   │ │RqLite 2 (L)┼──┼───┼─►RqLite 2 (F)│  │ ││
//	││   │ │            ◄──┼───┼─┼            │  │ ││
//	││   │ └────────────┘  │   │ └────────────┘  │ ││
//	││   └──────▲─┬────────┘   └─┬──▲────────────┘ ││
//	│└──────────┼─┼──────────────┘  │              ││
//	└───────────┼─┼─────────────────┘              ││
//	            │ └────────────────────────────────┘│
//	            └───────────────────────────────────┘
//
// Changes in the cluster are always directed towards a leader of the Zen cluster.
// When leader decides that this change should take place it writes it into the raft log.
// Change takes effect only after it has been applied to the state from the raft log.
type ZenNode struct {
	store      *store.Store
	controller *controller
	server     *server.Server
	client     *client.ClientManager
	logger     hclog.Logger
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	node := ZenNode{
		logger: hclog.Default().Named(fmt.Sprintf("zen-node-%s", conf.Cluster.NodeId)),
	}

	mux, err := network.NewNodeMux(conf.Cluster.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZenNode mux on %s: %w", conf.Cluster.Addr, err)
	}

	zenRaftLn := network.NewZenBpmRaftListener(mux)
	raftTn := tcp.NewLayer(zenRaftLn, network.NewZenBpmRaftDialer())
	node.store = store.New(raftTn, store.DefaultConfig(conf.Cluster))
	if err = node.store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	clusterSrvLn := network.NewZenBpmClusterListener(mux)
	clusterSrv := server.New(clusterSrvLn, node.store)
	if err = clusterSrv.Open(); err != nil {
		return nil, fmt.Errorf("failed to open cluster GRPC server: %w", err)
	}
	node.server = clusterSrv

	node.controller, err = NewController(node.store, mux, conf.Cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to create node controller: %w", err)
	}

	err = node.controller.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start node controller: %w", err)
	}

	node.client = client.NewClientManager(node.store)

	// bootstrapping logic
	nodes, err := node.store.Nodes()
	if err != nil {
		errInfo := fmt.Errorf("failed to get nodes %s", err.Error())
		node.logger.Error(errInfo.Error())
		return nil, errInfo
	}
	if len(nodes) > 0 {
		node.logger.Info("Preexisting configuration detected. Skipping bootstrap.")
		return &node, nil
	}

	bootDoneFn := func() bool {
		leader, _ := node.store.LeaderAddr()
		return leader != ""
	}
	clusterSuf := cluster.VoterSuffrage(!conf.Cluster.Raft.NonVoter)

	joiner := NewJoiner(node.client, conf.Cluster.Raft.JoinAttempts, conf.Cluster.Raft.JoinInterval)
	if len(conf.Cluster.Raft.JoinAddresses) > 0 && conf.Cluster.Raft.BootstrapExpect == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(mainCtx, conf.Cluster.Raft.JoinAddresses, node.store.ID(), conf.Cluster.Adv, clusterSuf)
		if err != nil {
			return nil, fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		node.logger.Info(fmt.Sprintf("successfully joined cluster at %s", j))
		return &node, nil
	}

	if len(conf.Cluster.Raft.JoinAddresses) > 0 && conf.Cluster.Raft.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := NewBootstrapper(
			cluster.NewAddressProviderString(conf.Cluster.Raft.JoinAddresses),
			node.client,
		)
		err := bs.Boot(mainCtx, node.store.ID(), conf.Cluster.Adv, clusterSuf, bootDoneFn, conf.Cluster.Raft.BootstrapExpectTimeout)
		return &node, err

	}

	return &node, nil
}

func (node *ZenNode) Stop() error {
	var joinErr error
	err := node.controller.NotifyShutdown()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to notify cluster about node shutdown: %w", err))
	}
	err = node.controller.Stop()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop node controller: %w", err))
	}
	err = node.server.Close()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop grpc server: %w", err))
	}
	err = node.client.Close()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to close client manager: %w", err))
	}
	err = node.store.Close(true)
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to close zen node store: %w", err))
	}
	return joinErr
}

func (node *ZenNode) IsPartitionLeader(ctx context.Context, partition uint32) bool {
	return node.controller.IsPartitionLeader(ctx, partition)
}

func (node *ZenNode) IsAnyPartitionLeader(ctx context.Context) bool {
	return node.controller.IsAnyPartitionLeader(ctx)
}

// GetPartitionStore exposes Storage interface for use in engines
func (node *ZenNode) GetPartitionStore(ctx context.Context, partition uint32) (storage.Storage, error) {
	partitionNode, ok := node.controller.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d storage not available in zen node", partition)
	}
	return partitionNode.rqliteDB, nil
}

// GetPartitionDB exposes DBTX interface for use in internal packages
func (node *ZenNode) GetPartitionDB(ctx context.Context, partition uint32) (sql.DBTX, error) {
	partitionNode, ok := node.controller.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d storage not available in zen node", partition)
	}
	return partitionNode.rqliteDB, nil
}
