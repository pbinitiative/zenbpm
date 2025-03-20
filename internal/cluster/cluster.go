package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/rqlite/rqlite/v8/command/proto"
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
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	node := ZenNode{}

	mux, err := network.NewMux(conf.Cluster.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZenNode mux on %s: %w", conf.Cluster.RaftAddr, err)
	}

	zenRaftLn := network.NewZenBpmRaftListener(mux)
	raftTn := tcp.NewLayer(zenRaftLn, network.NewZenBpmRaftDialer())
	node.store = store.New(raftTn, store.DefaultConfig(conf.Cluster))
	if err = node.store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	sLn := network.NewZenBpmClusterListener(mux)
	clusterSrv := server.New(sLn, node.store)
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
	err = node.store.Close(true)
	joinErr = errors.Join(joinErr, err)
	return joinErr
}

func (node *ZenNode) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	return node.controller.partitions[0].Query(ctx, req)
}

// Execute TODO: this needs to implement that only the leader can execute
func (node *ZenNode) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return node.controller.partitions[0].Execute(ctx, req)
}

func (node *ZenNode) IsLeader(ctx context.Context) bool {
	return node.controller.partitions[0].IsLeader(ctx)
}

func (node *ZenNode) IsPartitionLeader(ctx context.Context, partition uint32) bool {
	return node.controller.IsPartitionLeader(ctx, partition)
}

func (node *ZenNode) IsAnyPartitionLeader(ctx context.Context) bool {
	return node.controller.IsAnyPartitionLeader(ctx)
}
