package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// ZenNode is the main node of the zen cluster.
// It serves as a controller of the underlying RqLite clusters as partitions of the main cluster.
//
//	     ┌─────────────────┐   ┌─────────────────┐
//	     │Zen cluster 1    │   │Zen cluster 1    │
//	     │ ┌────────────┐  ◄───┼ ┌────────────┐  │
//	     │ │Partition 1 │  ┼───► │Partition 1 │  │
//	     │ │RqLite 1    ┼──┼───┼─►RqLite 1    │  │
//	┌────┼ │            ◄──┼───┼─┼            │  ┼──┐
//	│┌───► └────────────┘  │   │ └────────────┘  ◄─┐│
//	││   └───────┬───▲─────┘   └───▲───┬─────────┘ ││
//	││   ┌───────▼───┼─────┐   ┌───┴───▼─────────┐ ││
//	││   │Zen cluster 1    │   │Zen cluster 1    │ ││
//	││   │ ┌────────────┐  │   │ ┌────────────┐  │ ││
//	││   │ │Partition 2 │  │   │ │Partition 2 │  │ ││
//	││   │ │RqLite 2    ┼──┼───┼─►RqLite 2    │  │ ││
//	││   │ │            ◄──┼───┼─┼            │  │ ││
//	││   │ └────────────┘  │   │ └────────────┘  │ ││
//	││   └──────▲─┬────────┘   └─┬──▲────────────┘ ││
//	│└──────────┼─┼──────────────┘  │              ││
//	└───────────┼─┼─────────────────┘              ││
//	            │ └────────────────────────────────┘│
//	            └───────────────────────────────────┘
type ZenNode struct {
	partitions []*ZenPartitionNode
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	rqLiteConfig := conf.RqLite
	if conf.RqLite == nil {
		defaultConfig := GetDefaultConfig("partition-1", "localhost:9091", "localhost:9081", "./partition-1", []string{})
		rqLiteConfig = &defaultConfig
	}
	err := rqLiteConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate rqLite configuration: %w", err)
	}
	partition, err := StartZenPartitionNode(mainCtx, rqLiteConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start zen partition: %w", err)
	}
	return &ZenNode{
		partitions: []*ZenPartitionNode{partition},
	}, nil
}

func (node *ZenNode) Stop() error {
	var joinErr error
	for _, partition := range node.partitions {
		err := partition.Stop()
		if err != nil {
			joinErr = errors.Join(joinErr, err)
		}
	}
	return joinErr
}

func (node *ZenNode) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	return node.partitions[0].Query(ctx, req)
}

// Execute TODO: this needs to implement that only the leader can execute
func (node *ZenNode) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return node.partitions[0].Execute(ctx, req)
}

func (node *ZenNode) IsLeader(ctx context.Context) bool {
	return node.partitions[0].IsLeader(ctx)
}
