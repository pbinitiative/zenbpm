package store

import (
	"sync"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

func TestClusterStateConcurrentWithFSMApply(t *testing.T) {
	s := &Store{
		state: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	fsm := NewFSM(s)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10_000; i++ {
			nodeID := "node-1"
			partitionID := uint32(i % 2)
			partitionState := proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZED
			fsm.applyPartitionChange(&proto.NodePartitionChange{
				NodeId:      &nodeID,
				PartitionId: &partitionID,
				State:       &partitionState,
			})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10_000; i++ {
			cs := s.ClusterState()
			_ = len(cs.Nodes)
			_ = len(cs.Partitions)
		}
	}()

	close(start)
	wg.Wait()
}
