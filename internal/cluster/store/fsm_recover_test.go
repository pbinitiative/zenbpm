package store

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/rqlitecompat/rsync"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
)

func TestApplyRecoversObserverPanic(t *testing.T) {
	var logBuf synchronizedBuffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &logBuf,
		Level:  hclog.Error,
	})

	s := &Store{
		open:   &atomic.Bool{},
		logger: logger,
		state: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
		appliedTarget:              rsync.NewReadyTarget[uint64](),
		clusterStateChangeObserver: func(ctx context.Context) {},
	}

	observerRan := make(chan struct{})
	f := &FSM{
		store: s,
		clusterStateChangeObserver: func(ctx context.Context) {
			defer close(observerRan)
			panic("boom in cluster state change observer")
		},
	}

	nodeID := "test-node"
	cmd := &proto.Command{
		Type: proto.Command_TYPE_NODE_CHANGE.Enum(),
		Request: &proto.Command_NodeChange{
			NodeChange: &proto.NodeChange{
				NodeId: &nodeID,
				State:  proto.NodeState_NODE_STATE_STARTED.Enum(),
			},
		},
	}
	data, err := pb.Marshal(cmd)
	assert.NoError(t, err)

	f.Apply(&raft.Log{Index: 1, Data: data})

	select {
	case <-observerRan:
	case <-time.After(2 * time.Second):
		t.Fatal("observer goroutine spawned by Apply did not run")
	}

	assert.Eventually(t, func() bool {
		return logBuf.Contains([]byte("cluster-state-change-observer"))
	}, time.Second, 10*time.Millisecond, "panic should be recovered and logged by safego.Go")
}

func TestFsmApplyPartitionChangeDemotesPreviousLeader(t *testing.T) {
	partitionID := uint32(1)
	clusterState := state.Cluster{
		Partitions: map[uint32]state.Partition{partitionID: {Id: partitionID, LeaderId: "old"}},
		Nodes: map[string]state.Node{
			"old": {Id: "old", Partitions: map[uint32]state.NodePartition{
				partitionID: {Id: partitionID, Role: state.RoleLeader, State: state.NodePartitionStateInitialized},
			}},
			"new": {Id: "new", Partitions: map[uint32]state.NodePartition{
				partitionID: {Id: partitionID, Role: state.RoleFollower, State: state.NodePartitionStateInitialized},
			}},
		},
	}
	store := staticFsmStore{clusterState: clusterState}

	result := FsmApplyPartitionChange(store, &proto.NodePartitionChange{
		NodeId: new("new"), PartitionId: new(partitionID),
		State: proto.NodePartitionState_NODE_PARTITION_STATE_INITIALIZING.Enum(),
		Role:  proto.Role_ROLE_TYPE_LEADER.Enum(),
	})

	assert.Equal(t, "new", result.Partitions[partitionID].LeaderId)
	assert.Equal(t, state.RoleFollower, result.Nodes["old"].Partitions[partitionID].Role)
	assert.Equal(t, state.RoleLeader, result.Nodes["new"].Partitions[partitionID].Role)
}

type synchronizedBuffer struct {
	mu sync.RWMutex
	b  bytes.Buffer
}

type staticFsmStore struct {
	clusterState state.Cluster
}

func (s staticFsmStore) LeaderID() (string, error)   { return "", nil }
func (s staticFsmStore) ClusterState() state.Cluster { return *s.clusterState.DeepCopy() }

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *synchronizedBuffer) Contains(value []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return bytes.Contains(b.b.Bytes(), value)
}
