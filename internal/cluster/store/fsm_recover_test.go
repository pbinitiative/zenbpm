package store

import (
	"bytes"
	"context"
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
	var logBuf bytes.Buffer
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
		appliedTarget: rsync.NewReadyTarget[uint64](),
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
		return bytes.Contains(logBuf.Bytes(), []byte("cluster-state-change-observer"))
	}, time.Second, 10*time.Millisecond, "panic should be recovered and logged by safego.Go")
}
