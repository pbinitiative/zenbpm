package jobmanager

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientReachesLeaderElectedAfterStart(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	_, completer := createServerNode(t, 1, listener, serverStore)
	loader := completer.loader

	// node-2 starts before any partition leader is known.
	clientStore := &testStore{
		state:  *serverStore.state.DeepCopy(),
		nodeId: "node-2",
	}
	clientStore.state.Partitions[1] = state.Partition{Id: 1, LeaderId: ""}

	clientManager := createClientNode(t, clientStore)
	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job"))

	// Leadership appears after the client subscription already exists.
	clientStore.state.Partitions[1] = state.Partition{Id: 1, LeaderId: "node-1"}
	node1 := clientStore.state.Nodes["node-1"]
	node1.Partitions = map[uint32]state.NodePartition{
		1: {Id: 1, State: state.NodePartitionStateInitialized, Role: state.RoleLeader},
	}
	clientStore.state.Nodes["node-1"] = node1
	clientManager.OnClusterStateChange(t.Context())

	generatedJobs := generateJobs(1)
	loader.addJobs(generatedJobs...)

	select {
	case job := <-clientJobs:
		assert.Equal(t, generatedJobs[0].Key, job.Key)
	case <-time.After(2 * time.Second):
		t.Fatal("client never received a job after partition leader became available")
	}
}
