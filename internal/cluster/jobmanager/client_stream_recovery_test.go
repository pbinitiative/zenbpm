package jobmanager

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientResubscribesAfterStreamLoss(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	serverManager, completer := createServerNode(t, 1, listener, serverStore)
	loader := completer.loader

	clientStore := &testStore{
		state:  *serverStore.state.DeepCopy(),
		nodeId: "node-2",
	}
	clientManager := createClientNode(t, clientStore)

	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job"))

	firstBatch := generateJobs(1)
	loader.addJobs(firstBatch...)
	firstJob := <-clientJobs
	assert.Equal(t, firstBatch[0].Key, firstJob.Key)
	require.NoError(t, clientManager.CompleteJobReq(t.Context(), "client-1", firstJob.Key, nil))

	// Restart the leader-side server without publishing a role change to node-2.
	serverStore.state.Partitions = map[uint32]state.Partition{}
	serverManager.OnPartitionRoleChange(t.Context())
	assert.Nil(t, serverManager.server)

	*serverStore = *getTestStore(listener)
	serverManager.OnPartitionRoleChange(t.Context())
	require.NotNil(t, serverManager.server)

	secondBatch := generateJobs(1)
	loader.addJobs(secondBatch...)
	select {
	case job := <-clientJobs:
		assert.Equal(t, secondBatch[0].Key, job.Key)
	case <-time.After(10 * time.Second):
		t.Fatal("client never received a job after the leader stream was lost and restored")
	}
}
