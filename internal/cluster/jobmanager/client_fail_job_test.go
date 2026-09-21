package jobmanager

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientFailJobNamesTheFailingClient shows a job failure reaches the
// leader with the client id of the worker, as a completion does, so the
// leader can tell the lock holder from another client.
func TestClientFailJobNamesTheFailingClient(t *testing.T) {
	mux, nodeListener, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeListener.Close()) }()
	listener := network.NewZenBpmClusterListener(mux)

	serverStore := getTestStore(listener)
	_, completer, leaderGRPC := createServerNodeWithGRPC(t, listener, serverStore)
	clientManager := createClientNode(t, serverStore.forNode("node-2"))

	clientJobs := make(chan Job)
	require.NoError(t, clientManager.AddClient(t.Context(), "client-1", clientJobs))
	require.NoError(t, clientManager.AddClientJobSub(t.Context(), "client-1", "test-job", SubscriptionSettings{}))
	completer.loader.addJobs(generateJobs(1)...)
	job := <-clientJobs

	require.NoError(t, clientManager.FailJobReq(t.Context(), "client-1", job.Key, "boom", nil, nil))

	requests := leaderGRPC.receivedFailRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "client-1", requests[0].GetClientId(), "the failure must name the worker which failed the job")
	assert.Equal(t, job.Key, requests[0].GetKey())
	assert.Contains(t, completer.failedJobs, job.Key)
}
