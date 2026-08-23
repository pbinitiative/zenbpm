package jobmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	partition = uint32(1)
	gen, _    = snowflake.NewNode(int64(partition))
)

func TestServerDropsOnlyClosingNodeStreamClientsFromRoundRobin(t *testing.T) {
	server := newJobServer("node-1", nil, nil)
	oldStream := &nodeSub{nodeID: "node-2"}
	server.nodeSubs["node-2"] = oldStream

	server.subscribeClient("node-2", "client-1", "test-job")
	assert.Equal(t, []ClientID{"client-1"}, server.jobTypes["test-job"].clients)

	// node-2 reconnects and replays its subscriptions before the old stream exits
	replacementStream := &nodeSub{nodeID: "node-2"}
	server.nodeSubs["node-2"] = replacementStream
	server.subscribeClient("node-2", "client-1", "test-job")
	assert.Equal(t, []ClientID{"client-1"}, server.jobTypes["test-job"].clients,
		"replayed subscription must not duplicate the client in the round robin list")

	server.removeNode(oldStream)
	assert.Same(t, replacementStream, server.nodeSubs["node-2"])
	assert.Same(t, replacementStream, server.subscriptions["test-job"]["client-1"])
	assert.Equal(t, []ClientID{"client-1"}, server.jobTypes["test-job"].clients)

	server.distributedJobs = append(server.distributedJobs, distributedJob{
		sentTime: time.Now(),
		client:   "client-1",
		jobKey:   gen.Generate().Int64(),
	})
	server.removeNode(replacementStream)
	assert.Empty(t, server.jobTypes["test-job"].clients)
	assert.Empty(t, server.subscriptions["test-job"])
	assert.Empty(t, server.distributedJobs, "jobs assigned to disconnected clients must become immediately eligible again")
}

func TestServerRespectsPerClientCapacityWithinBatch(t *testing.T) {
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
	}
	completer := &testCompleter{
		completedJobs: []int64{},
		loader:        loader,
	}
	server, stream := newTestJobServer(t, loader, completer)
	server.subscribeClient("node-2", "client-1", "test-job")
	server.subscribeClient("node-2", "client-2", "test-job")

	// client-1 already holds all but one of its slots, client-2 holds none
	now := time.Now()
	for range maxActiveJobsPerClient - 1 {
		server.distributedJobs = append(server.distributedJobs, distributedJob{
			sentTime: now,
			client:   "client-1",
			jobKey:   gen.Generate().Int64(),
		})
	}

	// enough jobs to fill the remaining capacity of both clients
	generatedJobs := generateJobs(int(maxActiveJobsPerClient) + 1)
	loader.addJobs(generatedJobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.totalSent() == len(generatedJobs)
	}, 5*time.Second, 10*time.Millisecond, "expected all jobs to be distributed")
	cancel()

	assert.Equal(t, 1, stream.sentTo("client-1"), "client-1 must not be assigned more jobs than its remaining capacity within a batch")
	assert.Equal(t, int(maxActiveJobsPerClient), stream.sentTo("client-2"), "client-2 should receive the rest of the batch")
}

func TestServerDistributesToSingleClient(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "test-job")

	generatedJobs := generateJobs(3)
	loader.addJobs(generatedJobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-1") == len(generatedJobs)
	}, 5*time.Second, 10*time.Millisecond, "a single subscribed client must receive all jobs")
}

func TestServerRoundRobinUsesBothClients(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "test-job")
	server.subscribeClient("node-2", "client-2", "test-job")

	generatedJobs := generateJobs(10)
	loader.addJobs(generatedJobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.totalSent() == len(generatedJobs)
	}, 5*time.Second, 10*time.Millisecond, "expected all jobs to be distributed")
	cancel()

	assert.Equal(t, 5, stream.sentTo("client-1"), "round robin must distribute jobs evenly between two clients")
	assert.Equal(t, 5, stream.sentTo("client-2"), "round robin must distribute jobs evenly between two clients")
}

func TestServerDistributesFairlyBetweenMultipleClients(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	clientIDs := []ClientID{"client-1", "client-2", "client-3"}
	for _, clientID := range clientIDs {
		server.subscribeClient("node-2", clientID, "test-job")
	}

	generatedJobs := generateJobs(9)
	loader.addJobs(generatedJobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.totalSent() == len(generatedJobs)
	}, 5*time.Second, 10*time.Millisecond, "expected all jobs to be distributed")
	cancel()

	for _, clientID := range clientIDs {
		assert.Equal(t, 3, stream.sentTo(clientID), "round robin must distribute jobs evenly between eligible clients")
	}
}

func TestServerNeverAssignsJobsToSaturatedClients(t *testing.T) {
	loadMu := sync.Mutex{}
	loadedJobTypes := make([][]string, 0)
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(jobTypes []string, _ []int64, _ int64) {
			loadMu.Lock()
			loadedJobTypes = append(loadedJobTypes, slices.Clone(jobTypes))
			loadMu.Unlock()
		},
	}
	server, stream := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "job-a")
	server.subscribeClient("node-2", "client-2", "job-b")

	// client-1 holds all of its slots
	now := time.Now()
	for range maxActiveJobsPerClient {
		server.distributedJobs = append(server.distributedJobs, distributedJob{
			sentTime: now,
			client:   "client-1",
			jobKey:   gen.Generate().Int64(),
		})
	}

	jobsA := generateJobsOfType(int(maxActiveJobsPerClient), "job-a")
	jobsB := generateJobsOfType(2, "job-b")
	loader.addJobs(jobsA...)
	loader.addJobs(jobsB...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-2") == len(jobsB)
	}, 5*time.Second, 10*time.Millisecond, "client-2 must receive all job-b jobs")
	cancel()

	assert.Zero(t, stream.sentTo("client-1"), "a saturated client must never receive additional jobs")
	loadMu.Lock()
	defer loadMu.Unlock()
	require.NotEmpty(t, loadedJobTypes)
	assert.NotContains(t, loadedJobTypes[0], "job-a", "types with no remaining client capacity must not consume the query limit")
	assert.Contains(t, loadedJobTypes[0], "job-b")
}

func TestServerRetriesJobImmediatelyAfterTransientSendFailure(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	stream.failSends = 1
	server.subscribeClient("node-2", "client-1", "test-job")
	loader.addJobs(generateJobs(1)...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-1") == 1
	}, 5*time.Second, 10*time.Millisecond, "a failed send must not lock the undelivered job for the full lock duration")
	cancel()

	assert.GreaterOrEqual(t, stream.sendAttempts(), 2)
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	assert.Len(t, server.distributedJobs, 1, "only the successful delivery may retain a reservation")
}

func TestServerSkipListContainsOnlyValidJobKeys(t *testing.T) {
	skipListMu := sync.Mutex{}
	skipLists := make([][]int64, 0)
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(_ []string, idsToSkip []int64, _ int64) {
			skipListMu.Lock()
			skipLists = append(skipLists, slices.Clone(idsToSkip))
			skipListMu.Unlock()
		},
	}
	server, _ := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "test-job")

	activeKey := gen.Generate().Int64()
	server.distributedJobs = []distributedJob{
		{sentTime: time.Now().Add(-2 * jobLockDuration), client: "client-1", jobKey: gen.Generate().Int64()},
		{sentTime: time.Now(), client: "client-1", jobKey: activeKey},
		{sentTime: time.Now().Add(-2 * jobLockDuration), client: "client-1", jobKey: gen.Generate().Int64()},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		skipListMu.Lock()
		defer skipListMu.Unlock()
		return len(skipLists) > 0
	}, 5*time.Second, 10*time.Millisecond, "loader must be called at least once")
	cancel()

	skipListMu.Lock()
	defer skipListMu.Unlock()
	assert.Equal(t, []int64{activeKey}, skipLists[0], "skip list must contain only keys of still-locked jobs")
	for _, skipList := range skipLists {
		assert.NotContains(t, skipList, int64(0), "expired jobs must not introduce zero IDs into the skip list")
	}
}

func TestServerHandlesSubscriptionChangesDuringDistribution(t *testing.T) {
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var loadStartedOnce sync.Once
	var releaseLoadOnce sync.Once
	release := func() {
		releaseLoadOnce.Do(func() { close(releaseLoad) })
	}
	defer release()

	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(_ []string, _ []int64, _ int64) {
			loadStartedOnce.Do(func() { close(loadStarted) })
			<-releaseLoad
		},
	}
	server, stream := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "test-job")

	generatedJobs := generateJobs(int(maxActiveJobsPerClient))
	loader.addJobs(generatedJobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		select {
		case <-loadStarted:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "distribution must start loading jobs")

	// Replace the only eligible client while a batch is being loaded. The new
	// client has no capacity in the in-flight snapshot and becomes eligible on
	// the next distribution iteration.
	server.unsubscribeClient("client-1", "test-job")
	server.subscribeClient("node-2", "client-2", "test-job")
	release()

	assert.Eventually(t, func() bool {
		return stream.totalSent() == len(generatedJobs)
	}, 5*time.Second, 10*time.Millisecond, "jobs must be distributed while subscriptions change")
	cancel()

	assert.Zero(t, stream.sentTo("client-1"), "an unsubscribed client must not receive jobs from the in-flight batch")
	assert.Equal(t, len(generatedJobs), stream.sentTo("client-2"))
	assert.Equal(t, 1, serverClientCount(server, "test-job"), "only client-2 must remain subscribed")
}

func TestManagerHandlesLeaderChanges(t *testing.T) {
	// leader changes will be handled later
	t.SkipNow()
	mux, nodeLn, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeLn.Close()) }()
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)
	testStr.state.Partitions = make(map[uint32]state.Partition)

	jm1, completer := createServerNode(t, 1, ln, testStr)
	loader := completer.loader
	_ = loader
	assert.Nil(t, jm1.server, "server should not be initialized")

	*testStr = *getTestStore(ln)
	jm1.OnPartitionRoleChange(t.Context())
	assert.NotNil(t, jm1.server, "server should be initialized")

	testStr2 := &testStore{
		state: *testStr.state.DeepCopy(),
	}
	testStr2.nodeId = "node-2"

	jm2 := createClientNode(t, testStr2)

	client1 := make(chan Job)
	err = jm2.AddClient(t.Context(), "client-1", client1)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-1", "test-job")
	assert.NoError(t, err)

	generatedJobs := generateJobs(1)

	loader.addJobs(generatedJobs...)
	job := <-client1
	assert.NotEmpty(t, job.Key)
	err = jm2.CompleteJobReq(t.Context(), "client-1", job.Key, nil)
	assert.NoError(t, err)
	jm2.RemoveClient(t.Context(), "client-1")

	testStr.state.Partitions = map[uint32]state.Partition{}
	jm1.OnPartitionRoleChange(t.Context())
	assert.Nil(t, jm1.server, "server should not be initialized")

	testStr2.state.Partitions = map[uint32]state.Partition{}
	jm2.OnPartitionRoleChange(t.Context())
	assert.Empty(t, jm2.client.nodeStreams)
}

func TestManagerDistributesJob(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeLn.Close()) }()
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)

	jm1, completer := createServerNode(t, 1, ln, testStr)
	loader := completer.loader

	testStr2 := &testStore{
		state: *testStr.state.DeepCopy(),
	}
	testStr2.nodeId = "node-2"

	jm2 := createClientNode(t, testStr2)

	client1 := make(chan Job)
	// client connects to the node-2
	err = jm2.AddClient(t.Context(), "client-1", client1)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-1", "test-job")
	assert.NoError(t, err)

	generatedJobs := generateJobs(1)
	loader.addJobs(generatedJobs...)
	job := <-client1
	err = jm2.CompleteJobReq(t.Context(), "client-1", job.Key, nil)
	assert.NoError(t, err)

	assert.Contains(t, completer.completedJobs, generatedJobs[0].Key)
	assert.Positive(t, serverClientCount(jm1.server, job.Type))

	jm2.RemoveClient(t.Context(), "client-1")
	assert.Eventually(t, func() bool {
		return serverClientCount(jm1.server, job.Type) == 0
	}, 1*time.Second, 100*time.Millisecond)
}

func TestManagerHandlesMultipleClients(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeLn.Close()) }()
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)

	jm1, completer := createServerNode(t, 1, ln, testStr)
	loader := completer.loader

	testStr2 := &testStore{
		state: *testStr.state.DeepCopy(),
	}
	testStr2.nodeId = "node-2"

	jm2 := createClientNode(t, testStr2)

	client1 := make(chan Job)
	// client connects to the node-2
	err = jm2.AddClient(t.Context(), "client-1", client1)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-1", "test-job")
	assert.NoError(t, err)

	client2 := make(chan Job)
	err = jm2.AddClient(t.Context(), "client-2", client2)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-2", "test-job")
	assert.NoError(t, err)

	client1Jobs := consumeJobs(t, "client-1", client1, jm2)
	client2Jobs := consumeJobs(t, "client-2", client2, jm2)

	generatedJobs := generateJobs(6)
	loader.addJobs(generatedJobs...)

	waitForJobsToBeConsumed(t, generatedJobs, completer, 2*time.Second)

	assert.Equal(t, int64(len(generatedJobs)), client1Jobs.Load()+client2Jobs.Load(), "clients must consume all generated jobs")
	assert.NotZero(t, client1Jobs.Load())
	assert.NotZero(t, client2Jobs.Load())

	jm2.RemoveClient(t.Context(), "client-1")
	jm2.RemoveClient(t.Context(), "client-2")
	assert.Eventually(t, func() bool {
		return serverClientCount(jm1.server, "test-job") == 0
	}, 1*time.Second, 100*time.Millisecond)
}

func TestManagerHandlesClientConnections(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeLn.Close()) }()
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)

	jm1, completer := createServerNode(t, 1, ln, testStr)
	loader := completer.loader

	testStr2 := &testStore{
		state: *testStr.state.DeepCopy(),
	}
	testStr2.nodeId = "node-2"

	jm2 := createClientNode(t, testStr2)

	client1 := make(chan Job)
	// client connects to the node-2
	err = jm2.AddClient(t.Context(), "client-1", client1)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-1", "test-job")
	assert.NoError(t, err)

	client1Jobs := consumeJobs(t, "client-1", client1, jm2)

	generatedJobs := generateJobs(6)
	loader.addJobs(generatedJobs...)

	client2 := make(chan Job)
	err = jm2.AddClient(t.Context(), "client-2", client2)
	assert.NoError(t, err)
	err = jm2.AddClientJobSub(t.Context(), "client-2", "test-job")
	assert.NoError(t, err)

	client2Jobs := consumeJobs(t, "client-2", client2, jm2)

	generatedJobsBatch2 := generateJobs(6)
	fmt.Println("batch 2 start", generatedJobsBatch2)

	loader.addJobs(generatedJobsBatch2...)

	waitForJobsToBeConsumed(t, generatedJobsBatch2, completer, 2*time.Second)

	assert.Equal(t, int64(len(generatedJobs)+len(generatedJobsBatch2)), client1Jobs.Load()+client2Jobs.Load(), "clients must consume all generated jobs")
	assert.NotZero(t, client1Jobs.Load())
	assert.NotZero(t, client2Jobs.Load())

	client1JobsOnDisconnect := client1Jobs.Load()
	client2JobsOnDisconnect := client2Jobs.Load()
	jm2.RemoveClient(t.Context(), "client-1")

	assert.Eventually(t, func() bool {
		return serverClientCount(jm1.server, "test-job") == 1
	}, 1*time.Second, 100*time.Millisecond)

	generatedJobsBatch3 := generateJobs(6)
	fmt.Println("batch 3 start", generatedJobsBatch3)

	loader.addJobs(generatedJobsBatch3...)

	waitForJobsToBeConsumed(t, generatedJobsBatch3, completer, 2*time.Second)

	assert.Equal(t, client1JobsOnDisconnect, client1Jobs.Load(), "client 1 should not receive jobs after disconnect")
	assert.Equal(t, int64(len(generatedJobsBatch3)), client2Jobs.Load()-client2JobsOnDisconnect, "client 2 should consume all the jobs from batch 3")

	jm2.RemoveClient(t.Context(), "client-2")
	assert.Eventually(t, func() bool {
		return serverClientCount(jm1.server, "test-job") == 0
	}, 1*time.Second, 100*time.Millisecond)
}

// TestManagerTroughput is an on demand test to check throughput of the manager
func TestManagerTroughput(t *testing.T) {
	t.SkipNow()
	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	defer func() { require.NoError(t, f.Close()) }()
	mux, nodeLn, err := network.NewNodeMux("")
	require.NoError(t, err)
	defer func() { require.NoError(t, nodeLn.Close()) }()
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)
	testStr.state.Partitions = make(map[uint32]state.Partition)

	jm1, completer := createServerNode(t, 1, ln, testStr)
	loader := completer.loader
	_ = loader
	assert.Nil(t, jm1.server, "server should not be initialized")

	*testStr = *getTestStore(ln)
	jm1.OnPartitionRoleChange(t.Context())
	assert.NotNil(t, jm1.server, "server should be initialized")

	testStr2 := &testStore{
		state: *testStr.state.DeepCopy(),
	}
	testStr2.nodeId = "node-2"

	jm2 := createClientNode(t, testStr2)

	jobsToDistribute := 10000
	maxClients := 10
	channels := make([]chan Job, maxClients)
	jobType := JobType("test-job")
	// test iterative number of clients
	for i := range maxClients {
		client := make(chan Job)
		channels[i] = client
		clientID := ClientID(fmt.Sprintf("client-%d", i))
		err = jm2.AddClient(t.Context(), clientID, client)
		assert.NoError(t, err)
		err = jm2.AddClientJobSub(t.Context(), clientID, jobType)
		assert.NoError(t, err)

		go func() {
			for {
				job := <-client
				if job.Key == 0 {
					return
				}
				err := jm2.CompleteJobReq(t.Context(), clientID, job.Key, nil)
				assert.NoError(t, err)
			}
		}()
		assert.Eventually(t, func() bool {
			return serverClientCount(jm1.server, "test-job") == i+1
		}, 5*time.Second, 100*time.Millisecond, "wait for client to register")

		generatedJobs := generateJobs(jobsToDistribute)
		start := time.Now()
		loader.addJobs(generatedJobs...)
		waitForJobsToBeConsumed(t, generatedJobs, completer, 100*time.Second)
		end := time.Now()
		fmt.Printf("%d clients took %s\n", i+1, end.Sub(start))
	}
	for i := range maxClients {
		clientID := ClientID(fmt.Sprintf("client-%d", i))
		jm2.RemoveClient(t.Context(), clientID)
	}
}

func newTestJobServer(t *testing.T, loader JobLoader, completer JobCompleter) (*jobServer, *captureStream) {
	t.Helper()
	require.NoError(t, registerMetrics())
	server := newJobServer("node-1", loader, completer)
	stream := &captureStream{ctx: t.Context(), sent: map[ClientID]int{}}
	server.nodeSubs["node-2"] = &nodeSub{nodeID: "node-2", stream: stream}
	return server, stream
}

func consumeJobs(t *testing.T, clientID ClientID, client chan Job, jm *JobManager) *atomic.Int64 {
	counter := &atomic.Int64{}
	go func() {
		for {
			job := <-client
			if job.Key == 0 {
				return
			}
			fmt.Printf("%s completing %d\n", clientID, job.Key)
			err := jm.CompleteJobReq(t.Context(), clientID, job.Key, nil)
			assert.NoError(t, err)
			counter.Add(1)
		}
	}()
	return counter
}

func serverClientCount(server *jobServer, jobType JobType) int {
	server.clientMu.RLock()
	defer server.clientMu.RUnlock()
	return len(server.jobTypes[jobType].clients)
}

func waitForJobsToBeConsumed(t *testing.T, jobs []sql.Job, completer *testCompleter, timeout time.Duration) {
	loader := completer.loader
	assert.Eventually(t, func() bool {
		loader.mu.RLock()
		defer loader.mu.RUnlock()
		if len(loader.jobsToSend) != 0 {
			return false
		}
		for _, job := range jobs {
			match := false
		loadedJobs:
			for _, completedKey := range completer.completedJobs {
				if job.Key == completedKey {
					match = true
					break loadedJobs
				}
			}
			if !match {
				fmt.Printf("Job %d not completed\n", job.Key)
				fmt.Println(completer.completedJobs)
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond)
}

func generateJobs(count int) []sql.Job {
	return generateJobsOfType(count, "test-job")
}

func generateJobsOfType(count int, jobType string) []sql.Job {
	resp := make([]sql.Job, 0, count)
	for range count {
		resp = append(resp,
			sql.Job{
				Key:                gen.Generate().Int64(),
				ElementInstanceKey: rand.Int63(),
				ElementID:          "test-id",
				ProcessInstanceKey: rand.Int63(),
				Type:               jobType,
				State:              int64(runtime.ActivityStateActive),
				CreatedAt:          time.Now().UnixMilli(),
				InputVariables:     "{}",
			},
		)
	}
	return resp
}

func getTestStore(ln net.Listener) *testStore {
	return &testStore{
		state: state.Cluster{
			Partitions: map[uint32]state.Partition{
				1: {
					Id:       1,
					LeaderId: "node-1",
				},
			},
			Nodes: map[string]state.Node{
				"node-1": {
					Id:   "node-1",
					Addr: ln.Addr().String(),
					Role: state.RoleLeader,
					// the job client only subscribes to partitions whose leader already finished its local initialization
					Partitions: map[uint32]state.NodePartition{
						1: {
							Id:    1,
							State: state.NodePartitionStateInitialized,
							Role:  state.RoleLeader,
						},
					},
				},
				"node-2": {
					Id:         "node-2",
					Addr:       "",
					Role:       state.RoleFollower,
					Partitions: map[uint32]state.NodePartition{},
				},
			},
		},
		nodeId: "node-1",
	}
}

func createServerNode(t *testing.T, partition uint32, listener net.Listener, store *testStore) (*JobManager, *testCompleter) {
	cm := client.NewClientManager(store)
	completer := &testCompleter{
		completedJobs: []int64{},
		loader: &testLoader{
			jobsToSend: []sql.Job{},
			mu:         &sync.RWMutex{},
		},
	}
	jm := New(t.Context(), store, cm, completer.loader, completer)

	srv := grpc.NewServer()
	zenSrv := &grpcSrv{
		jobManager: jm,
	}
	proto.RegisterZenServiceServer(srv, zenSrv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	t.Cleanup(func() {
		srv.Stop()
		err := <-serveErr
		require.True(t, isExpectedGRPCServerStopError(err), "gRPC server failed: %v", err)
	})
	jm.Start()
	return jm, completer
}

func createClientNode(t *testing.T, store *testStore) *JobManager {
	cm := client.NewClientManager(store)
	completer := &testCompleter{
		completedJobs: []int64{},
		loader: &testLoader{
			jobsToSend: []sql.Job{},
			mu:         &sync.RWMutex{},
		},
	}
	jm := New(t.Context(), store, cm, completer.loader, completer)
	jm.Start()
	return jm
}

type grpcSrv struct {
	proto.UnimplementedZenServiceServer
	jobManager *JobManager
}

func (s *grpcSrv) SubscribeJob(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	return s.jobManager.AddNodeSubscription(stream)
}

func (s *grpcSrv) CompleteJob(ctx context.Context, req *proto.CompleteJobRequest) (*proto.CompleteJobResponse, error) {
	md, found := metadata.FromIncomingContext(ctx)
	clientID := ClientID("")
	if found {
		clientIDs := md.Get(MetadataClientID)
		if len(clientIDs) == 1 {
			clientID = ClientID(clientIDs[0])
		}
	}
	vars := make(map[string]any)
	if req.Variables != nil {
		err := json.Unmarshal(req.Variables, &vars)
		if err != nil {
			errMsg := fmt.Errorf("failed to unmarshal variables: %w", err)
			return &proto.CompleteJobResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: new(errMsg.Error()),
				},
			}, errMsg
		}
	}
	err := s.jobManager.CompleteJob(ctx, clientID, req.GetKey(), vars)
	if err != nil {
		errMsg := fmt.Errorf("failed to complete job %d: %w", req.Key, err)
		return &proto.CompleteJobResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: new(errMsg.Error()),
			},
		}, errMsg
	}
	return &proto.CompleteJobResponse{}, nil
}

type testCompleter struct {
	completedJobs []int64
	failedJobs    []int64
	loader        *testLoader
}

func (c *testCompleter) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error {
	c.loader.mu.Lock()
	defer c.loader.mu.Unlock()
	for i := len(c.loader.jobsToSend) - 1; i >= 0; i-- {
		if c.loader.jobsToSend[i].Key == jobKey {
			c.loader.jobsToSend = append(c.loader.jobsToSend[:i], c.loader.jobsToSend[i+1:]...)
		}
	}
	c.completedJobs = append(c.completedJobs, jobKey)
	return nil
}

func (c *testCompleter) JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	c.loader.mu.Lock()
	defer c.loader.mu.Unlock()
	for i := len(c.loader.jobsToSend) - 1; i >= 0; i-- {
		if c.loader.jobsToSend[i].Key == jobKey {
			c.loader.jobsToSend = append(c.loader.jobsToSend[:i], c.loader.jobsToSend[i+1:]...)
		}
	}
	c.failedJobs = append(c.failedJobs, jobKey)
	return nil
}

type testLoader struct {
	jobsToSend []sql.Job
	mu         *sync.RWMutex
	// onLoad is an optional hook invoked with the arguments of every LoadJobsToDistribute call
	onLoad func(jobTypes []string, idsToSkip []int64, count int64)
}

func (l *testLoader) addJobs(jobs ...sql.Job) {
	l.mu.Lock()
	l.jobsToSend = append(l.jobsToSend, jobs...)
	l.mu.Unlock()
}

func (l *testLoader) LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int64) ([]sql.Job, error) {
	if l.onLoad != nil {
		l.onLoad(jobTypes, idsToSkip, count)
	}
	distributedJobs := make([]sql.Job, 0)
	l.mu.Lock()
	currentCount := int64(0)
	jobTypesSet := make(map[string]struct{}, len(jobTypes))
	for _, jobType := range jobTypes {
		jobTypesSet[jobType] = struct{}{}
	}
	idsToSkipMap := make(map[int64]struct{}, len(idsToSkip))
	for _, id := range idsToSkip {
		idsToSkipMap[id] = struct{}{}
	}
	for _, job := range l.jobsToSend {
		_, requestedType := jobTypesSet[job.Type]
		if _, skipped := idsToSkipMap[job.Key]; requestedType && !skipped {
			distributedJobs = append(distributedJobs, job)
			currentCount++
			if currentCount >= count {
				break
			}
		}
	}
	l.mu.Unlock()
	return distributedJobs, nil
}

func isExpectedGRPCServerStopError(err error) bool {
	return err == nil || errors.Is(err, grpc.ErrServerStopped) || err.Error() == "network connection closed"
}

type testStore struct {
	state  state.Cluster
	nodeId string
}

func (s *testStore) ClusterState() state.Cluster {
	return s.state
}

func (s *testStore) NodeID() string {
	return s.nodeId
}

func (s *testStore) LeaderWithID() (string, string) {
	for _, node := range s.state.Nodes {
		if node.Role == state.RoleLeader {
			return node.Addr, node.Id
		}
	}
	return "", ""
}

func (s *testStore) PartitionLeaderWithID(partition uint32) (string, string) {
	partState := s.state.Partitions[partition]
	leaderId := partState.LeaderId
	leader := s.state.Nodes[leaderId]
	return leader.Addr, leader.Id
}

// captureStream is a fake job subscription stream that records how many jobs
// were sent to each client.
type captureStream struct {
	ctx       context.Context
	mu        sync.Mutex
	sent      map[ClientID]int
	attempts  int
	failSends int
}

func (s *captureStream) Send(resp *proto.SubscribeJobResponse) error {
	if resp.GetJob() == nil {
		// stream close message sent on shutdown
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failSends > 0 {
		s.failSends--
		return errors.New("transient send failure")
	}
	s.sent[ClientID(resp.GetClientId())]++
	return nil
}

func (s *captureStream) Recv() (*proto.SubscribeJobRequest, error) {
	<-s.ctx.Done()
	return nil, io.EOF
}

func (s *captureStream) totalSent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, count := range s.sent {
		total += count
	}
	return total
}

func (s *captureStream) sentTo(clientID ClientID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent[clientID]
}

func (s *captureStream) sendAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *captureStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureStream) SendHeader(metadata.MD) error { return nil }
func (s *captureStream) SetTrailer(metadata.MD)       {}
func (s *captureStream) Context() context.Context     { return s.ctx }
func (s *captureStream) SendMsg(_ any) error          { return nil }
func (s *captureStream) RecvMsg(_ any) error          { return nil }
