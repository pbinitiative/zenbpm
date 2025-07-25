package jobmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	partition = uint32(1)
	gen, _    = snowflake.NewNode(int64(partition))
)

func TestManagerHandlesLeaderChanges(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	defer nodeLn.Close()
	assert.NoError(t, err)
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)
	testStr.state.Partitions = make(map[uint32]store.Partition)

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
	jm2.AddClientJobSub(t.Context(), "client-1", "test-job")

	generatedJobs := generateJobs(1)

	loader.addJobs(generatedJobs...)
	job := <-client1
	assert.NotEmpty(t, job.Key)
	err = jm2.CompleteJobReq(t.Context(), "client-1", job.Key, nil)
	assert.NoError(t, err)
	jm2.RemoveClient(t.Context(), "client-1")

	testStr.state.Partitions = map[uint32]store.Partition{}
	jm1.OnPartitionRoleChange(t.Context())
	assert.Nil(t, jm1.server, "server should not be initialized")

	testStr2.state.Partitions = map[uint32]store.Partition{}
	jm2.OnPartitionRoleChange(t.Context())
	assert.Empty(t, jm2.client.nodeStreams)
}

func TestManagerDistributesJob(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	defer nodeLn.Close()
	assert.NoError(t, err)
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
	jm2.AddClientJobSub(t.Context(), "client-1", "test-job")

	generatedJobs := generateJobs(1)
	loader.addJobs(generatedJobs...)
	job := <-client1
	err = jm2.CompleteJobReq(t.Context(), "client-1", job.Key, nil)
	assert.NoError(t, err)

	assert.Contains(t, completer.completedJobs, generatedJobs[0].Key)
	assert.NotEmpty(t, jm1.server.jobClients[job.Type])

	jm2.RemoveClient(t.Context(), "client-1")
	assert.Eventually(t, func() bool {
		return len(jm1.server.jobClients[job.Type]) == 0
	}, 1*time.Second, 100*time.Millisecond)
}

func TestManagerHandlesMultipleClients(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	defer nodeLn.Close()
	assert.NoError(t, err)
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
	jm2.AddClientJobSub(t.Context(), "client-1", "test-job")

	client2 := make(chan Job)
	err = jm2.AddClient(t.Context(), "client-2", client2)
	assert.NoError(t, err)
	jm2.AddClientJobSub(t.Context(), "client-2", "test-job")

	client1Jobs := consumeJobs(t, "client-1", client1, jm2)
	client2Jobs := consumeJobs(t, "client-2", client2, jm2)

	generatedJobs := generateJobs(6)
	loader.addJobs(generatedJobs...)

	waitForJobsToBeConsumed(t, generatedJobs, completer)

	assert.Equal(t, len(generatedJobs), *client1Jobs+*client2Jobs, "clients must consume all generated jobs")
	assert.NotEqual(t, 0, client1Jobs)
	assert.NotEqual(t, 0, client2Jobs)

	jm2.RemoveClient(t.Context(), "client-1")
	jm2.RemoveClient(t.Context(), "client-2")
	assert.Eventually(t, func() bool {
		return len(jm1.server.jobClients["test-job"]) == 0
	}, 1*time.Second, 100*time.Millisecond)
}

func TestManagerHandlesClientConnections(t *testing.T) {
	mux, nodeLn, err := network.NewNodeMux("")
	defer nodeLn.Close()
	assert.NoError(t, err)
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
	jm2.AddClientJobSub(t.Context(), "client-1", "test-job")

	client1Jobs := consumeJobs(t, "client-1", client1, jm2)

	generatedJobs := generateJobs(6)
	loader.addJobs(generatedJobs...)

	client2 := make(chan Job)
	err = jm2.AddClient(t.Context(), "client-2", client2)
	assert.NoError(t, err)
	jm2.AddClientJobSub(t.Context(), "client-2", "test-job")

	client2Jobs := consumeJobs(t, "client-2", client2, jm2)

	generatedJobsBatch2 := generateJobs(6)
	fmt.Println("batch 2 start", generatedJobsBatch2)

	loader.addJobs(generatedJobsBatch2...)

	waitForJobsToBeConsumed(t, generatedJobsBatch2, completer)

	assert.Equal(t, len(generatedJobs)+len(generatedJobsBatch2), *client1Jobs+*client2Jobs, "clients must consume all generated jobs")
	assert.NotEqual(t, 0, client1Jobs)
	assert.NotEqual(t, 0, client2Jobs)

	client1JobsOnDisconnect := *client1Jobs
	client2JobsOnDisconnect := *client2Jobs
	jm2.RemoveClient(t.Context(), "client-1")

	assert.Eventually(t, func() bool {
		return len(jm1.server.jobClients["test-job"]) == 1
	}, 1*time.Second, 100*time.Millisecond)

	generatedJobsBatch3 := generateJobs(6)
	fmt.Println("batch 3 start", generatedJobsBatch3)

	loader.addJobs(generatedJobsBatch3...)

	waitForJobsToBeConsumed(t, generatedJobsBatch3, completer)

	assert.Equal(t, client1JobsOnDisconnect, *client1Jobs, "client 1 should not receive jobs after disconnect")
	assert.Equal(t, len(generatedJobsBatch3), *client2Jobs-client2JobsOnDisconnect, "client 2 should consume all the jobs from batch 3")

	jm2.RemoveClient(t.Context(), "client-2")
	assert.Eventually(t, func() bool {
		return len(jm1.server.jobClients["test-job"]) == 0
	}, 1*time.Second, 100*time.Millisecond)
}

func consumeJobs(t *testing.T, clientID ClientID, client1 chan Job, jm *JobManager) *int {
	counter := 0
	go func() {
		for {
			job := <-client1
			if job.Key == 0 {
				return
			}
			fmt.Printf("%s completing %d\n", clientID, job.Key)
			err := jm.CompleteJobReq(t.Context(), clientID, job.Key, nil)
			assert.NoError(t, err)
			counter++
		}
	}()
	return &counter
}

func waitForJobsToBeConsumed(t *testing.T, jobs []sql.Job, completer *testCompleter) {
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
	}, 1*time.Second, 100*time.Millisecond)
}

func generateJobs(count int) []sql.Job {
	resp := make([]sql.Job, 0, count)
	for range count {
		resp = append(resp,
			sql.Job{
				Key:                gen.Generate().Int64(),
				ElementInstanceKey: rand.Int63(),
				ElementID:          "test-id",
				ProcessInstanceKey: rand.Int63(),
				Type:               "test-job",
				State:              int64(runtime.ActivityStateActive),
				CreatedAt:          time.Now().UnixMilli(),
				Variables:          "{}",
			},
		)
	}
	return resp
}

func getTestStore(ln net.Listener) *testStore {
	return &testStore{
		state: store.ClusterState{
			Partitions: map[uint32]store.Partition{
				1: {
					Id:       1,
					LeaderId: "node-1",
				},
			},
			Nodes: map[string]store.Node{
				"node-1": {
					Id:   "node-1",
					Addr: ln.Addr().String(),
					Role: store.RoleLeader,
				},
				"node-2": {
					Id:   "node-2",
					Addr: "",
					Role: store.RoleFollower,
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
	go srv.Serve(listener)
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
		clientIDs := md.Get(metadataClientID)
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
					Code:    0,
					Message: errMsg.Error(),
				},
			}, errMsg
		}
	}
	err := s.jobManager.CompleteJob(ctx, clientID, req.Key, vars)
	if err != nil {
		errMsg := fmt.Errorf("failed to complete job %d: %w", req.Key, err)
		return &proto.CompleteJobResponse{
			Error: &proto.ErrorResult{
				Code:    0,
				Message: errMsg.Error(),
			},
		}, errMsg
	}
	return &proto.CompleteJobResponse{}, nil
}

type testCompleter struct {
	completedJobs []int64
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

type testLoader struct {
	jobsToSend []sql.Job
	mu         *sync.RWMutex
}

func (l *testLoader) addJobs(jobs ...sql.Job) {
	l.mu.Lock()
	l.jobsToSend = append(l.jobsToSend, jobs...)
	l.mu.Unlock()
}

func (l *testLoader) LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int) ([]sql.Job, error) {
	distributedJobs := make([]sql.Job, 0)
	l.mu.Lock()
	currentCount := 0
	idsToSkipMap := make(map[int64]struct{}, len(idsToSkip))
	for _, id := range idsToSkip {
		idsToSkipMap[id] = struct{}{}
	}
	for _, job := range l.jobsToSend {
		if _, ok := idsToSkipMap[job.Key]; !ok {
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

type testStore struct {
	state  store.ClusterState
	nodeId string
}

func (s *testStore) ClusterState() store.ClusterState {
	return s.state
}

func (s *testStore) NodeID() string {
	return s.nodeId
}

func (s *testStore) LeaderWithID() (string, string) {
	for _, node := range s.state.Nodes {
		if node.Role == store.RoleLeader {
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

// TestManagerTroughput is an on demand test to check throughput of the manager
func TestManagerTroughput(t *testing.T) {
	t.SkipNow()
	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	pprof.StartCPUProfile(f)
	mux, nodeLn, err := network.NewNodeMux("")
	defer nodeLn.Close()
	assert.NoError(t, err)
	ln := network.NewZenBpmClusterListener(mux)
	assert.NoError(t, err)

	testStr := getTestStore(ln)
	testStr.state.Partitions = make(map[uint32]store.Partition)

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
		jm2.AddClientJobSub(t.Context(), clientID, jobType)

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
			return len(jm1.server.jobClients["test-job"]) == i+1
		}, 5*time.Second, 100*time.Millisecond, "wait for client to register")

		generatedJobs := generateJobs(jobsToDistribute)
		start := time.Now()
		loader.addJobs(generatedJobs...)
		assert.Eventually(t, func() bool {
			loader.mu.RLock()
			defer loader.mu.RUnlock()
			if len(loader.jobsToSend) != 0 {
				return false
			}
			for _, job := range generatedJobs {
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
		}, 100*time.Second, 100*time.Millisecond)
		end := time.Now()
		fmt.Printf("%d clients took %s\n", i+1, end.Sub(start))
	}
	for i := range maxClients {
		clientID := ClientID(fmt.Sprintf("client-%d", i))
		jm2.RemoveClient(t.Context(), clientID)
	}
	pprof.StopCPUProfile()
}
