package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	ssql "database/sql"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/server/servertest"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/go-hclog"
	comproto "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/stretchr/testify/assert"
)

func prepareTestSetup(t *testing.T) (*ZenPartitionNode, config.Persistence, *client.ClientManager, *testStore, *servertest.TestServer) {
	ctx := context.Background()
	mux, muxLn, err := network.NewNodeMux("")
	if err != nil {
		t.Fatalf("failed to create mux: %s", err)
	}
	c := GetRqLiteDefaultConfig(
		"test-rq-lite",
		muxLn.Addr().String(),
		t.TempDir(),
		[]string{muxLn.Addr().String()},
	)
	conf := config.Persistence{
		RqLite:           &c,
		ProcDefCacheTTL:  types.TTL(24 * time.Hour),
		ProcDefCacheSize: 200,
	}

	ts := servertest.NewTestServer()
	ts.FindActiveMessageHandler = func(famr *zenproto.FindActiveMessageRequest) (*zenproto.FindActiveMessageResponse, error) {
		err = fmt.Errorf("message subscription was not found %d", 1)
		return &zenproto.FindActiveMessageResponse{
			Error: &zenproto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, status.Error(codes.NotFound, err.Error())
	}

	partitions := make(map[uint32]state.Partition)
	partitions[1] = state.Partition{
		Id:       1,
		LeaderId: "test-rq-lite",
	}
	tStore := &testStore{
		id:   "test-rq-lite",
		addr: ts.Addr(),
		clusterState: state.Cluster{
			Config: state.ClusterConfig{
				DesiredPartitions: 1,
			},
			Partitions: partitions,
			Nodes:      map[string]state.Node{},
		},
		leader: true,
	}
	clientMgr := client.NewClientManager(tStore)

	partition, err := StartZenPartitionNode(ctx, mux, conf, clientMgr, 1, PartitionChangesCallbacks{}, func() state.Cluster {
		return state.Cluster{
			Config: state.ClusterConfig{},
			Partitions: map[uint32]state.Partition{
				1: state.Partition{
					Id:       1,
					LeaderId: "node-1",
				}},
			Nodes: map[string]state.Node{
				"node-1": state.Node{
					Id:         "node-1",
					Addr:       "localhost:",
					Suffrage:   0,
					State:      0,
					Role:       0,
					Partitions: map[uint32]state.NodePartition{},
				}},
		}
	})
	if err != nil {
		t.Fatalf("failed to create partition node: %s", err)
	}
	migrations, err := sql.GetMigrations()
	if err != nil {
		t.Fatalf("failed to get migrations: %s", err)
	}
	stmts := make([]*proto.Statement, len(migrations))
	for i, mig := range migrations {
		stmts[i] = &proto.Statement{
			Sql: mig.SQL,
		}
	}
	_, err = partition.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader for partition: %s", err)
	}

	// run migrations
	_, err = partition.Execute(ctx, &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: false,
			Statements:  stmts,
		},
	})
	if err != nil {
		t.Fatalf("failed to run migrations: %s", err)
	}
	return partition, conf, clientMgr, tStore, ts
}

func TestRqLiteStorage(t *testing.T) {
	partition, conf, clientMgr, tStore, ts := prepareTestSetup(t)
	defer partition.Stop()

	db, err := NewDB(partition.DB.Store, partition.PartitionId, hclog.Default().Named("test-rq-lite-db"), conf, clientMgr, tStore.ClusterState)
	assert.NoError(t, err)

	tester := storagetest.StorageTester{}
	tester.PrepareTestData(db, t)

	tests := tester.GetTests()
	for name, testFunc := range tests {
		t.Run(name, testFunc(db, t))
	}
	testInstanceParent(t, db)
	testMessageCorrelation(t, db, ts)
}

func TestDataCleanup(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t)
	defer partition.Stop()

	db, err := NewDB(partition.DB.Store, partition.PartitionId, hclog.Default().Named("test-data-cleanup"), conf, clientMgr, tStore.ClusterState)
	assert.NoError(t, err)

	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`

	r := db.GenerateId()
	pd := runtime.ProcessDefinition{
		BpmnProcessId:    fmt.Sprintf("id-%d", r),
		Version:          1,
		Key:              r,
		BpmnData:         fmt.Sprintf(data, r),
		BpmnChecksum:     [16]byte{1},
		BpmnResourceName: fmt.Sprintf("resource-%d", r),
	}
	err = db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	createTestData := func(ctx context.Context, idArr *[]int64) {
		r := db.GenerateId()
		*idArr = append(*idArr, r)
		inst1 := runtime.ProcessInstance{
			Definition:                  &pd,
			Key:                         r,
			VariableHolder:              runtime.VariableHolder{},
			CreatedAt:                   time.Now(),
			State:                       runtime.ActivityStateReady,
			ParentProcessExecutionToken: nil,
		}
		err = db.SaveProcessInstance(ctx, inst1)
		assert.NoError(t, err)
		inst1.State = runtime.ActivityStateActive
		err = db.SaveProcessInstance(ctx, inst1)
		assert.NoError(t, err)

		token := runtime.ExecutionToken{
			Key:                r + 50,
			ElementInstanceKey: r + 60,
			ElementId:          "test-64654",
			ProcessInstanceKey: inst1.Key,
			State:              runtime.TokenStateRunning,
		}
		err = db.SaveToken(t.Context(), token)
		assert.NoError(t, err)

		messageSub := runtime.MessageSubscription{
			Key:                  r + 70,
			ElementId:            "message-sub",
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   inst1.Key,
			Name:                 "name",
			CorrelationKey:       "cor-1",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
			Token:                token,
		}
		err = db.SaveMessageSubscription(ctx, messageSub)
		assert.NoError(t, err)

		incident := runtime.Incident{
			Key:                r + 100,
			ElementInstanceKey: r + 101,
			ElementId:          "something-465",
			ProcessInstanceKey: inst1.Key,
			Message:            "something went wrong",
			CreatedAt:          time.Now(),
			Token:              token,
		}
		err = db.SaveIncident(ctx, incident)
		assert.NoError(t, err)

		r2 := db.GenerateId()
		*idArr = append(*idArr, r2)
		inst2 := runtime.ProcessInstance{
			Definition:                  &pd,
			Key:                         r2,
			VariableHolder:              runtime.VariableHolder{},
			CreatedAt:                   time.Now(),
			State:                       runtime.ActivityStateReady,
			ParentProcessExecutionToken: &token,
		}
		err = db.SaveProcessInstance(ctx, inst2)
		assert.NoError(t, err)
		inst2.State = runtime.ActivityStateActive
		err = db.SaveProcessInstance(ctx, inst2)
		assert.NoError(t, err)
		inst2.State = runtime.ActivityStateCompleted
		err = db.SaveProcessInstance(ctx, inst2)
		assert.NoError(t, err)

		timer := runtime.Timer{
			ElementId:            "timer-elem",
			Key:                  r2 + 80,
			ElementInstanceKey:   r2 + 81,
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   inst2.Key,
			TimerState:           runtime.TimerStateCreated,
			CreatedAt:            time.Now(),
			DueAt:                time.Now().Add(1 * time.Hour),
			Duration:             1 * time.Hour,
			Token:                token,
		}
		err = db.SaveTimer(ctx, timer)
		assert.NoError(t, err)

		job := runtime.Job{
			ElementId:          "job-123",
			ElementInstanceKey: r2 + 91,
			ProcessInstanceKey: inst2.Key,
			Key:                r2 + 90,
			State:              runtime.ActivityStateActive,
			Type:               "test-job",
			Variables:          map[string]any{"foo": "bar"},
			CreatedAt:          time.Now(),
			Token:              token,
		}
		err = db.SaveJob(ctx, job)
		assert.NoError(t, err)

		flowHist := runtime.FlowElementHistoryItem{
			Key:                job.Key,
			ProcessInstanceKey: inst2.Key,
			ElementId:          "job-123",
			CreatedAt:          job.CreatedAt,
		}
		err = db.SaveFlowElementHistory(ctx, flowHist)
		assert.NoError(t, err)
	}

	idsToBeDeleted := []int64{}
	idsToKeep := []int64{}
	for i := range db.historyDeleteThreshold {
		ctx := t.Context()
		if i < db.historyDeleteThreshold/2 {
			ctx = appcontext.WithHistoryTTL(ctx, types.TTL(1*time.Hour))
			createTestData(ctx, &idsToKeep)
		} else {
			createTestData(ctx, &idsToBeDeleted)
		}
	}

	nowSeconds := ssql.NullInt64{
		Int64: time.Now().Unix(),
		Valid: true,
	}
	inactiveIds, err := db.Queries.FindInactiveInstancesToDelete(t.Context(), nowSeconds)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(inactiveIds))
	activeBeforeCleanup, err := db.Queries.FindActiveInstances(t.Context())
	assert.NoError(t, err)

	err = db.dataCleanup(time.Now())
	assert.NoError(t, err)

	activeAfterCleanup, err := db.Queries.FindActiveInstances(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, activeBeforeCleanup, activeAfterCleanup)

	createTestData(t.Context(), &idsToBeDeleted)

	idsToComplete := append(idsToBeDeleted, idsToKeep...)
	for _, id := range idsToComplete {
		pi, err := db.FindProcessInstanceByKey(t.Context(), id)
		assert.NoError(t, err)
		pi.State = runtime.ActivityStateCompleted
		err = db.SaveProcessInstance(t.Context(), pi)
		assert.NoError(t, err)
	}
	inactiveIds, err = db.Queries.FindInactiveInstancesToDelete(t.Context(), nowSeconds)
	assert.NoError(t, err)
	assert.ElementsMatch(t, inactiveIds, idsToBeDeleted)

	err = db.dataCleanup(time.Now())
	assert.NoError(t, err)

	inactiveIds, err = db.Queries.FindInactiveInstancesToDelete(t.Context(), nowSeconds)
	assert.NoError(t, err)
	assert.Empty(t, inactiveIds)
	count := queryCount(t, db, "select count(*) from process_instance")
	assert.Equal(t, int64(db.historyDeleteThreshold), count)
	count = queryCount(t, db, "select count(*) from message_subscription")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)
	count = queryCount(t, db, "select count(*) from timer")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)
	count = queryCount(t, db, "select count(*) from job")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)
	count = queryCount(t, db, "select count(*) from execution_token")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)
	count = queryCount(t, db, "select count(*) from flow_element_history")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)
	count = queryCount(t, db, "select count(*) from incident")
	assert.Equal(t, int64(db.historyDeleteThreshold/2), count)

	db.historyDeleteThreshold = 1
	err = db.dataCleanup(time.Now().Add(2 * time.Hour))
	assert.NoError(t, err)

	inactiveIds, err = db.Queries.FindInactiveInstancesToDelete(t.Context(), nowSeconds)
	assert.NoError(t, err)
	assert.Empty(t, inactiveIds)
	count = queryCount(t, db, "select count(*) from process_instance")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from message_subscription")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from timer")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from job")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from execution_token")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from flow_element_history")
	assert.Empty(t, count)
	count = queryCount(t, db, "select count(*) from incident")
	assert.Empty(t, count)
}

func queryCount(t *testing.T, db *DB, query string) int64 {
	row := db.QueryRowContext(t.Context(), query)
	var count int64
	err := row.Scan(&count)
	assert.NoError(t, err)
	return count
}

func testMessageCorrelation(t *testing.T, db *DB, ts *servertest.TestServer) {
	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`
	r := db.GenerateId()
	pd := runtime.ProcessDefinition{
		BpmnProcessId:    fmt.Sprintf("id-%d", r),
		Version:          1,
		Key:              r,
		BpmnData:         fmt.Sprintf(data, r),
		BpmnChecksum:     [16]byte{1},
		BpmnResourceName: fmt.Sprintf("resource-%d", r),
	}
	err := db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	inst1 := runtime.ProcessInstance{
		Definition:                  &pd,
		Key:                         r,
		VariableHolder:              runtime.VariableHolder{},
		CreatedAt:                   time.Now(),
		State:                       runtime.ActivityStateActive,
		ParentProcessExecutionToken: nil,
	}
	err = db.SaveProcessInstance(t.Context(), inst1)
	assert.NoError(t, err)
	token := runtime.ExecutionToken{
		Key:                r + 50,
		ElementInstanceKey: r + 60,
		ElementId:          "test-64654",
		ProcessInstanceKey: inst1.Key,
		State:              runtime.TokenStateRunning,
	}
	err = db.SaveToken(t.Context(), token)
	assert.NoError(t, err)

	t.Run("test message subscription db save", func(t *testing.T) {
		err = db.SaveMessageSubscription(t.Context(), runtime.MessageSubscription{
			Key:                  db.GenerateId(),
			ElementId:            "123",
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   inst1.Key,
			Name:                 "test-message",
			CorrelationKey:       "duplicate_correlation_key",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
			Token: runtime.ExecutionToken{
				Key: 46465132,
			},
		})
		assert.NoError(t, err)

		err = db.SaveMessageSubscription(t.Context(), runtime.MessageSubscription{
			Key:                  db.GenerateId(),
			ElementId:            "123",
			ProcessDefinitionKey: 1,
			ProcessInstanceKey:   1,
			Name:                 "test-message",
			CorrelationKey:       "duplicate_correlation_key",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
			Token: runtime.ExecutionToken{
				Key: 16465133,
			},
		})
		assert.Error(t, err)
	})
	t.Run("test message subscription batch save", func(t *testing.T) {
		err = db.SaveMessageSubscription(t.Context(), runtime.MessageSubscription{
			Key:                  db.GenerateId(),
			ElementId:            "124",
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   inst1.Key,
			Name:                 "test-message",
			CorrelationKey:       "duplicate_correlation_key_batch",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
			Token: runtime.ExecutionToken{
				Key: 46465132,
			},
		})
		assert.NoError(t, err)

		pointer, err := db.FindActiveMessageSubscriptionPointer(t.Context(), "test-message", "duplicate_correlation_key_batch")
		batch := db.NewBatch()
		err = batch.SaveMessageSubscription(t.Context(), runtime.MessageSubscription{
			Key:                  db.GenerateId(),
			ElementId:            "124",
			ProcessDefinitionKey: 3,
			ProcessInstanceKey:   3,
			Name:                 "test-message",
			CorrelationKey:       "duplicate_correlation_key_batch",
			State:                runtime.ActivityStateActive,
			CreatedAt:            time.Now(),
			Token: runtime.ExecutionToken{
				Key: 16465133,
			},
		})
		assert.NoError(t, err)
		ts.FindActiveMessageHandler = func(famr *zenproto.FindActiveMessageRequest) (*zenproto.FindActiveMessageResponse, error) {
			return &zenproto.FindActiveMessageResponse{
				Key:                  &pointer.MessageSubscriptionKey,
				ElementId:            nil,
				ProcessDefinitionKey: ptr.To(int64(1)),
				ProcessInstanceKey:   ptr.To(int64(1)),
				Name:                 &pointer.Name,
				State:                &pointer.State,
				CorrelationKey:       &pointer.CorrelationKey,
				ExecutionToken:       &pointer.ExecutionTokenKey,
			}, nil
		}
		err = batch.Flush(t.Context())
		assert.Error(t, err)
	})
}

func testInstanceParent(t *testing.T, db *DB) {
	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`
	r := db.GenerateId()
	pd := runtime.ProcessDefinition{
		BpmnProcessId:    fmt.Sprintf("id-%d", r),
		Version:          1,
		Key:              r,
		BpmnData:         fmt.Sprintf(data, r),
		BpmnChecksum:     [16]byte{1},
		BpmnResourceName: fmt.Sprintf("resource-%d", r),
	}
	err := db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	inst1 := runtime.ProcessInstance{
		Definition:                  &pd,
		Key:                         r,
		VariableHolder:              runtime.VariableHolder{},
		CreatedAt:                   time.Now(),
		State:                       runtime.ActivityStateActive,
		ParentProcessExecutionToken: nil,
	}
	err = db.SaveProcessInstance(t.Context(), inst1)
	assert.NoError(t, err)

	tok1 := runtime.ExecutionToken{
		Key:                r,
		ElementInstanceKey: 12345,
		ElementId:          "some-id",
		ProcessInstanceKey: inst1.Key,
		State:              runtime.TokenStateWaiting,
	}
	err = db.SaveToken(t.Context(), tok1)
	assert.NoError(t, err)

	inst2 := runtime.ProcessInstance{
		Definition:                  &pd,
		Key:                         r + 1,
		VariableHolder:              runtime.VariableHolder{},
		CreatedAt:                   time.Now(),
		State:                       runtime.ActivityStateActive,
		ParentProcessExecutionToken: &tok1,
	}
	err = db.SaveProcessInstance(t.Context(), inst2)
	assert.NoError(t, err)
	dbInst1, err := db.FindProcessInstanceByKey(t.Context(), inst1.Key)
	assert.NoError(t, err)
	assert.Nil(t, dbInst1.ParentProcessExecutionToken)

	dbInst2, err := db.FindProcessInstanceByKey(t.Context(), inst2.Key)
	assert.NoError(t, err)
	assert.NotNil(t, dbInst2.ParentProcessExecutionToken)

	instncs, err := db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		ProcessDefinitionKey: 0,
		ParentInstanceKey:    0,
		Offst:                0,
		Size:                 20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 5)

	instncs, err = db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		ProcessDefinitionKey: dbInst2.Definition.Key,
		ParentInstanceKey:    0,
		Offst:                0,
		Size:                 20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 2)

	instncs, err = db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		ProcessDefinitionKey: dbInst2.Definition.Key,
		ParentInstanceKey:    tok1.Key,
		Offst:                0,
		Size:                 20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 1)

	subProcesses, err := db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		ProcessDefinitionKey: inst2.Definition.Key,
		ParentInstanceKey:    inst1.GetInstanceKey(),
		Offst:                0,
		Size:                 20,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, subProcesses)
	assert.Equal(t, tok1.Key, subProcesses[0].ParentProcessExecutionToken.Int64)
}

type testStore struct {
	id           string
	addr         string
	clusterState state.Cluster
	leader       bool
}

func (c *testStore) Addr() string {
	return c.addr
}

func (c *testStore) ClusterState() state.Cluster {
	return c.clusterState
}

func (c *testStore) ID() string {
	return c.id
}

func (c *testStore) IsLeader() bool {
	return c.leader
}

func (c *testStore) LeaderWithID() (string, string) {
	return c.addr, c.id
}

func (c *testStore) PartitionLeaderWithID(partition uint32) (string, string) {
	return c.addr, c.id
}

func (c *testStore) Role() comproto.Role {
	if c.leader == true {
		return comproto.Role_ROLE_TYPE_LEADER
	} else {
		return comproto.Role_ROLE_TYPE_FOLLOWER
	}
}

func (c *testStore) LeaderID() (string, error) {
	return c.id, nil
}

func (c *testStore) Join(jr *zenproto.JoinRequest) error {
	panic("unexpected call to Join")
}

func (c *testStore) Notify(nr *zenproto.NotifyRequest) error {
	panic("unexpected call to Notify")
}

func (c *testStore) WriteNodeChange(change *comproto.NodeChange) error {
	c.clusterState = store.FsmApplyNodeChange(c, change)
	return nil
}

func (c *testStore) WritePartitionChange(change *comproto.NodePartitionChange) error {
	c.clusterState = store.FsmApplyPartitionChange(c, change)
	return nil
}
