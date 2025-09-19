// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/server/servertest"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/go-hclog"
	comproto "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/stretchr/testify/assert"
)

func TestRqLiteStorage(t *testing.T) {
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
		ProcDefCacheTTL:  24 * time.Hour,
		ProcDefCacheSize: 200,
	}

	ts := servertest.NewTestServer()
	ts.FindActiveMessageHandler = func(famr *zenproto.FindActiveMessageRequest) (*zenproto.FindActiveMessageResponse, error) {
		err = fmt.Errorf("message subscription was not found %d", 1)
		return &zenproto.FindActiveMessageResponse{
			Error: &zenproto.ErrorResult{
				Code:    0,
				Message: err.Error(),
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
	defer partition.Stop()
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
				Key:                  pointer.MessageSubscriptionKey,
				ElementId:            "",
				ProcessDefinitionKey: 1,
				ProcessInstanceKey:   1,
				Name:                 pointer.Name,
				State:                pointer.State,
				CorrelationKey:       pointer.CorrelationKey,
				ExecutionToken:       pointer.ExecutionTokenKey,
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
