// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package cluster

import (
	"context"
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
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

	partitions := make(map[uint32]store.Partition)
	partitions[1] = store.Partition{
		Id:       1,
		LeaderId: "test-rq-lite",
	}
	tStore := &controllerTestStore{
		id:   "test-rq-lite",
		addr: fmt.Sprintf("127.0.0.1:%s", "1234"),
		clusterState: store.ClusterState{
			Config: store.ClusterConfig{
				DesiredPartitions: 1,
			},
			Partitions: partitions,
			Nodes:      map[string]store.Node{},
		},
		leader: true,
	}
	srvLn := network.NewZenBpmClusterListener(mux)
	srv := server.New(srvLn, tStore, nil, nil)
	err = srv.Open()
	assert.NoError(t, err)

	clientMgr := client.NewClientManager(tStore)

	partition, err := StartZenPartitionNode(ctx, mux, conf, clientMgr, tStore, 1, PartitionChangesCallbacks{})
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

	db, err := NewRqLiteDB(partition.rqliteDB.store, partition.partitionId, hclog.Default().Named("test-rq-lite-db"), conf, clientMgr, tStore)
	assert.NoError(t, err)

	tester := storagetest.StorageTester{}
	tester.PrepareTestData(db, t)

	tests := tester.GetTests()
	for name, testFunc := range tests {
		t.Run(name, testFunc(db, t))
	}
	testInstanceParent(t, db)
}

func testInstanceParent(t *testing.T, db *RqLiteDB) {
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
