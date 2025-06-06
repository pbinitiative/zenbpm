package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/sql"
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
	partition, err := StartZenPartitionNode(ctx, mux, &c, 1, PartitionChangesCallbacks{})
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

	db, err := NewRqLiteDB(partition.rqliteDB.store, partition.partitionId, hclog.Default().Named("test-rq-lite-db"))
	assert.NoError(t, err)

	tester := storagetest.StorageTester{}
	tester.PrepareTestData(db, t)

	tests := tester.GetTests()
	for name, testFunc := range tests {
		t.Run(name, testFunc(db, t))
	}
}
