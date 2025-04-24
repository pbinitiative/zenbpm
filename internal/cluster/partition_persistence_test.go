package cluster

import (
	"context"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
	"github.com/rqlite/rqlite/v8/command/proto"
)

func TestRqLiteStorage(t *testing.T) {
	ctx := context.Background()
	mux, err := network.NewNodeMux("")
	if err != nil {
		t.Fatalf("failed to create mux: %s", err)
	}
	ln := mux.Listen(0)
	c := GetRqLiteDefaultConfig(
		"test-rq-lite",
		ln.Addr().String(),
		t.TempDir(),
		[]string{},
	)
	partition, err := StartZenPartitionNode(ctx, mux, &c, 1)
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
			Sql: mig,
		}
	}
	// run migrations
	partition.Execute(ctx, &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: false,
			Statements:  stmts,
		},
	})

	db := NewRqLiteDB(partition.rqliteDB.store, hclog.Default().Named("test-rq-lite-db"))

	tester := storagetest.StorageTester{}
	tester.PrepareTestData(db, t)

	tests := tester.GetTests()
	for name, testFunc := range tests {
		t.Run(name, testFunc(db, t))
	}
}
