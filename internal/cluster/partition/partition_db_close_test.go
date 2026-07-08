package partition

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBCloseTerminatesDataCleanup(t *testing.T) {
	partition, conf, clientMgr, testStore, _ := prepareTestSetup(t, false)
	defer func() { assert.NoError(t, partition.Stop()) }()

	// The fixture partition disables cleanup. Build a DB with production
	// options so this test observes a running cleanup goroutine.
	db, err := newDB(
		partition.DB.Store,
		partition.PartitionId,
		hclog.Default().Named("test-db-close"),
		conf,
		clientMgr,
		testStore.ClusterState,
		defaultDBOptions(),
	)
	require.NoError(t, err)
	require.NotNil(t, db.cleanupDone)

	cleanupDone := db.cleanupDone
	db.Close()

	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("data cleanup goroutine still running 5s after DB.Close")
	}

	assert.NotPanics(t, func() { db.Close() })
}
