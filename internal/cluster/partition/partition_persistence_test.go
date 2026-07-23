package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"slices"

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
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareTestSetupWithTestMigration(t *testing.T) (*ZenPartitionNode, config.Persistence, *client.ClientManager, *testStore, *servertest.TestServer) {
	return prepareTestSetup(t, true)
}

func testDBOptions() dbOptions {
	opts := defaultDBOptions()
	opts.startDataCleanup = false
	return opts
}

func newTestDB(t *testing.T, partition *ZenPartitionNode, conf config.Persistence, clientMgr *client.ClientManager, tStore *testStore, loggerName string) *DB {
	t.Helper()
	db, err := newDB(
		partition.DB.Store,
		partition.PartitionId,
		hclog.Default().Named(loggerName),
		conf,
		clientMgr,
		tStore.ClusterState,
		testDBOptions(),
	)
	require.NoError(t, err)
	return db
}

func prepareTestSetup(t *testing.T, runMigrationWithRollback bool) (*ZenPartitionNode, config.Persistence, *client.ClientManager, *testStore, *servertest.TestServer) {
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

	migrationDir := ""
	if runMigrationWithRollback {
		migrationDir = "internal/cluster/partition/testdata/migrations_test"
	}

	conf := config.Persistence{
		RqLite:           &c,
		ProcDefCacheTTL:  types.TTL(24 * time.Hour),
		ProcDefCacheSize: 200,
		Migration:        config.Migration{Dir: migrationDir},
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

	partition, err := startZenPartitionNode(ctx, mux, conf, clientMgr, 1, PartitionChangesCallbacks{}, func() state.Cluster {
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
	}, testDBOptions())
	if err != nil {
		t.Fatalf("failed to create partition node: %s", err)
	}
	_, err = partition.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader for partition: %s", err)
	}

	err = partition.DB.RunMigrations(ctx)
	if err != nil {
		if !runMigrationWithRollback {
			t.Fatalf("failed to run migrations: %s", err)
		}
	}
	return partition, conf, clientMgr, tStore, ts
}

func TestRqLiteStorage(t *testing.T) {
	partition, conf, clientMgr, tStore, ts := prepareTestSetup(t, false)
	defer partition.Stop()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-rq-lite-db")

	tester := storagetest.StorageTester{}
	tester.PrepareTestData(db, t)

	tests := tester.GetTests()
	for name, testFunc := range tests {
		t.Run(name, testFunc(db, t))
	}
	testInstanceParent(t, db)
	t.Run("FindProcessInstancesPage filters and sort", func(t *testing.T) {
		testFindProcessInstancesPageFiltersAndSort(t, db)
	})
	testMessageCorrelation(t, db, ts)
	t.Run("TestHasActiveSubProcessInstance", tester.TestHasActiveSubProcessInstance(db, t))
}

func TestRunUpMigrations(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-migration-lite-db")

	appliedMigrations, err := db.Queries.GetMigrations(t.Context())
	assert.NoError(t, err)

	migrations, err := sql.GetUpMigrations(db.migrationDir)
	assert.NoError(t, err)

	assert.Equal(t, len(migrations), len(appliedMigrations),
		"Expected number of applied migrations to match number of available migrations")

	appliedMigrationNames := make(map[string]struct{}, len(appliedMigrations))
	for _, appliedMigration := range appliedMigrations {
		appliedMigrationNames[appliedMigration.Name] = struct{}{}
	}

	for _, migration := range migrations {
		_, found := appliedMigrationNames[migration.Filename]
		assert.True(t, found, "migration %s was not applied", migration.Filename)
	}
}

func TestRunRollbackMigration(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetupWithTestMigration(t)
	defer partition.Stop()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-migration-lite-db")

	appliedMigrations, err := db.Queries.GetMigrations(t.Context())
	assert.NoError(t, err)

	migrations, err := sql.GetUpMigrations(db.migrationDir)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(migrations), "Expected number of migrations to be 3, but got %d migrations", len(migrations))

	assert.Equal(t, 1, len(appliedMigrations),
		"Expected number of applied migrations to be 1, but got %d migrations", len(appliedMigrations))

	appliedMigrationNames := make(map[string]struct{}, len(appliedMigrations))
	for _, appliedMigration := range appliedMigrations {
		appliedMigrationNames[appliedMigration.Name] = struct{}{}
	}

	assert.Contains(t, appliedMigrationNames, "0000_init.up.sql", "init migration should be applied")

	var tableCount int
	err = db.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ? or name = ?",
		"rollback_probe", "should_not_exists",
	).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Zero(t, tableCount, "Only the migration table should exist after a failed migration rollback.")
}

func TestDataCleanup(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-data-cleanup")
	const cleanupBatchSize = 10

	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`

	r := db.GenerateId()
	pd := runtime.ProcessDefinition{
		BpmnProcessId: fmt.Sprintf("id-%d", r),
		Version:       1,
		Key:           r,
		BpmnData:      fmt.Sprintf(data, r),
		BpmnChecksum:  [16]byte{1},
	}
	err := db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	createTestData := func(ctx context.Context, idArr *[]int64) {
		var historyTTLSec *int64
		if historyTTL, found := appcontext.HistoryTTLFromContext(ctx); found {
			ttl := int64(historyTTL.Seconds())
			historyTTLSec = &ttl
		}

		r := db.GenerateId()
		*idArr = append(*idArr, r)
		inst1 := runtime.DefaultProcessInstance{
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &pd,
				Key:            r,
				VariableHolder: runtime.VariableHolder{},
				CreatedAt:      time.Now(),
				State:          runtime.ActivityStateReady,
				HistoryTTLSec:  historyTTLSec,
			},
		}
		err = db.SaveProcessInstance(ctx, &inst1)
		assert.NoError(t, err)
		inst1.ProcessInstance().State = runtime.ActivityStateActive
		err = db.SaveProcessInstance(ctx, &inst1)
		assert.NoError(t, err)

		token := runtime.ExecutionToken{
			Key:                r + 50,
			ElementInstanceKey: r + 60,
			ElementId:          "test-64654",
			ProcessInstanceKey: inst1.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
		err = db.SaveToken(t.Context(), token)
		assert.NoError(t, err)

		messageSub := &runtime.TokenMessageSubscription{
			Token:              token,
			ProcessInstanceKey: inst1.ProcessInstance().Key,
			CorrelationKey:     "cor-1",
			MessageSubscriptionData: runtime.MessageSubscriptionData{
				Key:                  r + 70,
				ElementId:            "message-sub",
				ProcessDefinitionKey: pd.Key,
				Name:                 "name",
				State:                runtime.ActivityStateActive,
				CreatedAt:            time.Now(),
			},
		}
		err = db.SaveMessageSubscription(ctx, messageSub)
		assert.NoError(t, err)

		incident := runtime.Incident{
			Key:                r + 100,
			ElementInstanceKey: r + 101,
			ElementId:          "something-465",
			ProcessInstanceKey: inst1.ProcessInstance().Key,
			Message:            "something went wrong",
			CreatedAt:          time.Now(),
			Token:              token,
		}
		err = db.SaveIncident(ctx, incident)
		assert.NoError(t, err)

		errorSubscription := runtime.ErrorSubscription{
			Key:                  r + 110,
			ElementInstanceKey:   r + 111,
			ElementId:            "error-catch-465",
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   inst1.ProcessInstance().Key,
			State:                runtime.ErrorStateCreated,
			CreatedAt:            time.Now(),
			Token:                token,
		}
		err = db.SaveErrorSubscription(ctx, errorSubscription)
		assert.NoError(t, err)

		r2 := db.GenerateId()
		*idArr = append(*idArr, r2)
		inst2 := runtime.SubProcessInstance{
			ParentProcessExecutionToken:           token,
			ParentProcessTargetElementInstanceKey: token.ElementInstanceKey,
			ParentProcessTargetElementId:          token.ElementId,
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &pd,
				Key:            r2,
				VariableHolder: runtime.VariableHolder{},
				CreatedAt:      time.Now(),
				State:          runtime.ActivityStateReady,
				HistoryTTLSec:  historyTTLSec,
			},
		}
		err = db.SaveProcessInstance(ctx, &inst2)
		assert.NoError(t, err)
		inst2.ProcessInstance().State = runtime.ActivityStateActive
		err = db.SaveProcessInstance(ctx, &inst2)
		assert.NoError(t, err)
		inst2.ProcessInstance().State = runtime.ActivityStateCompleted
		err = db.SaveProcessInstance(ctx, &inst2)
		assert.NoError(t, err)

		timer := runtime.Timer{
			ElementId:            "timer-elem",
			Key:                  r2 + 80,
			ElementInstanceKey:   ptr.To(r2 + 81),
			ProcessDefinitionKey: pd.Key,
			ProcessInstanceKey:   ptr.To(inst2.ProcessInstance().Key),
			TimerState:           runtime.TimerStateCreated,
			CreatedAt:            time.Now(),
			DueAt:                time.Now().Add(1 * time.Hour),
			Duration:             1 * time.Hour,
			Token:                &token,
		}
		err = db.SaveTimer(ctx, timer)
		assert.NoError(t, err)

		job := runtime.Job{
			ElementId:          "job-123",
			ElementInstanceKey: r2 + 91,
			ProcessInstanceKey: inst2.ProcessInstance().Key,
			Key:                r2 + 90,
			State:              runtime.ActivityStateActive,
			Type:               "test-job",
			InputVariables:     map[string]any{"foo": "bar"},
			CreatedAt:          time.Now(),
			Token:              token,
		}
		err = db.SaveJob(ctx, job)
		assert.NoError(t, err)

		flowHist := runtime.FlowElementInstance{
			Key:                job.Key,
			ProcessInstanceKey: inst2.ProcessInstance().Key,
			ElementId:          "job-123",
			CreatedAt:          job.CreatedAt,
		}
		err = db.SaveFlowElementInstance(ctx, flowHist)
		assert.NoError(t, err)
	}

	idsToBeDeleted := []int64{}
	idsToKeep := []int64{}
	idsWithoutTTL := []int64{}
	for i := range cleanupBatchSize {
		ctx := t.Context()
		if i < cleanupBatchSize/2 {
			ctx = appcontext.WithHistoryTTL(ctx, types.TTL(time.Hour))
			createTestData(ctx, &idsToKeep)
		} else {
			ctx = appcontext.WithHistoryTTL(ctx, types.TTL(time.Second))
			createTestData(ctx, &idsToBeDeleted)
		}
	}
	// Instances created without a history TTL must be retained forever.
	createTestData(t.Context(), &idsWithoutTTL)

	findInactiveIDs := func(at time.Time) ([]int64, error) {
		return db.Queries.FindInactiveInstancesToDelete(t.Context(), sql.FindInactiveInstancesToDeleteParams{
			CurrUnix: ssql.NullInt64{
				Int64: at.Unix(),
				Valid: true,
			},
			Limit: 100000,
		})
	}

	inactiveIDs, err := findInactiveIDs(time.Now())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(inactiveIDs))
	activeBeforeCleanup, err := db.Queries.FindActiveInstances(t.Context())
	assert.NoError(t, err)

	cleanupTriggered, err := db.dataCleanupWithLimit(t.Context(), time.Now(), cleanupBatchSize)
	assert.NoError(t, err)
	assert.False(t, cleanupTriggered)

	activeAfterCleanup, err := db.Queries.FindActiveInstances(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, activeBeforeCleanup, activeAfterCleanup)

	idsToComplete := slices.Concat(idsToBeDeleted, idsToKeep, idsWithoutTTL)
	for _, id := range idsToComplete {
		pi, err := db.FindProcessInstanceByKey(t.Context(), id)
		assert.NoError(t, err)
		pi.ProcessInstance().State = runtime.ActivityStateCompleted
		err = db.SaveProcessInstance(t.Context(), pi)
		assert.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		inactiveIDs, err = findInactiveIDs(time.Now())
		return err == nil && int64SlicesMatch(inactiveIDs, idsToBeDeleted)
	}, 10*time.Second, 100*time.Millisecond)
	assert.ElementsMatch(t, idsToBeDeleted, inactiveIDs)

	// Verify that cleanup also processes a partially filled batch.
	require.Eventually(t, func() bool {
		cleanupTriggered, err = db.dataCleanupWithLimit(t.Context(), time.Now(), len(idsToBeDeleted)+1)
		return err == nil && cleanupTriggered
	}, 10*time.Second, 100*time.Millisecond)

	inactiveIDs, err = findInactiveIDs(time.Now())
	require.NoError(t, err)
	require.Empty(t, inactiveIDs)
	// Each createTestData call creates two instances and one row per history table.
	remainingCalls := int64((len(idsToKeep) + len(idsWithoutTTL)) / 2)
	count := queryCount(t, db, "select count(*) from process_instance")
	require.Equal(t, int64(len(idsToKeep)+len(idsWithoutTTL)), count)
	count = queryCount(t, db, "select count(*) from message_subscription")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from timer")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from job")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from execution_token")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from flow_element_instance")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from incident")
	require.Equal(t, remainingCalls, count)
	count = queryCount(t, db, "select count(*) from error_subscription")
	require.Equal(t, remainingCalls, count)

	// Should delete only one instance.
	cleanupTriggered, err = db.dataCleanupWithLimit(t.Context(), time.Now().Add(2*time.Hour), 1)
	assert.NoError(t, err)
	assert.True(t, cleanupTriggered)

	inactiveIDs, err = findInactiveIDs(time.Now())
	require.NoError(t, err)
	require.Empty(t, inactiveIDs)
	count = queryCount(t, db, "select count(*) from process_instance")
	require.Equal(t, int64(len(idsToKeep)+len(idsWithoutTTL)-1), count)

	sumHistoryRows := func() int64 {
		return queryCount(t, db, "select count(*) from message_subscription") +
			queryCount(t, db, "select count(*) from timer") +
			queryCount(t, db, "select count(*) from job") +
			queryCount(t, db, "select count(*) from execution_token") +
			queryCount(t, db, "select count(*) from flow_element_instance") +
			queryCount(t, db, "select count(*) from incident") +
			queryCount(t, db, "select count(*) from error_subscription")
	}
	// The deleted instance is the newest deletable one: the child of the last
	// idsToKeep call, which owns 3 history rows (timer, job, flow element).
	require.Equal(t, remainingCalls*7-3, sumHistoryRows())

	// Instances without a TTL must never be cleaned up, no matter how far in
	// the future the cleanup runs.
	cleanupTriggered, err = db.dataCleanupWithLimit(t.Context(), time.Now().Add(2*time.Hour), defaultHistoryDeleteBatchSize)
	require.NoError(t, err)
	require.True(t, cleanupTriggered)

	inactiveIDs, err = findInactiveIDs(time.Now().Add(1000 * time.Hour))
	require.NoError(t, err)
	require.Empty(t, inactiveIDs)
	count = queryCount(t, db, "select count(*) from process_instance")
	require.Equal(t, int64(len(idsWithoutTTL)), count)
	require.Equal(t, int64(len(idsWithoutTTL)/2*7), sumHistoryRows())
}

func TestChildProcessInstanceInheritsParentHistoryTTL(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer func() {
		if err := partition.Stop(); err != nil {
			t.Logf("failed to stop partition: %s", err)
		}
	}()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-child-history-ttl")

	definition := runtime.ProcessDefinition{
		BpmnProcessId: "child-history-ttl-process",
		Version:       1,
		Key:           db.GenerateId(),
		BpmnData:      `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="child-history-ttl-process" name="child history ttl" isExecutable="true"></bpmn:process></xml>`,
		BpmnChecksum:  [16]byte{1},
	}
	require.NoError(t, db.SaveProcessDefinition(t.Context(), definition))

	parent := runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &definition,
			Key:            db.GenerateId(),
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateReady,
		},
	}
	parentContext := appcontext.WithHistoryTTL(t.Context(), types.TTL(time.Hour))
	require.NoError(t, db.SaveProcessInstance(parentContext, &parent))

	parentRow, err := db.Queries.GetProcessInstance(t.Context(), parent.ProcessInstance().Key)
	require.NoError(t, err)
	require.True(t, parentRow.HistoryTtlSec.Valid)
	require.Equal(t, int64(time.Hour.Seconds()), parentRow.HistoryTtlSec.Int64)

	parentToken := runtime.ExecutionToken{
		Key:                db.GenerateId(),
		ElementInstanceKey: db.GenerateId(),
		ElementId:          "sub-process",
		ProcessInstanceKey: parent.ProcessInstance().Key,
		State:              runtime.TokenStateWaiting,
	}
	require.NoError(t, db.SaveToken(t.Context(), parentToken))

	child := runtime.SubProcessInstance{
		ParentProcessExecutionToken:           parentToken,
		ParentProcessTargetElementInstanceKey: parentToken.ElementInstanceKey,
		ParentProcessTargetElementId:          parentToken.ElementId,
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &definition,
			Key:            db.GenerateId(),
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateReady,
			HistoryTTLSec:  new(int64(time.Hour.Seconds())),
		},
	}
	childContext := appcontext.WithHistoryTTL(t.Context(), types.TTL(2*time.Hour))
	require.NoError(t, db.SaveProcessInstance(childContext, &child))

	childRow, err := db.Queries.GetProcessInstance(t.Context(), child.ProcessInstance().Key)
	require.NoError(t, err)
	require.True(t, childRow.HistoryTtlSec.Valid)
	require.Equal(t, int64(time.Hour.Seconds()), childRow.HistoryTtlSec.Int64)

	// A child instance whose parent had no TTL carries no TTL on its runtime object
	// either, and must not pick one up from the resuming request's context.
	childWithoutTTL := runtime.SubProcessInstance{
		ParentProcessExecutionToken:           parentToken,
		ParentProcessTargetElementInstanceKey: parentToken.ElementInstanceKey,
		ParentProcessTargetElementId:          parentToken.ElementId,
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &definition,
			Key:            db.GenerateId(),
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateReady,
		},
	}
	require.NoError(t, db.SaveProcessInstance(childContext, &childWithoutTTL))

	childWithoutTTLRow, err := db.Queries.GetProcessInstance(t.Context(), childWithoutTTL.ProcessInstance().Key)
	require.NoError(t, err)
	require.False(t, childWithoutTTLRow.HistoryTtlSec.Valid)
}

func TestSaveProcessInstanceExplicitBusinessKeyOverridesContext(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer func() {
		if err := partition.Stop(); err != nil {
			t.Logf("failed to stop partition: %s", err)
		}
	}()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-business-key-override")
	definition := runtime.ProcessDefinition{
		BpmnProcessId: "business-key-override-process",
		Version:       1,
		Key:           db.GenerateId(),
		BpmnData:      `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="business-key-override-process" isExecutable="true"></bpmn:process>`,
		BpmnChecksum:  [16]byte{1},
	}
	require.NoError(t, db.SaveProcessDefinition(t.Context(), definition))

	emptyBusinessKey := ""
	instance := runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &definition,
			Key:            db.GenerateId(),
			BusinessKey:    &emptyBusinessKey,
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateReady,
		},
	}
	ctx := appcontext.WithBusinessKey(t.Context(), "context-business-key")
	require.NoError(t, db.SaveProcessInstance(ctx, &instance))

	row, err := db.Queries.GetProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.True(t, row.BusinessKey.Valid)
	assert.Equal(t, "", row.BusinessKey.String)
}

func queryCount(t *testing.T, db *DB, query string) int64 {
	row := db.QueryRowContext(t.Context(), query)
	var count int64
	err := row.Scan(&count)
	assert.NoError(t, err)
	return count
}

func int64SlicesMatch(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[int64]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}

func testMessageCorrelation(t *testing.T, db *DB, ts *servertest.TestServer) {
	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`
	r := db.GenerateId()
	pd := runtime.ProcessDefinition{
		BpmnProcessId: fmt.Sprintf("id-%d", r),
		Version:       1,
		Key:           r,
		BpmnData:      fmt.Sprintf(data, r),
		BpmnChecksum:  [16]byte{1},
	}
	err := db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	inst1 := runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &pd,
			Key:            r,
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateActive,
		},
	}

	err = db.SaveProcessInstance(t.Context(), &inst1)
	assert.NoError(t, err)
	token := runtime.ExecutionToken{
		Key:                r + 50,
		ElementInstanceKey: r + 60,
		ElementId:          "test-64654",
		ProcessInstanceKey: inst1.ProcessInstance().Key,
		State:              runtime.TokenStateRunning,
	}
	err = db.SaveToken(t.Context(), token)
	assert.NoError(t, err)

	t.Run("test message subscription db save", func(t *testing.T) {
		err = db.SaveMessageSubscription(t.Context(), &runtime.TokenMessageSubscription{
			Token:              runtime.ExecutionToken{Key: 46465132},
			ProcessInstanceKey: inst1.ProcessInstance().Key,
			CorrelationKey:     "duplicate_correlation_key",
			MessageSubscriptionData: runtime.MessageSubscriptionData{
				Key:                  db.GenerateId(),
				ElementId:            "123",
				ProcessDefinitionKey: pd.Key,
				Name:                 "test-message",
				State:                runtime.ActivityStateActive,
				CreatedAt:            time.Now(),
			},
		})
		assert.NoError(t, err)

		err = db.SaveMessageSubscription(t.Context(), &runtime.TokenMessageSubscription{
			Token:              runtime.ExecutionToken{Key: 16465133},
			ProcessInstanceKey: 1,
			CorrelationKey:     "duplicate_correlation_key",
			MessageSubscriptionData: runtime.MessageSubscriptionData{
				Key:                  db.GenerateId(),
				ElementId:            "123",
				ProcessDefinitionKey: 1,
				Name:                 "test-message",
				State:                runtime.ActivityStateActive,
				CreatedAt:            time.Now(),
			},
		})
		assert.Error(t, err)
	})
	t.Run("test message subscription batch save", func(t *testing.T) {
		err = db.SaveMessageSubscription(t.Context(), &runtime.TokenMessageSubscription{
			Token:              runtime.ExecutionToken{Key: 46465132},
			ProcessInstanceKey: inst1.ProcessInstance().Key,
			CorrelationKey:     "duplicate_correlation_key_batch",
			MessageSubscriptionData: runtime.MessageSubscriptionData{
				Key:                  db.GenerateId(),
				ElementId:            "124",
				ProcessDefinitionKey: pd.Key,
				Name:                 "test-message",
				State:                runtime.ActivityStateActive,
				CreatedAt:            time.Now(),
			},
		})
		assert.NoError(t, err)

		pointer, err := db.FindActiveMessageSubscriptionPointer(t.Context(), "test-message", "duplicate_correlation_key_batch")
		batch := db.NewBatch()
		err = batch.SaveMessageSubscription(t.Context(), &runtime.TokenMessageSubscription{
			Token:              runtime.ExecutionToken{Key: 16465133},
			ProcessInstanceKey: 3,
			CorrelationKey:     "duplicate_correlation_key_batch",
			MessageSubscriptionData: runtime.MessageSubscriptionData{
				Key:                  db.GenerateId(),
				ElementId:            "124",
				ProcessDefinitionKey: 3,
				Name:                 "test-message",
				State:                runtime.ActivityStateActive,
				CreatedAt:            time.Now(),
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
				ExecutionToken:       &pointer.MessageSubscriptionKey,
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
		BpmnProcessId: fmt.Sprintf("id-%d", r),
		Version:       1,
		Key:           r,
		BpmnData:      fmt.Sprintf(data, r),
		BpmnChecksum:  [16]byte{1},
	}
	err := db.SaveProcessDefinition(t.Context(), pd)
	assert.NoError(t, err)

	inst1 := runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &pd,
			Key:            r,
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateActive,
		},
	}

	err = db.SaveProcessInstance(t.Context(), &inst1)
	assert.NoError(t, err)

	tok1 := runtime.ExecutionToken{
		Key:                r,
		ElementInstanceKey: 12345,
		ElementId:          "some-id",
		ProcessInstanceKey: inst1.ProcessInstance().Key,
		State:              runtime.TokenStateWaiting,
	}
	err = db.SaveToken(t.Context(), tok1)
	assert.NoError(t, err)

	inst2 := runtime.CallActivityInstance{
		ParentProcessExecutionToken:           tok1,
		ParentProcessTargetElementInstanceKey: tok1.ElementInstanceKey,
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &pd,
			Key:            r + 1,
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      time.Now(),
			State:          runtime.ActivityStateActive,
		},
	}

	err = db.SaveProcessInstance(t.Context(), &inst2)
	assert.NoError(t, err)
	dbInst1, err := db.FindProcessInstanceByKey(t.Context(), inst1.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ProcessTypeDefault, dbInst1.Type())

	dbInst2, err := db.FindProcessInstanceByKey(t.Context(), inst2.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ProcessTypeCallActivity, dbInst2.Type())
	assert.NotNil(t, dbInst2.(*runtime.CallActivityInstance).ParentProcessExecutionToken)

	instncs, err := db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		SortByOrder:             ssql.NullString{String: "", Valid: false},
		ProcessDefinitionKey:    0,
		ParentInstanceKey:       0,
		BusinessKey:             ssql.NullString{String: "", Valid: false},
		BpmnProcessID:           ssql.NullString{String: "", Valid: false},
		CreatedFrom:             ssql.NullInt64{Int64: 0, Valid: false},
		CreatedTo:               ssql.NullInt64{Int64: 0, Valid: false},
		State:                   ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeCallActivity:  ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeMultiInstance: ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeDefault:       ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeSubProcess:    ssql.NullInt64{Int64: 0, Valid: false},
		Offset:                  0,
		Size:                    20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 5)

	instncs, err = db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		SortByOrder:             ssql.NullString{String: "", Valid: false},
		ProcessDefinitionKey:    dbInst2.ProcessInstance().Definition.Key,
		ParentInstanceKey:       0,
		BusinessKey:             ssql.NullString{String: "", Valid: false},
		BpmnProcessID:           ssql.NullString{String: "", Valid: false},
		CreatedFrom:             ssql.NullInt64{Int64: 0, Valid: false},
		CreatedTo:               ssql.NullInt64{Int64: 0, Valid: false},
		State:                   ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeCallActivity:  ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeMultiInstance: ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeDefault:       ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeSubProcess:    ssql.NullInt64{Int64: 0, Valid: false},
		Offset:                  0,
		Size:                    20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 2)

	instncs, err = db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		SortByOrder:             ssql.NullString{String: "", Valid: false},
		ProcessDefinitionKey:    dbInst2.ProcessInstance().Definition.Key,
		ParentInstanceKey:       tok1.Key,
		BusinessKey:             ssql.NullString{String: "", Valid: false},
		BpmnProcessID:           ssql.NullString{String: "", Valid: false},
		CreatedFrom:             ssql.NullInt64{Int64: 0, Valid: false},
		CreatedTo:               ssql.NullInt64{Int64: 0, Valid: false},
		State:                   ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeCallActivity:  ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeMultiInstance: ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeDefault:       ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeSubProcess:    ssql.NullInt64{Int64: 0, Valid: false},
		Offset:                  0,
		Size:                    20,
	})
	assert.NoError(t, err)
	assert.Len(t, instncs, 1)

	subProcesses, err := db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		SortByOrder:             ssql.NullString{String: "", Valid: false},
		ProcessDefinitionKey:    inst2.ProcessInstance().Definition.Key,
		ParentInstanceKey:       inst1.ProcessInstance().GetInstanceKey(),
		BusinessKey:             ssql.NullString{String: "", Valid: false},
		BpmnProcessID:           ssql.NullString{String: "", Valid: false},
		CreatedFrom:             ssql.NullInt64{Int64: 0, Valid: false},
		CreatedTo:               ssql.NullInt64{Int64: 0, Valid: false},
		State:                   ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeCallActivity:  ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeMultiInstance: ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeDefault:       ssql.NullInt64{Int64: 0, Valid: false},
		FilterTypeSubProcess:    ssql.NullInt64{Int64: 0, Valid: false},
		Offset:                  0,
		Size:                    20,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, subProcesses)
	assert.Equal(t, tok1.Key, subProcesses[0].ParentProcessExecutionToken.Int64)

	tok2 := runtime.ExecutionToken{
		Key:                db.GenerateId(),
		ElementInstanceKey: tok1.ElementInstanceKey + 1,
		ElementId:          "some-other-id",
		ProcessInstanceKey: inst1.ProcessInstance().Key,
		State:              runtime.TokenStateWaiting,
	}
	require.NoError(t, db.SaveToken(t.Context(), tok2))

	inst3 := runtime.CallActivityInstance{
		ParentProcessExecutionToken:           tok2,
		ParentProcessTargetElementInstanceKey: tok2.ElementInstanceKey,
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition:     &pd,
			Key:            db.GenerateId(),
			VariableHolder: runtime.VariableHolder{},
			CreatedAt:      inst2.ProcessInstance().CreatedAt.Add(time.Second),
			State:          runtime.ActivityStateActive,
		},
	}
	require.NoError(t, db.SaveProcessInstance(t.Context(), &inst3))

	childProcessParams := sql.FindChildProcessInstancesPageParams{
		SortByOrder:             ssql.NullString{String: "createdAt_asc", Valid: true},
		ParentInstanceKey:       inst1.ProcessInstance().GetInstanceKey(),
		FilterTypeCallActivity:  int64(runtime.ProcessTypeCallActivity),
		FilterTypeMultiInstance: int64(runtime.ProcessTypeMultiInstance),
		FilterTypeSubProcess:    int64(runtime.ProcessTypeSubProcess),
		State:                   ssql.NullInt64{Valid: false},
		Offset:                  0,
		Size:                    20,
	}
	childProcesses, err := db.Queries.FindChildProcessInstancesPage(t.Context(), childProcessParams)
	require.NoError(t, err)
	require.Len(t, childProcesses, 2)
	assert.Equal(t, inst2.ProcessInstance().GetInstanceKey(), childProcesses[0].Key)
	assert.Equal(t, tok1.Key, childProcesses[0].ParentProcessExecutionToken.Int64)
	assert.Equal(t, inst3.ProcessInstance().GetInstanceKey(), childProcesses[1].Key)
	assert.Equal(t, tok2.Key, childProcesses[1].ParentProcessExecutionToken.Int64)
	assert.Equal(t, int64(2), childProcesses[0].TotalCount)

	childProcessParams.State = ssql.NullInt64{Int64: int64(runtime.ActivityStateCompleted), Valid: true}
	childProcesses, err = db.Queries.FindChildProcessInstancesPage(t.Context(), childProcessParams)
	require.NoError(t, err)
	assert.Empty(t, childProcesses)
}

func testFindProcessInstancesPageFiltersAndSort(t *testing.T, db *DB) {
	t.Helper()

	definitionKey := db.GenerateId()
	definition := runtime.ProcessDefinition{
		BpmnProcessId: fmt.Sprintf("filtered-process-%d", definitionKey),
		Version:       1,
		Key:           definitionKey,
		BpmnData:      `<bpmn:process id="filtered-process" isExecutable="true"></bpmn:process>`,
		BpmnChecksum:  [16]byte{1},
	}
	require.NoError(t, db.SaveProcessDefinition(t.Context(), definition))

	createdAt := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	instances := []*runtime.DefaultProcessInstance{
		{
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &definition,
				Key:            db.GenerateId(),
				VariableHolder: runtime.VariableHolder{},
				CreatedAt:      createdAt,
				State:          runtime.ActivityStateTerminated,
			},
		},
		{
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &definition,
				Key:            db.GenerateId(),
				VariableHolder: runtime.VariableHolder{},
				CreatedAt:      createdAt.Add(time.Second),
				State:          runtime.ActivityStateCompleted,
			},
		},
		{
			ProcessInstanceData: runtime.ProcessInstanceData{
				Definition:     &definition,
				Key:            db.GenerateId(),
				VariableHolder: runtime.VariableHolder{},
				CreatedAt:      createdAt.Add(2 * time.Second),
				State:          runtime.ActivityStateTerminated,
			},
		},
	}
	for _, instance := range instances {
		require.NoError(t, db.SaveProcessInstance(t.Context(), instance))
	}

	result, err := db.Queries.FindProcessInstancesPage(t.Context(), sql.FindProcessInstancesPageParams{
		SortByOrder:          ssql.NullString{String: "createdAt_asc", Valid: true},
		ProcessDefinitionKey: 0,
		ParentInstanceKey:    0,
		BpmnProcessID:        ssql.NullString{String: definition.BpmnProcessId, Valid: true},
		CreatedFrom:          ssql.NullInt64{Int64: createdAt.UnixMilli(), Valid: true},
		CreatedTo:            ssql.NullInt64{Valid: false},
		State:                ssql.NullInt64{Int64: int64(runtime.ActivityStateTerminated), Valid: true},
		Offset:               0,
		Size:                 20,
	})
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, instances[0].ProcessInstance().Key, result[0].Key)
	assert.Equal(t, instances[2].ProcessInstance().Key, result[1].Key)
	assert.Equal(t, int64(2), result[0].TotalCount)
	assert.Equal(t, int64(2), result[1].TotalCount)
}

func TestGetLatestDecisionDefinitionById(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-decision-def")

	// Helper to create and save a DMN resource definition
	saveDmnResource := func(t *testing.T, key int64) dmnruntime.DmnResourceDefinition {
		def := dmnruntime.DmnResourceDefinition{
			Id:                fmt.Sprintf("dmn-resource-%d", key),
			Version:           1,
			Key:               key,
			DmnData:           []byte(fmt.Sprintf("dmn-data-%d", key)),
			DmnChecksum:       [16]byte{1},
			DmnDefinitionName: fmt.Sprintf("resource-%d", key),
		}
		err := db.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)
		return def
	}

	// Helper to create and save a decision definition
	saveDecision := func(t *testing.T, key int64, version int64, decisionId string, versionTag string, dmnResourceDef dmnruntime.DmnResourceDefinition) dmnruntime.DecisionDefinition {
		dec := dmnruntime.DecisionDefinition{
			Key:                      key,
			Version:                  version,
			Id:                       decisionId,
			VersionTag:               versionTag,
			DmnResourceDefinitionId:  dmnResourceDef.Id,
			DmnResourceDefinitionKey: dmnResourceDef.Key,
		}
		err := db.SaveDecisionDefinition(t.Context(), dec)
		assert.NoError(t, err)
		return dec
	}

	t.Run("returns decision definition when found", func(t *testing.T) {
		resourceKey := db.GenerateId()
		dmnResource := saveDmnResource(t, resourceKey)

		decKey := db.GenerateId()
		decisionId := fmt.Sprintf("decision-%d", decKey)
		saved := saveDecision(t, decKey, 1, decisionId, "v1", dmnResource)

		result, err := db.GetLatestDecisionDefinitionById(t.Context(), decisionId)
		assert.NoError(t, err)
		assert.Equal(t, saved.Key, result.Key)
		assert.Equal(t, saved.Version, result.Version)
		assert.Equal(t, saved.Id, result.Id)
		assert.Equal(t, saved.VersionTag, result.VersionTag)
		assert.Equal(t, saved.DmnResourceDefinitionId, result.DmnResourceDefinitionId)
		assert.Equal(t, saved.DmnResourceDefinitionKey, result.DmnResourceDefinitionKey)
	})

	t.Run("returns latest version when multiple versions exist", func(t *testing.T) {
		resourceKey := db.GenerateId()
		dmnResource := saveDmnResource(t, resourceKey)

		decisionId := fmt.Sprintf("decision-multi-%d", resourceKey)

		decKey1 := db.GenerateId()
		saveDecision(t, decKey1, 1, decisionId, "v1", dmnResource)

		decKey2 := db.GenerateId()
		latestSaved := saveDecision(t, decKey2, 2, decisionId, "v2", dmnResource)

		result, err := db.GetLatestDecisionDefinitionById(t.Context(), decisionId)
		assert.NoError(t, err)
		assert.Equal(t, latestSaved.Key, result.Key)
		assert.Equal(t, int64(2), result.Version)
		assert.Equal(t, decisionId, result.Id)
		assert.Equal(t, "v2", result.VersionTag)
	})

	t.Run("returns ErrNotFound when decision id does not exist", func(t *testing.T) {
		_, err := db.GetLatestDecisionDefinitionById(t.Context(), "non-existent-decision-id")
		assert.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})
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
