package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreFenceRejectsWritesWithoutTheOwnerToken(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	ctx := t.Context()
	db := partition.DB

	insert := func(ctx context.Context, key int64) error {
		processID := fmt.Sprintf("fenced-%d", key)
		_, err := db.ExecContext(ctx,
			"INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_process_name) VALUES (?, ?, ?, ?, ?, ?)",
			key, int64(1), processID, "<x/>", []byte{1}, processID)
		return err
	}

	// no fence: writes flow
	require.NoError(t, insert(ctx, 1))
	_, fenced := db.RestoreFence()
	assert.False(t, fenced)

	token := RestoreToken{OperationID: "op-1", Epoch: 2}
	db.EnterRestoreFence(token)
	fence, fenced := db.RestoreFence()
	assert.True(t, fenced)
	assert.Equal(t, token, fence)

	// application writes (no token) and writes of a superseded owner are refused
	err := insert(ctx, 2)
	require.ErrorIs(t, err, ErrPartitionFenced)
	err = insert(WithRestoreToken(ctx, RestoreToken{OperationID: "op-1", Epoch: 1}), 3)
	require.ErrorIs(t, err, ErrPartitionFenced)
	err = insert(WithRestoreToken(ctx, RestoreToken{OperationID: "op-0", Epoch: 2}), 4)
	require.ErrorIs(t, err, ErrPartitionFenced)
	assert.Equal(t, int64(1), queryCount(t, db, "SELECT COUNT(*) FROM process_definition"))

	// the owner's writes flow; reads are never fenced
	require.NoError(t, insert(WithRestoreToken(ctx, token), 5))
	assert.Equal(t, int64(2), queryCount(t, db, "SELECT COUNT(*) FROM process_definition"))

	// a newer owner replaces the fence
	newer := RestoreToken{OperationID: "op-2", Epoch: 3}
	db.EnterRestoreFence(newer)
	require.ErrorIs(t, insert(WithRestoreToken(ctx, token), 6), ErrPartitionFenced)
	require.NoError(t, insert(WithRestoreToken(ctx, newer), 7))

	db.LeaveRestoreFence()
	require.NoError(t, insert(ctx, 8))
}

// TestDefinitionImporterImportsWithoutStartingExecution covers the restore
// reconciliation path: definitions land in a fenced partition through an
// engine that is never started, definition-level subscriptions are created
// only when asked, and the imported process runs once the partition is
// un-gated.
func TestDefinitionImporterImportsWithoutStartingExecution(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	ctx := t.Context()
	db := partition.DB

	startEnd := readFixture(t, "..", "..", "..", "pkg", "bpmn", "test-cases", "start-end.bpmn")
	timerStart := readFixture(t, "..", "..", "..", "test", "e2e", "testdata", "timer_start_event", "timer_start_event.bpmn")
	dmn := readFixture(t, "..", "..", "..", "pkg", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn")

	token := RestoreToken{OperationID: "op-1", Epoch: 1}
	db.EnterRestoreFence(token)
	fencedCtx := WithRestoreToken(ctx, token)

	importer := partition.NewDefinitionImporter()
	require.NoError(t, importer.ImportProcessDefinition(fencedCtx, 4711, 1, startEnd, false))
	require.NoError(t, importer.ImportProcessDefinition(fencedCtx, 4712, 1, timerStart, true))
	require.NoError(t, importer.ImportDmnResourceDefinition(fencedCtx, 4713, 1, dmn, map[string]int32{"example_canAutoLiquidateRule": 1}))
	// importing is idempotent: the same key is a no-op
	require.NoError(t, importer.ImportProcessDefinition(fencedCtx, 4711, 1, startEnd, false))
	importer.Close()

	// the fence held: an import without the owner's token is refused
	simpleTask := readFixture(t, "..", "..", "..", "pkg", "bpmn", "test-cases", "simple_task.bpmn")
	unfenced := partition.NewDefinitionImporter()
	err := unfenced.ImportProcessDefinition(ctx, 4714, 1, simpleTask, false)
	unfenced.Close()
	require.ErrorIs(t, err, ErrPartitionFenced)

	def, err := db.FindProcessDefinitionByKey(ctx, 4711)
	require.NoError(t, err)
	assert.Equal(t, int64(4711), def.Key)
	assert.Equal(t, int32(1), def.Version)
	assert.Equal(t, int64(2), queryCount(t, db, "SELECT COUNT(*) FROM process_definition"))
	assert.Equal(t, int64(0), queryCount(t, db, "SELECT COUNT(*) FROM process_instance"), "importing never starts execution")

	// subscriptions were registered only for the definition that owns them
	timers, err := db.FindProcessDefinitionTimers(ctx, 4712, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.Len(t, timers, 1, "the timer start event of the imported definition is armed")
	timers, err = db.FindProcessDefinitionTimers(ctx, 4711, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, timers)

	dmnDef, err := db.FindDmnResourceDefinitionByKey(ctx, 4713)
	require.NoError(t, err)
	assert.Equal(t, int64(4713), dmnDef.Key)
	assert.Equal(t, int64(1), dmnDef.Version)

	// once the partition is un-gated a regular engine executes the imported process
	db.LeaveRestoreFence()
	engine := bpmn.NewEngine(bpmn.EngineWithStorage(db))
	defer engine.Stop()
	instance, err := engine.CreateInstanceByKey(ctx, 4711, nil)
	require.NoError(t, err)
	assert.Equal(t, bpmnruntime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func readFixture(t *testing.T, parts ...string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(parts...))
	require.NoError(t, err)
	return data
}

func TestLoadUnderFenceIsOrderedAgainstFenceChanges(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	db := partition.DB
	token := RestoreToken{OperationID: "op-1", Epoch: 1}

	// no fence: the load is refused
	err := db.LoadUnderFence(token, func() error { t.Fatal("load must not run"); return nil })
	require.ErrorIs(t, err, ErrPartitionFenced)

	db.EnterRestoreFence(RestoreToken{OperationID: "op-2", Epoch: 2})
	err = db.LoadUnderFence(token, func() error { t.Fatal("load must not run"); return nil })
	require.ErrorIs(t, err, ErrPartitionFenced, "a superseded owner cannot load")

	db.EnterRestoreFence(token)
	loadStarted := make(chan struct{})
	release := make(chan struct{})
	loadDone := make(chan error, 1)
	go func() {
		loadDone <- db.LoadUnderFence(token, func() error {
			close(loadStarted)
			<-release
			return nil
		})
	}()
	<-loadStarted

	// a takeover arriving mid-load waits for the load to finish
	fenceChanged := make(chan struct{})
	go func() {
		db.EnterRestoreFence(RestoreToken{OperationID: "op-3", Epoch: 3})
		close(fenceChanged)
	}()
	select {
	case <-fenceChanged:
		t.Fatal("fence change must wait for the in-flight load")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-loadDone)
	select {
	case <-fenceChanged:
	case <-time.After(2 * time.Second):
		t.Fatal("fence change did not proceed after the load finished")
	}
	// and the old owner is refused from now on
	err = db.LoadUnderFence(token, func() error { t.Fatal("load must not run"); return nil })
	require.ErrorIs(t, err, ErrPartitionFenced)
}

func TestRestoreFenceRefusesTaggedWritesWhenUnfenced(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	db := partition.DB
	ctx := WithRestoreToken(t.Context(), RestoreToken{OperationID: "retired", Epoch: 1})
	_, err := db.ExecContext(ctx,
		"INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_process_name) VALUES (?, ?, ?, ?, ?, ?)",
		int64(1), int64(1), "retired", "<x/>", []byte{1}, "retired")
	require.ErrorIs(t, err, ErrPartitionFenced, "a retired restore must not write as an ordinary client")
}

// TestDefinitionImporterRegistersMessageStartWithoutPointerRPC covers a
// message start event whose routing pointer would normally be written through
// the ordinary cross-partition RPC. Under a restore token the subscription is
// stored locally and no pointer is written: the coordinator rebuilds every
// pointer table after definition sync.
func TestDefinitionImporterRegistersMessageStartWithoutPointerRPC(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	ctx := t.Context()
	db := partition.DB

	messageStart := readFixture(t, "..", "..", "..", "test", "e2e", "testdata", "message_event", "message-start-event-process.bpmn")
	token := RestoreToken{OperationID: "op-1", Epoch: 1}
	db.EnterRestoreFence(token)
	importer := partition.NewDefinitionImporter()
	defer importer.Close()
	require.NoError(t, importer.ImportProcessDefinition(WithRestoreToken(ctx, token), 4721, 1, messageStart, true))

	assert.Equal(t, int64(1), queryCount(t, db, "SELECT COUNT(*) FROM message_subscription WHERE process_definition_key = 4721"),
		"the definition-level message subscription is stored on the owning partition")
	assert.Equal(t, int64(0), queryCount(t, db, "SELECT COUNT(*) FROM message_subscription_pointer"),
		"no routing pointer is written while fenced; the restore coordinator rebuilds them")
}

// TestDefinitionImporterKeepsHistoricalVersionsHistorical covers the
// reconciliation of a partition whose version history has a hole: version 2
// of a process is present, version 1 is imported afterwards. The import must
// store it as version 1, leave version 2 the latest one with its
// subscriptions intact, and refuse a copy that would collide with a version
// the partition already holds under another key.
func TestDefinitionImporterKeepsHistoricalVersionsHistorical(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	ctx := t.Context()
	db := partition.DB

	timerStart := readFixture(t, "..", "..", "..", "test", "e2e", "testdata", "timer_start_event", "timer_start_event.bpmn")
	token := RestoreToken{OperationID: "op-1", Epoch: 1}
	db.EnterRestoreFence(token)
	fencedCtx := WithRestoreToken(ctx, token)
	importer := partition.NewDefinitionImporter()
	defer importer.Close()

	require.NoError(t, importer.ImportProcessDefinition(fencedCtx, 4802, 2, timerStart, true))
	require.NoError(t, importer.ImportProcessDefinition(fencedCtx, 4801, 1, timerStart, true))

	v1, err := db.FindProcessDefinitionByKey(ctx, 4801)
	require.NoError(t, err)
	assert.Equal(t, int32(1), v1.Version, "the version is copied from the source partition")
	latest, err := db.FindLatestProcessDefinitionById(ctx, v1.BpmnProcessId)
	require.NoError(t, err)
	assert.Equal(t, int64(4802), latest.Key, "a historical version never becomes the latest one")

	timers, err := db.FindProcessDefinitionTimers(ctx, 4802, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.Len(t, timers, 1, "the subscriptions of the latest version survive the import")
	timers, err = db.FindProcessDefinitionTimers(ctx, 4801, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, timers, "a historical version registers no subscriptions")

	err = importer.ImportProcessDefinition(fencedCtx, 4803, 2, timerStart, true)
	require.ErrorIs(t, err, storage.ErrUniqueConstraint, "version 2 is already held by another definition")
	assert.Equal(t, int64(2), queryCount(t, db, "SELECT COUNT(*) FROM process_definition"))
}
