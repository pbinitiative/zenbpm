package partition

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func TestDBSizeBytesReturnsErrorWhenDatabaseFilesAreMissing(t *testing.T) {
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: t.TempDir()}}

	size, err := zpn.dbSizeBytes()

	assert.Zero(t, size)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sqlite database files")
}

func TestDBSizeBytesSumsSQLiteDatabaseFiles(t *testing.T) {
	dataPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, "db.sqlite"), []byte("database"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, "db.sqlite-wal"), []byte("wal"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, "unrelated"), []byte("ignored"), 0o600))
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: dataPath}}

	size, err := zpn.dbSizeBytes()

	require.NoError(t, err)
	assert.Equal(t, int64(len("database")+len("wal")), size)
}

func TestShouldRecordLeaderChange(t *testing.T) {
	tests := []struct {
		name       string
		previousID string
		newID      string
		expected   bool
	}{
		{name: "initial election", newID: "node-1", expected: false},
		{name: "leadership loss", previousID: "node-1", expected: false},
		{name: "same leader observed again", previousID: "node-1", newID: "node-1", expected: false},
		{name: "leader changed", previousID: "node-1", newID: "node-2", expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, shouldRecordLeaderChange(tt.previousID, tt.newID))
		})
	}
}

func TestRaftLogSizeBytesReturnsZeroWhenLogIsMissing(t *testing.T) {
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: t.TempDir()}}

	size, err := zpn.raftLogSizeBytes()

	require.NoError(t, err)
	assert.Zero(t, size)
}

func TestRaftLogSizeBytesReturnsLogFileSize(t *testing.T) {
	dataPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, raftLogFileName), []byte("raft-log"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, "db.sqlite"), []byte("ignored"), 0o600))
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: dataPath}}

	size, err := zpn.raftLogSizeBytes()

	require.NoError(t, err)
	assert.Equal(t, int64(len("raft-log")), size)
}

func TestNewestSnapshotReturnsZeroValuesWhenNoSnapshotsExist(t *testing.T) {
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: t.TempDir()}}

	name, modTime, err := zpn.newestSnapshot()

	require.NoError(t, err)
	assert.Empty(t, name)
	assert.True(t, modTime.IsZero())
}

func TestNewestSnapshotPicksMostRecentlyModifiedEntry(t *testing.T) {
	dataPath := t.TempDir()
	older := writeSnapshot(t, dataPath, "2-30-1785351717000", time.Now().Add(-2*time.Hour))
	newest := writeSnapshot(t, dataPath, "2-31-1785351717976", time.Now().Add(-1*time.Hour))
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: dataPath}}

	name, modTime, err := zpn.newestSnapshot()

	require.NoError(t, err)
	assert.Equal(t, filepath.Base(newest), name)
	assert.NotEqual(t, filepath.Base(older), name)
	assert.WithinDuration(t, time.Now().Add(-1*time.Hour), modTime, time.Minute)
}

func TestNewestSnapshotIgnoresHousekeepingAndIncompleteEntries(t *testing.T) {
	dataPath := t.TempDir()
	completedAt := time.Now().Add(-time.Hour)
	completed := writeSnapshot(t, dataPath, "2-31-1785351717976", completedAt)
	snapshotDir := filepath.Join(dataPath, snapshotsDirName)
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "REAP_PLAN"), []byte("newer housekeeping file"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "2-32-1785351718976.tmp"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "not-a-snapshot"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "2-33-1785351719976"), 0o750))
	mismatched := filepath.Join(snapshotDir, "2-34-1785351720976")
	require.NoError(t, os.MkdirAll(mismatched, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(mismatched, snapshotMetadataFileName), []byte(`{"ID":"different-snapshot"}`), 0o600))
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: dataPath}}

	name, modTime, err := zpn.newestSnapshot()

	require.NoError(t, err)
	assert.Equal(t, filepath.Base(completed), name)
	assert.WithinDuration(t, completedAt, modTime, time.Second)
}

func TestNewestSnapshotIgnoresMetadataSymlinkedOutsideSnapshotDirectory(t *testing.T) {
	dataPath := t.TempDir()
	completedAt := time.Now().Add(-time.Hour)
	completed := writeSnapshot(t, dataPath, "2-31-1785351717976", completedAt)

	// A newer entry whose meta.json points outside the snapshot directory must
	// never be read: reads are confined to the snapshot directory, so the entry
	// counts as incomplete and the older valid snapshot stays the newest one.
	outside := filepath.Join(dataPath, "outside-meta.json")
	require.NoError(t, os.WriteFile(outside, []byte(`{"ID":"2-99-1785351799999"}`), 0o600))
	escaping := filepath.Join(dataPath, snapshotsDirName, "2-99-1785351799999")
	require.NoError(t, os.MkdirAll(escaping, 0o750))
	require.NoError(t, os.Symlink(outside, filepath.Join(escaping, snapshotMetadataFileName)))
	zpn := &ZenPartitionNode{config: &config.RqLite{DataPath: dataPath}}

	name, modTime, err := zpn.newestSnapshot()

	require.NoError(t, err)
	assert.Equal(t, filepath.Base(completed), name)
	assert.WithinDuration(t, completedAt, modTime, time.Second)
}

func TestUpdateRaftStorageMetricsRecordsSizeAndSnapshotAge(t *testing.T) {
	dataPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataPath, raftLogFileName), []byte("raft-log"), 0o600))
	writeSnapshot(t, dataPath, "2-31-1785351717976", time.Now().Add(-90*time.Second))

	logSize, snapshotAge, observationAge := &fakeGauge{}, &fakeGauge{}, &fakeGauge{}
	zpn := &ZenPartitionNode{
		PartitionId: 1,
		config:      &config.RqLite{DataPath: dataPath},
		logger:      hclog.NewNullLogger(),
	}
	zpn.metrics.raftLogSize = logSize
	zpn.metrics.snapshotAge = snapshotAge
	zpn.metrics.snapshotObservationAge = observationAge

	zpn.updateRaftStorageMetrics(context.Background(), metric.WithAttributes(attribute.Int64("partition", 1)))

	require.Len(t, logSize.recorded(), 1)
	assert.Equal(t, int64(len("raft-log")), logSize.recorded()[0].value)
	require.Len(t, snapshotAge.recorded(), 1)
	assert.InDelta(t, 90, snapshotAge.recorded()[0].value, 5)
	require.Len(t, observationAge.recorded(), 1)
	assert.Equal(t, int64(-1), observationAge.recorded()[0].value, "a pre-existing snapshot only establishes the startup baseline")
}

func TestUpdateRaftStorageMetricsKeepsObservationTimeUntilNewSnapshot(t *testing.T) {
	dataPath := t.TempDir()
	writeSnapshot(t, dataPath, "2-31-1785351717976", time.Now())

	observationAge := &fakeGauge{}
	zpn := &ZenPartitionNode{
		PartitionId: 1,
		config:      &config.RqLite{DataPath: dataPath},
		logger:      hclog.NewNullLogger(),
	}
	zpn.metrics.snapshotObservationAge = observationAge
	attrs := metric.WithAttributes(attribute.Int64("partition", 1))

	zpn.updateRaftStorageMetrics(context.Background(), attrs)
	assert.True(t, zpn.lastSnapshotObservedAt.IsZero(), "the startup baseline is not a new observation")
	writeSnapshot(t, dataPath, "2-42-1785351719999", time.Now())
	zpn.updateRaftStorageMetrics(context.Background(), attrs)
	firstObservedAt := zpn.lastSnapshotObservedAt
	require.False(t, firstObservedAt.IsZero())
	zpn.updateRaftStorageMetrics(context.Background(), attrs)

	assert.Equal(t, firstObservedAt, zpn.lastSnapshotObservedAt, "no new snapshot must not reset the observation time")
	assert.Len(t, observationAge.recorded(), 3)
}

func TestUpdateRaftStorageMetricsRecordsFirstSnapshotCreatedAfterStartup(t *testing.T) {
	dataPath := t.TempDir()
	observationAge := &fakeGauge{}
	zpn := &ZenPartitionNode{
		PartitionId: 1,
		config:      &config.RqLite{DataPath: dataPath},
		logger:      hclog.NewNullLogger(),
	}
	zpn.metrics.snapshotObservationAge = observationAge
	attrs := metric.WithAttributes(attribute.Int64("partition", 1))

	zpn.updateRaftStorageMetrics(context.Background(), attrs)
	writeSnapshot(t, dataPath, "2-31-1785351717976", time.Now())
	zpn.updateRaftStorageMetrics(context.Background(), attrs)

	require.Len(t, observationAge.recorded(), 2)
	assert.Equal(t, int64(-1), observationAge.recorded()[0].value)
	assert.InDelta(t, 0, observationAge.recorded()[1].value, 1)
}

// writeSnapshot creates a snapshot directory with the given modification time
// inside the rqlite snapshot directory of dataPath and returns its full path.
func writeSnapshot(t *testing.T, dataPath, name string, modTime time.Time) string {
	t.Helper()
	dir := filepath.Join(dataPath, snapshotsDirName, name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotMetadataFileName), []byte(`{"ID":"`+name+`"}`), 0o600))
	require.NoError(t, os.Chtimes(dir, modTime, modTime))
	return dir
}
