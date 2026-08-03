package partition

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
