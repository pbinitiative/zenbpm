package partition

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetRqLiteDefaultConfigSnapshotDefaults(t *testing.T) {
	cfg := GetRqLiteDefaultConfig("node-1", "localhost:8090", t.TempDir(), []string{"localhost:8090"})

	require.Equal(t, uint64(8192), cfg.RaftSnapThreshold)
	require.Equal(t, uint64(50*1024*1024), cfg.RaftSnapThresholdWALSize)
	require.Equal(t, time.Minute, cfg.RaftSnapInterval)
}
