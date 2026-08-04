package e2e

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemStatus(t *testing.T) {
	t.Run("returns build metadata", systemStatusReturnsBuildMetadata)
}

func systemStatusReturnsBuildMetadata(t *testing.T) {
	response, err := app.NewRequest(nil).WithPath("/system/status").DoOk()
	require.NoError(t, err)

	status := ClusterStatus{}
	require.NoError(t, json.Unmarshal(response, &status))
	buildInfo, err := buildinfo.Current()
	require.NoError(t, err)
	assert.Equal(t, buildInfo.Version, status.Version)
	assert.Equal(t, buildInfo.Commit, status.Commit)
}
