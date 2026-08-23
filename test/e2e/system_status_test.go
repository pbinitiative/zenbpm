package e2e

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemStatus(t *testing.T) {
	t.Run("returns build metadata", func(t *testing.T) {
		response, err := app.NewRequest(nil).WithPath("/system/status").DoOk()
		require.NoError(t, err)

		status := ClusterStatus{}
		require.NoError(t, json.Unmarshal(response, &status))
		buildInfo := buildinfo.Current()
		expectedCommitID := buildInfo.Commit
		if len(expectedCommitID) > 12 {
			expectedCommitID = expectedCommitID[:12]
		}
		assert.Equal(t, buildInfo.Branch, status.Git.Branch)
		assert.Equal(t, expectedCommitID, status.Git.CommitID)
		assert.Equal(t, buildInfo.Version, status.Build.Version)
		assert.Equal(t, buildInfo.BuildTime, status.Build.Time)
	})
}
