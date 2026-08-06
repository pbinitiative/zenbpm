package buildinfo

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildInfo(t *testing.T) {
	t.Run("prefers the version injected by the release build", func(t *testing.T) {
		actual, err := resolveVersion("1.5.0-rc1", nil, errors.New("spec unavailable"))

		require.NoError(t, err)
		assert.Equal(t, "1.5.0-rc1", actual)
	})

	t.Run("uses the embedded OpenAPI version when no version was injected", func(t *testing.T) {
		actual, err := resolveVersion("", []byte(`{"info":{"version":"1.5.0"}}`), nil)

		require.NoError(t, err)
		assert.Equal(t, "1.5.0", actual)
	})

	t.Run("returns unknown when the embedded OpenAPI specification cannot be read", func(t *testing.T) {
		actual, err := resolveVersion("", nil, errors.New("spec unavailable"))

		assert.Equal(t, unknownVersion, actual)
		assert.ErrorContains(t, err, "read embedded OpenAPI specification")
	})

	t.Run("returns unknown when the embedded OpenAPI specification is invalid", func(t *testing.T) {
		actual, err := resolveVersion("", []byte(`{"info":`), nil)

		assert.Equal(t, unknownVersion, actual)
		assert.ErrorContains(t, err, "decode embedded OpenAPI specification")
	})

	t.Run("prefers the commit injected by the build pipeline", func(t *testing.T) {
		settings := []debug.BuildSetting{{Key: "vcs.revision", Value: "019fcc9239db744b9bd0c0544f71af3a"}}

		actual := resolveCommit("whatever", settings)

		assert.Equal(t, "whatever", actual)
	})

	t.Run("uses the Go VCS revision when no commit was injected", func(t *testing.T) {
		settings := []debug.BuildSetting{{Key: "vcs.revision", Value: "019fcc9239db744b9bd0c0544f71af3a"}}

		actual := resolveCommit(unknownCommit, settings)

		assert.Equal(t, "019fcc9239db744b9bd0c0544f71af3a", actual)
	})

	t.Run("uses the Go VCS revision when an empty commit was injected", func(t *testing.T) {
		settings := []debug.BuildSetting{{Key: "vcs.revision", Value: "019fcc9239db744b9bd0c0544f71af3a"}}

		actual := resolveCommit("", settings)

		assert.Equal(t, "019fcc9239db744b9bd0c0544f71af3a", actual)
	})

	t.Run("normalizes an empty commit to unknown when build metadata has no revision", func(t *testing.T) {
		actual := resolveCommit("", nil)

		assert.Equal(t, unknownCommit, actual)
	})

	t.Run("returns unknown when build metadata has no revision", func(t *testing.T) {
		actual := resolveCommit(unknownCommit, []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}})

		assert.Equal(t, unknownCommit, actual)
	})
}
