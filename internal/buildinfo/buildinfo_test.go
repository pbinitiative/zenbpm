package buildinfo

import (
	"runtime/debug"
	"testing"

	"github.com/pbinitiative/zenbpm"
	"github.com/stretchr/testify/assert"
)

func TestBuildInfo(t *testing.T) {
	t.Run("composes metadata injected by the build pipeline", func(t *testing.T) {
		settings := []debug.BuildSetting{{Key: "vcs.revision", Value: "019fcc9239db744b9bd0c0544f71af3a"}}

		actual := resolveInfo(
			"injected-commit",
			"main",
			"2026-08-07T12:13:14Z",
			settings,
		)

		assert.Equal(t, Info{
			Version:   zenbpm.Version(),
			Commit:    "injected-commit",
			Branch:    "main",
			BuildTime: "2026-08-07T12:13:14Z",
		}, actual)
	})

	t.Run("uses the Go VCS revision without inventing other metadata", func(t *testing.T) {
		settings := []debug.BuildSetting{{Key: "vcs.revision", Value: "019fcc9239db744b9bd0c0544f71af3a"}}

		actual := resolveInfo(unknown, unknown, unknown, settings)

		assert.Equal(t, Info{
			Version:   zenbpm.Version(),
			Commit:    "019fcc9239db744b9bd0c0544f71af3a",
			Branch:    unknown,
			BuildTime: unknown,
		}, actual)
	})

	t.Run("normalizes missing build metadata to unknown", func(t *testing.T) {
		actual := resolveInfo("", "", "", []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}})

		assert.Equal(t, Info{
			Version:   zenbpm.Version(),
			Commit:    unknown,
			Branch:    unknown,
			BuildTime: unknown,
		}, actual)
	})
}
