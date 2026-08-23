package zenbpm

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	t.Run("embeds the root version file", func(t *testing.T) {
		contents, err := os.ReadFile("VERSION")
		require.NoError(t, err)

		actual := Version()
		assert.Equal(t, strings.TrimSpace(string(contents)), actual)
		assert.Regexp(t, `^v[0-9]+\.[0-9]+\.[0-9]+$`, actual)
	})
}
