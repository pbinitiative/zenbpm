package rest

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessInstanceVersionTagIsOptional(t *testing.T) {
	t.Run("omits empty version tag", func(t *testing.T) {
		body, err := json.Marshal(public.ProcessInstancesSimple{VersionTag: optionalString("")})
		require.NoError(t, err)
		assert.NotContains(t, string(body), `"versionTag"`)
	})

	t.Run("includes configured version tag", func(t *testing.T) {
		body, err := json.Marshal(public.ProcessInstancesSimple{VersionTag: optionalString("v1")})
		require.NoError(t, err)
		assert.Contains(t, string(body), `"versionTag":"v1"`)
	})
}
