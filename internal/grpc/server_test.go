package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeVariables(t *testing.T) {
	t.Run("nil payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables(nil)
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("empty payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables([]byte{})
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("null payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables([]byte("null"))
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("object payload returns map", func(t *testing.T) {
		vars, err := decodeVariables([]byte(`{"testVar":123}`))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"testVar": float64(123)}, vars)
	})

	t.Run("invalid payload returns error", func(t *testing.T) {
		vars, err := decodeVariables([]byte("{"))
		require.Error(t, err)
		assert.Nil(t, vars)
	})
}
