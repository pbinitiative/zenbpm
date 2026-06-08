package grpc

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
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

func TestUnknownRequestError(t *testing.T) {
	t.Run("unknown subscription type returns error response, no panic", func(t *testing.T) {
		req := &proto.JobStreamRequest{
			Request: &proto.JobStreamRequest_Subscription{
				Subscription: &proto.StreamSubscriptionRequest{
					Type: ptr.To(proto.StreamSubscriptionRequest_Type(9999)),
				},
			},
		}
		var resp *proto.JobStreamResponse
		require.NotPanics(t, func() {
			resp = unknownRequestError(req.Request)
		})
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.NotNil(t, resp.Error.Message)
		assert.Contains(t, *resp.Error.Message, "unexpected")
	})

	t.Run("unknown top-level request type returns error response, no panic", func(t *testing.T) {
		var resp *proto.JobStreamResponse
		require.NotPanics(t, func() {
			resp = unknownRequestError(nil)
		})
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.NotNil(t, resp.Error.Message)
		assert.Contains(t, *resp.Error.Message, "unexpected")
	})
}
