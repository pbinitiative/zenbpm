package servertest

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerLifecycle(t *testing.T) {
	t.Run("close releases the underlying mux listener", func(t *testing.T) {
		server := NewTestServer()
		addr := server.muxListener.Addr().String()

		require.NoError(t, server.Close())

		listener, err := net.Listen("tcp4", addr)
		require.NoError(t, err)
		require.NoError(t, listener.Close())
	})

	t.Run("close is idempotent", func(t *testing.T) {
		server := NewTestServer()

		require.NoError(t, server.Close())
		require.NoError(t, server.Close())
	})
}
