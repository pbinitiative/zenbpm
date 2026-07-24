package network

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClosableDialerCloseClosesTrackedConnections(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	dialer := newClosableDialer(staticDialer{conn: client})

	conn, err := dialer.Dial("unused", time.Second)
	require.NoError(t, err)
	require.Len(t, dialer.conns, 1, "expected the connection to be tracked")

	require.NoError(t, dialer.Close())
	assert.Empty(t, dialer.conns, "expected all connections to be untracked")

	_, err = conn.Write([]byte("closed"))
	assert.Error(t, err, "expected the tracked connection to be closed")
	assert.NoError(t, dialer.Close(), "expected Close to be idempotent")
}

func TestClosableDialerRejectsConnectionsAfterClose(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	dialer := newClosableDialer(staticDialer{conn: client})
	require.NoError(t, dialer.Close())

	conn, err := dialer.Dial("unused", time.Second)
	assert.Nil(t, conn, "expected no connection from a closed dialer")
	assert.ErrorIs(t, err, net.ErrClosed)

	_, err = client.Write([]byte("closed"))
	assert.Error(t, err, "expected the newly-created connection to be closed")
}

type staticDialer struct {
	conn net.Conn
}

func (d staticDialer) Dial(string, time.Duration) (net.Conn, error) {
	return d.conn, nil
}
