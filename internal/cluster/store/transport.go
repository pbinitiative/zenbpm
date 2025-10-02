package store

import (
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/tcp"
)

// Transport is the network service provided to Raft, and wraps a Listener.
type Transport struct {
	ly *tcp.Layer
}

// NewTransport returns an initialized Transport.
func NewTransport(ly *tcp.Layer) *Transport {
	return &Transport{
		ly: ly,
	}
}

// Dial creates a new network connection.
func (t *Transport) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return t.ly.Dial(string(addr), timeout)
}

// Accept waits for the next connection.
func (t *Transport) Accept() (net.Conn, error) {
	return t.ly.Accept()
}

// Close closes the transport
func (t *Transport) Close() error {
	return t.ly.Close()
}

// Addr returns the binding address of the transport.
func (t *Transport) Addr() net.Addr {
	return t.ly.Addr()
}
