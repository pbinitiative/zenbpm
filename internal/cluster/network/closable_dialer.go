// Package network provides the networking primitives used for cluster communication.
package network

import (
	"errors"
	"net"
	"sync"
	"time"
)

type connectionDialer interface {
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// ClosableDialer tracks every connection created by a dialer so an embedded
// RQLite client can be shut down even though it does not expose a Close method.
type ClosableDialer struct {
	dialer connectionDialer

	mu     sync.Mutex
	conns  map[*trackedConn]struct{}
	closed bool
}

func newClosableDialer(dialer connectionDialer) *ClosableDialer {
	return &ClosableDialer{
		dialer: dialer,
		conns:  make(map[*trackedConn]struct{}),
	}
}

// Dial opens and tracks a connection so it can be closed with the dialer.
func (d *ClosableDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := d.dialer.Dial(address, timeout)
	if err != nil {
		return nil, err
	}

	tracked := &trackedConn{Conn: conn}
	tracked.onClose = func() {
		d.mu.Lock()
		delete(d.conns, tracked)
		d.mu.Unlock()
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		_ = tracked.Close()
		return nil, net.ErrClosed
	}
	d.conns[tracked] = struct{}{}
	d.mu.Unlock()

	return tracked, nil
}

// Close closes all tracked connections and prevents new connections from being opened.
func (d *ClosableDialer) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	connections := make([]*trackedConn, 0, len(d.conns))
	for conn := range d.conns {
		connections = append(connections, conn)
	}
	d.mu.Unlock()

	var closeErr error
	for _, conn := range connections {
		closeErr = errors.Join(closeErr, conn.Close())
	}
	return closeErr
}

type trackedConn struct {
	net.Conn

	closeOnce sync.Once
	closeErr  error
	onClose   func()
}

func (c *trackedConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.closeErr
}
