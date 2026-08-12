package network

import (
	"net"
)

type Dialer struct {
	header byte
}

// DialGRPC creates a connection without write deadline set
func (d *Dialer) DialGRPC(addr string) (conn net.Conn, retErr error) {
	dialer := &net.Dialer{}

	conn, retErr = dialer.Dial("tcp", addr)
	if retErr != nil {
		return nil, retErr
	}

	defer func(connToClose net.Conn) {
		if retErr != nil && connToClose != nil {
			// best-effort cleanup on the error path; the dial/write error is returned to the caller
			_ = connToClose.Close()
		}
	}(conn)

	// Write a marker byte to indicate message type.
	if _, err := conn.Write([]byte{d.header}); err != nil {
		return nil, err
	}
	return conn, nil
}
