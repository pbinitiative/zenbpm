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

	defer func() {
		if retErr != nil && conn != nil {
			conn.Close()
		}
	}()

	// Write a marker byte to indicate message type.
	if _, err := conn.Write([]byte{d.header}); err != nil {
		return nil, err
	}
	return conn, nil
}
