// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
