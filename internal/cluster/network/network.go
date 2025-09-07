// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package network

import (
	"crypto/tls"
	"fmt"
	"net"

	httpd "github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/tcp"
)

// muxHeader specifies the header byte for server communication
// bytes 1-10 are reserved. Bytes 10 - 255 are used by RqLite for partition communication
//
//	 partition bytes are assigned as:
//		1 - 11 - raft
//		  - 12 - cluster
//		2 - 13 - raft
//		  - 14 - cluster
//		3 - 15 - raft
//		  - 16 - cluster
type muxHeader byte

const (
	_ muxHeader = iota
	// ZenBpmMuxRaftHeader is the byte used to indicate ZenBPM internode Raft communication
	muxHeaderZenBpmRaft

	// ZenBpmMuxClusterHeader is the byte used to indicate ZenBPM internode Raft communication
	muxHeaderZenBpmCluster // ZenBPM Cluster state communications

	reservedBytes = 10
)

func NewZenBpmRaftListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderZenBpmRaft))
}

func NewZenBpmClusterListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderZenBpmCluster))
}

func NewZenBpmRaftDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxHeaderZenBpmRaft), nil)
}

func NewZenBpmClusterDialer() *Dialer {
	return &Dialer{header: byte(muxHeaderZenBpmCluster)}
}

func NewRqLiteRaftListener(partition uint32, mux *tcp.Mux) net.Listener {
	return mux.Listen(GetPartitionRaftHeaderByte(partition))
}

func NewRqLiteClusterListener(partition uint32, mux *tcp.Mux) net.Listener {
	return mux.Listen(GetPartitionClusterHeaderByte(partition))
}

func NewRqLiteRaftDialer(partition uint32, cert, key, caCert, serverName string, Insecure bool) (*tcp.Dialer, error) {
	var dialerTLSConfig *tls.Config
	var err error
	if cert != "" || key != "" {
		dialerTLSConfig, err = rtls.CreateClientConfig(cert, key, caCert, serverName, Insecure)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config for Raft dialer: %s", err.Error())
		}
	}
	return tcp.NewDialer(GetPartitionRaftHeaderByte(partition), dialerTLSConfig), nil
}

func NewRqLiteClusterDialer(partition uint32, cert, key, caCert, serverName string, Insecure bool) (*tcp.Dialer, error) {
	var dialerTLSConfig *tls.Config
	var err error
	if cert != "" || key != "" {
		dialerTLSConfig, err = rtls.CreateClientConfig(cert, key, caCert, serverName, Insecure)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config for Cluster dialer: %s", err.Error())
		}
	}
	return tcp.NewDialer(GetPartitionClusterHeaderByte(partition), dialerTLSConfig), nil
}

func GetPartitionRaftHeaderByte(partition uint32) byte {
	// cap the max partition number at 122 to prevent byte overflow
	// TODO: this is bad solution and we should handle this in the controller
	if partition > 122 {
		panic("maximum number of partition is 122")
	}
	headerByte := reservedBytes + partition*2 - 1
	return byte(headerByte)
}

func GetPartitionClusterHeaderByte(partition uint32) byte {
	// cap the max partition number at 122 to prevent byte overflow
	if partition > 122 {
		panic("maximum number of partition is 122")
	}
	headerByte := reservedBytes + partition*2
	return byte(headerByte)
}

func CheckJoinAddrs(joinAddrs []string) error {
	if len(joinAddrs) > 0 {
		if addr, ok := httpd.AnyServingHTTP(joinAddrs); ok {
			return fmt.Errorf("join address %s appears to be serving HTTP when it should be Raft", addr)
		}
	}
	return nil
}
