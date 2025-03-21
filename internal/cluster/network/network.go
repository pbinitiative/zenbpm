package network

import (
	"crypto/tls"
	"fmt"
	"net"

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
	muxheaderZenBpmCluster // ZenBPM Cluster state communications

	reservedBytes = 10
)

func NewZenBpmRaftListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderZenBpmRaft))
}

func NewZenBpmClusterListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxheaderZenBpmCluster))
}

func NewZenBpmRaftDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxHeaderZenBpmRaft), nil)
}

func NewZenBpmClusterDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxheaderZenBpmCluster), nil)
}

func NewRqLiteRaftListener(partition uint32, mux *tcp.Mux) net.Listener {
	return mux.Listen(getPartitionRaftHeaderByte(partition))
}

func NewRqLiteClusterListener(partition uint32, mux *tcp.Mux) net.Listener {
	return mux.Listen(getPartitionClusterHeaderByte(partition))
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
	return tcp.NewDialer(getPartitionRaftHeaderByte(partition), dialerTLSConfig), nil
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
	return tcp.NewDialer(getPartitionClusterHeaderByte(partition), dialerTLSConfig), nil
}

func getPartitionRaftHeaderByte(partition uint32) byte {
	// cap the max partition number at 122 to prevent byte overflow
	// TODO: this is bad solution and we should handle this in the controller
	if partition > 122 {
		panic("maximum number of partition is 122")
	}
	headerByte := reservedBytes + partition*2 - 1
	return byte(headerByte)
}
func getPartitionClusterHeaderByte(partition uint32) byte {
	// cap the max partition number at 122 to prevent byte overflow
	// TODO: this is bad solution and we should handle this in the controller
	if partition > 122 {
		panic("maximum number of partition is 122")
	}
	headerByte := reservedBytes + partition*2
	return byte(headerByte)
}
