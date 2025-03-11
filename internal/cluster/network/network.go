package network

import (
	"net"

	"github.com/rqlite/rqlite/v8/tcp"
)

type muxHeader byte

const (
	_ muxHeader = iota
	// ZenBpmMuxRaftHeader is the byte used to indicate ZenBPM internode Raft communication
	muxHeaderZenBpmRaft

	// ZenBpmMuxClusterHeader is the byte used to indicate ZenBPM internode Raft communication
	muxheaderZenBpmCluster // ZenBPM Cluster state communications

	// RqLiteMuxRaftHeader is the byte used to indicate internode Raft communications.
	muxHeaderRqLiteRaft

	// RqLiteMuxClusterHeader is the byte used to request internode cluster state information.
	muxHeaderRqLiteCluster // RqLite Cluster state communications
)

func NewZenBpmRaftListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderZenBpmRaft))
}

func NewZenBpmClusterListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxheaderZenBpmCluster))
}

func NewRqLiteRaftListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderRqLiteRaft))
}

func NewRqLiteClusterListener(mux *tcp.Mux) net.Listener {
	return mux.Listen(byte(muxHeaderRqLiteCluster))
}

func NewZenBpmRaftDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxHeaderZenBpmRaft), nil)
}
func NewZenBpmClusterDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxheaderZenBpmCluster), nil)
}
func NewRqLiteRaftDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxHeaderRqLiteRaft), nil)
}
func NewRqLiteClusterDialer() *tcp.Dialer {
	return tcp.NewDialer(byte(muxHeaderRqLiteCluster), nil)
}
