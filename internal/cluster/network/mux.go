package network

import (
	"fmt"
	"net"

	"github.com/rqlite/rqlite/v8/tcp"
)

// NewMux creates a new instance of TCP multiplexer.
// Multiplexer can route its connections based on a header byte.
func NewMux(address string) (*tcp.Mux, error) {
	// Create internode network mux and configure it.
	muxLn, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %s", address, err.Error())
	}
	mux, err := startNodeMux(address, muxLn)
	if err != nil {
		return nil, fmt.Errorf("failed to start node mux: %s", err.Error())
	}
	return mux, nil
}

// startNodeMux starts the TCP mux on the given listener, which should be already
// bound to the relevant interface.
func startNodeMux(address string, ln net.Listener) (*tcp.Mux, error) {
	var err error
	var adv net.Addr
	if address != "" {
		adv = tcp.NameAddress{
			Address: address,
		}
	}

	var mux *tcp.Mux
	mux, err = tcp.NewMux(ln, adv)
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	go func() {
		if err := mux.Serve(); err != nil {
			panic(err)
		}
	}()
	return mux, nil
}
