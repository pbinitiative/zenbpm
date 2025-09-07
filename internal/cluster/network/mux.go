// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package network

import (
	"fmt"
	"net"

	"github.com/rqlite/rqlite/v8/tcp"
)

// NewNodeMux creates a new instance of TCP multiplexer.
// Multiplexer can route its connections based on a header byte.
// Providing empty string to the address will start on a random free port.
func NewNodeMux(address string) (*tcp.Mux, net.Listener, error) {
	// Create internode network mux and configure it.
	muxLn, err := net.Listen("tcp4", address)
	if err != nil {
		return nil, muxLn, fmt.Errorf("failed to listen on %s: %s", address, err.Error())
	}
	mux, err := startNodeMux(address, muxLn)
	if err != nil {
		return nil, muxLn, fmt.Errorf("failed to start node mux: %s", err.Error())
	}
	return mux, muxLn, nil
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
			// TODO: make sure the mux is closed correctly
			_ = err
		}
	}()
	return mux, nil
}
