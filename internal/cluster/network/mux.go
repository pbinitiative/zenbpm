package network

import (
	"errors"
	"fmt"
	"net"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/rqlite/rqlite/v10/tcp"
)

// MuxOption configures a node mux created by NewNodeMux.
type MuxOption func(*muxOptions)

type muxOptions struct {
	advertise string
}

// WithAdvertise sets the address the mux advertises to peers. Required in
// environments where the bind address (e.g. ":8090") is not reachable from
// other nodes and peers must dial a routable FQDN instead. An empty string is
// treated as unset and the bind address is used.
func WithAdvertise(adv string) MuxOption {
	return func(o *muxOptions) { o.advertise = adv }
}

// NewNodeMux creates a new instance of TCP multiplexer.
// Multiplexer can route its connections based on a header byte.
// Providing empty string as bindAddress will start on a random free port.
func NewNodeMux(bindAddress string, opts ...MuxOption) (*tcp.Mux, net.Listener, error) {
	o := muxOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	muxLn, err := net.Listen("tcp4", bindAddress)
	if err != nil {
		return nil, muxLn, fmt.Errorf("failed to listen on %s: %s", bindAddress, err.Error())
	}
	mux, err := startNodeMux(o.advertise, muxLn)
	if err != nil {
		return nil, muxLn, fmt.Errorf("failed to start node mux: %s", err.Error())
	}
	return mux, muxLn, nil
}

// startNodeMux starts the TCP mux on the given listener, which should be already
// bound to the relevant interface. If advertise is empty, the mux advertises
// the listener's bind address.
func startNodeMux(advertise string, ln net.Listener) (*tcp.Mux, error) {
	var adv net.Addr
	if advertise != "" {
		adv = tcp.NameAddress{Address: advertise}
	}

	mux, err := tcp.NewMux(ln, adv)
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	safego.Go("node-mux", safego.DefaultLogger, func() {
		if err := mux.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			safego.DefaultLogger.Error("node mux stopped unexpectedly", "err", err)
		}
	})
	return mux, nil
}
