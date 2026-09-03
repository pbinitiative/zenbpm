package network

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNodeMux_DefaultAdvertiseUsesBindAddr(t *testing.T) {
	mux, ln, err := NewNodeMux("127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Register a dummy listener to extract mux's advertised addr.
	l := mux.Listen(1)
	assert.Equal(t, ln.Addr().String(), l.Addr().String(),
		"without WithAdvertise the mux should advertise the bind addr")
}

func TestNewNodeMux_WithAdvertiseOverridesBindAddr(t *testing.T) {
	const adv = "zenbpm-0.zenbpm-headless.default.svc.cluster.local:8090"
	mux, ln, err := NewNodeMux("127.0.0.1:0", WithAdvertise(adv))
	require.NoError(t, err)
	defer ln.Close()

	l := mux.Listen(1)
	assert.Equal(t, adv, l.Addr().String(),
		"WithAdvertise must set the mux's advertised address regardless of bind")
}

func TestNewNodeMux_EmptyAdvertiseFallsBackToBind(t *testing.T) {
	mux, ln, err := NewNodeMux("127.0.0.1:0", WithAdvertise(""))
	require.NoError(t, err)
	defer ln.Close()

	l := mux.Listen(1)
	assert.Equal(t, ln.Addr().String(), l.Addr().String(),
		"empty WithAdvertise should be treated as unset")
}
