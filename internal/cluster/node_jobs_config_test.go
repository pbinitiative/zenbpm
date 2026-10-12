package cluster

import (
	"net"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRefusedJobsConfigurationLeavesTheAddressFree shows a node which refuses
// its jobs configuration binds nothing: an embedding application which
// corrects the configuration can start on the same address without a restart.
func TestRefusedJobsConfigurationLeavesTheAddressFree(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	addr := probe.Addr().String()
	require.NoError(t, probe.Close())
	conf := config.Defaults()
	conf.Cluster.NodeId = "refused-node"
	conf.Cluster.Addr = addr
	conf.Jobs.DefaultRetries = 7
	conf.Jobs.MaxRetries = 5

	node, err := StartZenNode(t.Context(), conf)

	require.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "jobs.defaultRetries")
	rebound, err := net.Listen("tcp4", addr)
	require.NoError(t, err, "the refused start must not keep the address bound")
	require.NoError(t, rebound.Close())
}
