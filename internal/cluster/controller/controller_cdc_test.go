package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerCDCOutput(t *testing.T) {
	t.Run("applies public cdc settings when enabled", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled:   true,
				Output:    "https://example.com/cdc",
				ServiceID: "configured-source",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		require.NotNil(t, controller.persistenceConfig.RqLite)
		assert.Equal(t, config.CDC{
			Enabled:   true,
			Output:    "https://example.com/cdc",
			ServiceID: "configured-source",
		}, controller.cdcConfig)
	})

	t.Run("accepts an advanced cdc output file", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		cdcOutputPath := writeCDCControllerOutput(t, `{"endpoint":"https://example.com/cdc","service_id":"advanced-source","max_batch_size":25}`)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled: true,
				Output:  cdcOutputPath,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		assert.Equal(t, config.CDC{
			Enabled: true,
			Output:  cdcOutputPath,
		}, controller.cdcConfig)
	})

	t.Run("clears cdc output when disabled", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		rqLiteConfig := newCDCControllerRqLiteConfig(t, tStore)
		rqLiteConfig.RaftNonVoter = true
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Output:    "https://example.com/cdc",
				ServiceID: "configured-source",
			},
			Persistence: config.Persistence{
				RqLite: rqLiteConfig,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		assert.Empty(t, controller.cdcConfig)
	})

	t.Run("rejects enabled cdc on a non-voting node", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		rqLiteConfig := newCDCControllerRqLiteConfig(t, tStore)
		rqLiteConfig.RaftNonVoter = true
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled:   true,
				Output:    "https://example.com/cdc",
				ServiceID: "configured-source",
			},
			Persistence: config.Persistence{
				RqLite: rqLiteConfig,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.EqualError(t, err, "failed to start controller, rqLite config validation failed: CDC cannot be enabled on non-voting nodes")
		assert.False(t, controller.handleClusterChanges)
	})

	t.Run("rejects enabled cdc without output", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{Enabled: true},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.EqualError(t, err, "failed to start controller, CDC output validation failed: CDC output is required when CDC is enabled")
		assert.False(t, controller.handleClusterChanges)
	})

	t.Run("rejects a missing advanced cdc output file", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		cdcOutputPath := filepath.Join(t.TempDir(), "missing-cdc-output.json")
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled: true,
				Output:  cdcOutputPath,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.ErrorContains(t, err, "failed to start controller, CDC output validation failed: failed to load CDC output:")
		require.ErrorContains(t, err, "missing-cdc-output.json")
		assert.False(t, controller.handleClusterChanges)
	})

	t.Run("rejects malformed advanced cdc output", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		cdcOutputPath := writeCDCControllerOutput(t, `{"endpoint":`)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled: true,
				Output:  cdcOutputPath,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.ErrorContains(t, err, "failed to start controller, CDC output validation failed: failed to load CDC output:")
		require.ErrorContains(t, err, "unexpected end of JSON input")
		assert.False(t, controller.handleClusterChanges)
	})

	t.Run("rejects an unsupported cdc endpoint", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		cdcOutputPath := writeCDCControllerOutput(t, `{"endpoint":"ftp://example.com/cdc"}`)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled: true,
				Output:  cdcOutputPath,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.EqualError(t, err, `failed to start controller, CDC output validation failed: failed to validate CDC output endpoint: cdc: unsupported scheme "ftp"`)
		assert.False(t, controller.handleClusterChanges)
	})

	t.Run("rejects invalid cdc output tls settings", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		missingCAPath := filepath.Join(t.TempDir(), "missing-ca.pem")
		cdcOutputPath := writeCDCControllerOutput(t, `{"endpoint":"https://example.com/cdc","tls":{"ca_cert_file":`+fmt.Sprintf("%q", missingCAPath)+`}}`)
		controller, err := NewController(nil, config.Cluster{
			CDC: config.CDC{
				Enabled: true,
				Output:  cdcOutputPath,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.ErrorContains(t, err, "failed to start controller, CDC output validation failed: failed to build CDC output TLS settings:")
		require.ErrorContains(t, err, "missing-ca.pem")
		assert.False(t, controller.handleClusterChanges)
	})
}

func newCDCControllerTestStore() *ControllerTestStore {
	return &ControllerTestStore{
		id:   "test-node-1",
		addr: "localhost:4002",
		clusterState: state.Cluster{
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
}

func newCDCControllerRqLiteConfig(t *testing.T, tStore *ControllerTestStore) *config.RqLite {
	t.Helper()
	return &config.RqLite{
		DataPath: t.TempDir(),
		RaftAddr: tStore.addr,
		RaftAdv:  tStore.addr,
	}
}

func writeCDCControllerOutput(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cdc-output.json")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}
