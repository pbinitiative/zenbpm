package controller

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerCDCConfiguration(t *testing.T) {
	t.Run("applies public cdc config when enabled", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		controller, err := NewController(nil, config.Cluster{
			Persistence: config.Persistence{
				CDCEnabled: true,
				CDC:        "stdout",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		require.NotNil(t, controller.persistenceConfig.RqLite)
		assert.Equal(t, "stdout", controller.persistenceConfig.RqLite.CDCConfig)
	})

	t.Run("preserves nested cdc config when enabled", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		rqLiteConfig := newCDCControllerRqLiteConfig(t, tStore)
		rqLiteConfig.CDCConfig = "stdout"
		controller, err := NewController(nil, config.Cluster{
			Persistence: config.Persistence{
				CDCEnabled: true,
				RqLite:     rqLiteConfig,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		assert.Equal(t, "stdout", controller.persistenceConfig.RqLite.CDCConfig)
	})

	t.Run("public cdc config overrides nested config", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		rqLiteConfig := newCDCControllerRqLiteConfig(t, tStore)
		rqLiteConfig.CDCConfig = "stdout"
		controller, err := NewController(nil, config.Cluster{
			Persistence: config.Persistence{
				CDCEnabled: true,
				CDC:        "https://example.com/cdc",
				RqLite:     rqLiteConfig,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		assert.Equal(t, "https://example.com/cdc", controller.persistenceConfig.RqLite.CDCConfig)
	})

	t.Run("clears all cdc config when disabled", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		rqLiteConfig := newCDCControllerRqLiteConfig(t, tStore)
		rqLiteConfig.CDCConfig = "stdout"
		rqLiteConfig.RaftNonVoter = true
		controller, err := NewController(nil, config.Cluster{
			Persistence: config.Persistence{
				CDC:    "https://example.com/cdc",
				RqLite: rqLiteConfig,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		require.NoError(t, controller.Start(tStore, clientMgr))
		assert.Empty(t, controller.persistenceConfig.RqLite.CDCConfig)
	})

	t.Run("rejects enabled cdc without config", func(t *testing.T) {
		tStore := newCDCControllerTestStore()
		clientMgr := client.NewClientManager(tStore)
		controller, err := NewController(nil, config.Cluster{
			Persistence: config.Persistence{
				CDCEnabled: true,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, controller.Stop())
		})

		err = controller.Start(tStore, clientMgr)
		require.EqualError(t, err, "failed to start controller, persistence config validation failed: CDC configuration is required when CDC is enabled")
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
