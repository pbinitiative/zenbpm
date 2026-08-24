package config

import (
	"os"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCDCOutput(t *testing.T) {
	t.Run("loads enabled cdc settings from yaml", func(t *testing.T) {
		var cfg Config
		err := yaml.Unmarshal([]byte(`
cluster:
  cdc:
    enabled: true
    output: https://example.com/cdc
    serviceId: source-a
`), &cfg)

		require.NoError(t, err)
		require.True(t, cfg.Cluster.CDC.Enabled)
		require.Equal(t, "https://example.com/cdc", cfg.Cluster.CDC.Output)
		require.Equal(t, "source-a", cfg.Cluster.CDC.ServiceID)
	})

	t.Run("loads enabled cdc settings from environment", func(t *testing.T) {
		t.Setenv("RQLITE_CDC_ENABLED", "true")
		t.Setenv("RQLITE_CDC_OUTPUT", "https://example.com/cdc")
		t.Setenv("RQLITE_CDC_SERVICE_ID", "source-b")

		var cfg Config
		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.True(t, cfg.Cluster.CDC.Enabled)
		require.Equal(t, "https://example.com/cdc", cfg.Cluster.CDC.Output)
		require.Equal(t, "source-b", cfg.Cluster.CDC.ServiceID)
	})

	t.Run("applies cdc defaults", func(t *testing.T) {
		unsetEnv(t, "RQLITE_CDC_ENABLED")
		unsetEnv(t, "RQLITE_CDC_SERVICE_ID")

		var cfg Config
		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.False(t, cfg.Cluster.CDC.Enabled)
		require.Equal(t, "zenbpm", cfg.Cluster.CDC.ServiceID)
	})

	t.Run("environment can disable yaml cdc", func(t *testing.T) {
		t.Setenv("RQLITE_CDC_ENABLED", "false")
		var cfg Config
		require.NoError(t, yaml.Unmarshal([]byte(`
cluster:
  cdc:
    enabled: true
    output: https://example.com/cdc
`), &cfg))

		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.False(t, cfg.Cluster.CDC.Enabled)
	})

	t.Run("rejects enabled cdc without output", func(t *testing.T) {
		err := (Cluster{CDC: CDC{Enabled: true}}).ValidateCDC()

		require.EqualError(t, err, "CDC output is required when CDC is enabled")
	})
}

func unsetEnv(t *testing.T, name string) {
	t.Helper()
	value, wasSet := os.LookupEnv(name)
	require.NoError(t, os.Unsetenv(name))
	t.Cleanup(func() {
		if wasSet {
			require.NoError(t, os.Setenv(name, value))
			return
		}
		require.NoError(t, os.Unsetenv(name))
	})
}
