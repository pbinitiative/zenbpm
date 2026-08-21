package config

import (
	"os"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCDCConfiguration(t *testing.T) {
	t.Run("loads enabled cdc from yaml", func(t *testing.T) {
		var cfg Config
		err := yaml.Unmarshal([]byte(`
cluster:
  persistence:
    cdcEnabled: true
    cdc: /etc/zenbpm/cdc.json
`), &cfg)

		require.NoError(t, err)
		require.True(t, cfg.Cluster.Persistence.CDCEnabled)
		require.Equal(t, "/etc/zenbpm/cdc.json", cfg.Cluster.Persistence.CDC)
	})

	t.Run("loads enabled cdc from environment", func(t *testing.T) {
		t.Setenv("RQLITE_CDC_ENABLED", "true")
		t.Setenv("RQLITE_CDC_CONFIG", "https://example.com/cdc")

		var cfg Config
		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.True(t, cfg.Cluster.Persistence.CDCEnabled)
		require.Equal(t, "https://example.com/cdc", cfg.Cluster.Persistence.CDC)
	})

	t.Run("disables cdc by default", func(t *testing.T) {
		unsetCDCEnabledEnv(t)

		var cfg Config
		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.False(t, cfg.Cluster.Persistence.CDCEnabled)
	})

	t.Run("environment can disable yaml cdc", func(t *testing.T) {
		t.Setenv("RQLITE_CDC_ENABLED", "false")
		var cfg Config
		require.NoError(t, yaml.Unmarshal([]byte(`
cluster:
  persistence:
    cdcEnabled: true
    cdc: /etc/zenbpm/cdc.json
`), &cfg))

		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.False(t, cfg.Cluster.Persistence.CDCEnabled)
	})

	t.Run("rejects enabled cdc without configuration", func(t *testing.T) {
		err := (Persistence{CDCEnabled: true}).Validate()

		require.EqualError(t, err, "CDC configuration is required when CDC is enabled")
	})
}

func unsetCDCEnabledEnv(t *testing.T) {
	t.Helper()
	value, wasSet := os.LookupEnv("RQLITE_CDC_ENABLED")
	require.NoError(t, os.Unsetenv("RQLITE_CDC_ENABLED"))
	t.Cleanup(func() {
		if wasSet {
			require.NoError(t, os.Setenv("RQLITE_CDC_ENABLED", value))
			return
		}
		require.NoError(t, os.Unsetenv("RQLITE_CDC_ENABLED"))
	})
}
