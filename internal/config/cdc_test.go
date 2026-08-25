package config

import (
	"os"
	"path/filepath"
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

	t.Run("leaves the service ID empty by default", func(t *testing.T) {
		unsetEnv(t, "RQLITE_CDC_ENABLED")
		unsetEnv(t, "RQLITE_CDC_SERVICE_ID")

		var cfg Config
		require.NoError(t, cleanenv.ReadEnv(&cfg))
		require.False(t, cfg.Cluster.CDC.Enabled)
		require.Empty(t, cfg.Cluster.CDC.ServiceID)
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

	t.Run("rejects a direct URL without a service ID", func(t *testing.T) {
		err := (Cluster{CDC: CDC{
			Enabled: true,
			Output:  "https://example.com/cdc",
		}}).ValidateCDC()

		require.EqualError(t, err, "CDC service ID is required when CDC is enabled")
	})

	t.Run("rejects stdout without a service ID", func(t *testing.T) {
		err := (Cluster{CDC: CDC{
			Enabled: true,
			Output:  "stdout",
		}}).ValidateCDC()

		require.EqualError(t, err, "CDC service ID is required when CDC is enabled")
	})

	t.Run("rejects a whitespace-only configured service ID", func(t *testing.T) {
		err := (Cluster{CDC: CDC{
			Enabled:   true,
			Output:    "https://example.com/cdc",
			ServiceID: " \t ",
		}}).ValidateCDC()

		require.EqualError(t, err, "CDC service ID is required when CDC is enabled")
	})

	t.Run("accepts a service ID from an advanced output file", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		require.NoError(t, os.WriteFile(outputPath, []byte(`{
  "endpoint": "https://example.com/cdc",
  "service_id": "advanced-source"
}`), 0o600))

		err := (Cluster{CDC: CDC{
			Enabled: true,
			Output:  outputPath,
		}}).ValidateCDC()

		require.NoError(t, err)
	})

	t.Run("accepts the configured service ID when an advanced output file omits it", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		require.NoError(t, os.WriteFile(outputPath, []byte(`{
  "endpoint": "https://example.com/cdc"
}`), 0o600))

		err := (Cluster{CDC: CDC{
			Enabled:   true,
			Output:    outputPath,
			ServiceID: "configured-source",
		}}).ValidateCDC()

		require.NoError(t, err)
	})

	t.Run("advanced service ID takes precedence over the configured service ID", func(t *testing.T) {
		serviceID, err := (CDC{ServiceID: "configured-source"}).ResolveServiceID("advanced-source")

		require.NoError(t, err)
		require.Equal(t, "advanced-source", serviceID)
	})

	t.Run("uses the configured service ID when the advanced output omits it", func(t *testing.T) {
		serviceID, err := (CDC{ServiceID: "configured-source"}).ResolveServiceID("")

		require.NoError(t, err)
		require.Equal(t, "configured-source", serviceID)
	})

	t.Run("rejects a whitespace-only advanced service ID instead of using the configured fallback", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		require.NoError(t, os.WriteFile(outputPath, []byte(`{
  "endpoint": "https://example.com/cdc",
  "service_id": "   "
}`), 0o600))

		err := (Cluster{CDC: CDC{
			Enabled:   true,
			Output:    outputPath,
			ServiceID: "configured-source",
		}}).ValidateCDC()

		require.EqualError(t, err, "CDC service ID is required when CDC is enabled")
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
