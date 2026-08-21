package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRqLiteCDCValidation(t *testing.T) {
	t.Run("accepts disabled CDC", func(t *testing.T) {
		cfg := newValidRqLiteCDCConfig(t)

		require.NoError(t, cfg.Validate())
	})

	t.Run("accepts CDC on a voting node", func(t *testing.T) {
		cfg := newValidRqLiteCDCConfig(t)
		cfg.CDCConfig = "stdout"

		require.NoError(t, cfg.Validate())
	})

	t.Run("rejects CDC on a non-voting node", func(t *testing.T) {
		cfg := newValidRqLiteCDCConfig(t)
		cfg.RaftNonVoter = true
		cfg.CDCConfig = "stdout"

		err := cfg.Validate()

		require.EqualError(t, err, "CDC cannot be enabled on non-voting nodes")
	})
}

func newValidRqLiteCDCConfig(t *testing.T) RqLite {
	t.Helper()
	return RqLite{
		DataPath: t.TempDir(),
		RaftAddr: "localhost:4002",
		RaftAdv:  "localhost:4002",
	}
}
