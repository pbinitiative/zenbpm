//go:build cluster_e2e

package cluster

import (
	"os"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"
)

// TestMain is the entry point for the cluster e2e test package.
// Unlike the regular e2e suite which starts a single shared node,
// each test function in this package creates its own TestCluster
// with the topology it needs (1-node, 3-node, 5-node, etc.).
func TestMain(m *testing.M) {
	log.Init()

	// Initialize OTel metrics (required by REST middleware).
	// Tracing is disabled, but the meter provider and counters must exist.
	openTelemetry, err := otel.SetupOtel(config.Tracing{Enabled: false, Name: "cluster-e2e-test"})
	if err != nil {
		log.Error("Failed to set up OTel: %s", err)
		os.Exit(1)
	}
	defer openTelemetry.Stop(nil)

	os.Exit(m.Run())
}

// isSlow returns true if the current test run is in -short mode.
// Slow-tier tests should call skipIfShort(t) at the top.
func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping slow cluster e2e test in short mode")
	}
}
