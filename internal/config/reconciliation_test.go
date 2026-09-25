package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

func TestReconciliationConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var cfg Config
		if err := cleanenv.ReadEnv(&cfg); err != nil {
			t.Fatal(err)
		}
		e := cfg.Cluster.Engine
		if e.ReconciliationScanDisabled || e.ReconciliationIntervalSeconds != 60 || e.ReconciliationGracePeriodSeconds != 60 || e.ReconciliationBatchSize != 256 {
			t.Fatalf("unexpected recovery defaults: %+v", e)
		}
		if err := e.ValidateReconciliation(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("environment", func(t *testing.T) {
		t.Setenv("CLUSTER_ENGINE_RECONCILIATION_SCAN_DISABLED", "true")
		t.Setenv("CLUSTER_ENGINE_RECONCILIATION_INTERVAL_SECONDS", "12")
		t.Setenv("CLUSTER_ENGINE_RECONCILIATION_GRACE_PERIOD_SECONDS", "34")
		t.Setenv("CLUSTER_ENGINE_RECONCILIATION_BATCH_SIZE", "56")
		var cfg Config
		if err := cleanenv.ReadEnv(&cfg); err != nil {
			t.Fatal(err)
		}
		e := cfg.Cluster.Engine
		if !e.ReconciliationScanDisabled || e.ReconciliationIntervalSeconds != 12 || e.ReconciliationGracePeriodSeconds != 34 || e.ReconciliationBatchSize != 56 {
			t.Fatalf("unexpected environment recovery config: %+v", e)
		}
	})

	t.Run("YAML", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		contents := "cluster:\n  engine:\n    reconciliationScanDisabled: true\n    reconciliationIntervalSeconds: 13\n    reconciliationGracePeriodSeconds: 35\n    reconciliationBatchSize: 57\n"
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		var cfg Config
		if err := cleanenv.ReadConfig(path, &cfg); err != nil {
			t.Fatal(err)
		}
		e := cfg.Cluster.Engine
		if !e.ReconciliationScanDisabled || e.ReconciliationIntervalSeconds != 13 || e.ReconciliationGracePeriodSeconds != 35 || e.ReconciliationBatchSize != 57 {
			t.Fatalf("unexpected YAML recovery config: %+v", e)
		}
	})

	t.Run("invalid bounds", func(t *testing.T) {
		valid := Engine{ReconciliationIntervalSeconds: 60, ReconciliationGracePeriodSeconds: 60, ReconciliationBatchSize: 256}
		cases := []struct {
			name   string
			change func(*Engine)
			field  string
		}{
			{"zero interval", func(e *Engine) { e.ReconciliationIntervalSeconds = 0 }, "reconciliationIntervalSeconds"},
			{"overflow interval", func(e *Engine) { e.ReconciliationIntervalSeconds = math.MaxInt64/int64(time.Second) + 1 }, "reconciliationIntervalSeconds"},
			{"zero grace", func(e *Engine) { e.ReconciliationGracePeriodSeconds = 0 }, "reconciliationGracePeriodSeconds"},
			{"overflow grace", func(e *Engine) { e.ReconciliationGracePeriodSeconds = math.MaxInt64/int64(time.Second) + 1 }, "reconciliationGracePeriodSeconds"},
			{"zero batch", func(e *Engine) { e.ReconciliationBatchSize = 0 }, "reconciliationBatchSize"},
		}
		for _, tc := range cases {
			e := valid
			tc.change(&e)
			if err := e.ValidateReconciliation(); err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Errorf("%s: expected error containing %q, got %v", tc.name, tc.field, err)
			}
		}
	})
}
