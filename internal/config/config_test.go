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

func TestClusterPartitionRetryDelayFromEnv(t *testing.T) {
	t.Setenv("CLUSTER_PARTITION_RETRY_DELAY", "250ms")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.PartitionRetryDelay != 250*time.Millisecond {
		t.Errorf("expected PartitionRetryDelay 250ms, got %s", c.Cluster.PartitionRetryDelay)
	}
}

func TestClusterPartitionRetryDelayDefault(t *testing.T) {
	value, wasSet := os.LookupEnv("CLUSTER_PARTITION_RETRY_DELAY")
	t.Cleanup(func() {
		if wasSet {
			if err := os.Setenv("CLUSTER_PARTITION_RETRY_DELAY", value); err != nil {
				t.Errorf("failed to restore CLUSTER_PARTITION_RETRY_DELAY: %v", err)
			}
			return
		}
		if err := os.Unsetenv("CLUSTER_PARTITION_RETRY_DELAY"); err != nil {
			t.Errorf("failed to unset CLUSTER_PARTITION_RETRY_DELAY during cleanup: %v", err)
		}
	})
	if err := os.Unsetenv("CLUSTER_PARTITION_RETRY_DELAY"); err != nil {
		t.Fatalf("failed to unset CLUSTER_PARTITION_RETRY_DELAY: %v", err)
	}

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.PartitionRetryDelay != 5*time.Second {
		t.Errorf("expected default PartitionRetryDelay 5s, got %s", c.Cluster.PartitionRetryDelay)
	}
}

func TestRestoreLimitsDefaults(t *testing.T) {
	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.Restore.MaxPartitionRowBytes != 32<<20 {
		t.Errorf("expected default MaxPartitionRowBytes 33554432, got %d", c.Cluster.Restore.MaxPartitionRowBytes)
	}
	if c.Cluster.Restore.SpoolDir != "" {
		t.Errorf("expected SpoolDir to be resolved by validation, got %q", c.Cluster.Restore.SpoolDir)
	}
}

func TestRestoreSpoolDirDefaultsUnderTheDataDir(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("CLUSTER_RAFT_DIR", dataDir)

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if err := c.validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if want := filepath.Join(dataDir, "spool"); c.Cluster.Restore.SpoolDir != want {
		t.Errorf("expected SpoolDir %q, got %q", want, c.Cluster.Restore.SpoolDir)
	}
}

func TestRestoreSpoolDirFromEnvIsMadeAbsolute(t *testing.T) {
	t.Setenv("CLUSTER_RAFT_DIR", t.TempDir())
	t.Setenv("CLUSTER_RESTORE_SPOOL_DIR", "restore-scratch")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if err := c.validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if !filepath.IsAbs(c.Cluster.Restore.SpoolDir) || filepath.Base(c.Cluster.Restore.SpoolDir) != "restore-scratch" {
		t.Errorf("expected an absolute path ending in restore-scratch, got %q", c.Cluster.Restore.SpoolDir)
	}
}

func TestHttpServerLogModeFromEnv(t *testing.T) {
	t.Setenv("REST_API_LOG_MODE", "all")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.HttpServer.LogMode != LogModeAll {
		t.Errorf("expected LogMode %q, got %q", LogModeAll, c.HttpServer.LogMode)
	}
}

func TestHttpServerLogModeValidation(t *testing.T) {
	c := Config{}
	c.HttpServer.LogMode = "verbose"
	if err := c.validate(); err == nil {
		t.Error("expected validation error for unsupported log mode")
	}
}

func TestHttpServerLogModeDefault(t *testing.T) {
	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.HttpServer.LogMode != LogModeErrors {
		t.Errorf("expected default LogMode %q, got %q", LogModeErrors, c.HttpServer.LogMode)
	}
}

func TestHttpServerMaxRequestBodyBytesFromEnv(t *testing.T) {
	t.Setenv("REST_API_MAX_REQUEST_BODY_BYTES", "2048")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.HttpServer.MaxRequestBodyBytes != 2048 {
		t.Errorf("expected MaxRequestBodyBytes 2048, got %d", c.HttpServer.MaxRequestBodyBytes)
	}
}

func TestHttpServerMaxRequestBodyBytesDefault(t *testing.T) {
	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.HttpServer.MaxRequestBodyBytes != 10*1024*1024 {
		t.Errorf("expected MaxRequestBodyBytes 10485760, got %d", c.HttpServer.MaxRequestBodyBytes)
	}
}

func TestHttpServerMaxRequestBodyBytesValidation(t *testing.T) {
	c := Config{}
	c.HttpServer.LogMode = LogModeErrors

	err := c.validate()
	if err == nil {
		t.Fatal("expected validation error for non-positive request body limit")
	}
	if !strings.Contains(err.Error(), "httpServer.maxRequestBodyBytes must be greater than zero") {
		t.Errorf("expected request body limit validation error, got: %v", err)
	}
}

func TestEngineMaxProcessInstanceNestingDepthDefault(t *testing.T) {
	unsetEngineMaxProcessInstanceNestingDepthEnv(t)

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceNestingDepth != 100 {
		t.Errorf("expected default MaxProcessInstanceNestingDepth 100, got %d", c.Cluster.Engine.MaxProcessInstanceNestingDepth)
	}
}

func TestEngineMaxProcessInstanceNestingDepthFromEnv(t *testing.T) {
	t.Setenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH", "37")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceNestingDepth != 37 {
		t.Errorf("expected MaxProcessInstanceNestingDepth 37, got %d", c.Cluster.Engine.MaxProcessInstanceNestingDepth)
	}
}

func TestEngineMaxProcessInstanceNestingDepthFromYAML(t *testing.T) {
	unsetEngineMaxProcessInstanceNestingDepthEnv(t)
	configFile := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(configFile, []byte("cluster:\n  engine:\n    maxProcessInstanceNestingDepth: 42\n"), 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var c Config
	if err := cleanenv.ReadConfig(configFile, &c); err != nil {
		t.Fatalf("failed to read YAML config: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceNestingDepth != 42 {
		t.Errorf("expected MaxProcessInstanceNestingDepth 42, got %d", c.Cluster.Engine.MaxProcessInstanceNestingDepth)
	}
}

func TestEngineMaxProcessInstanceFlowNodeCountDefault(t *testing.T) {
	unsetEngineMaxProcessInstanceFlowNodeCountEnv(t)

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceFlowNodeCount != 10000 {
		t.Errorf("expected default MaxProcessInstanceFlowNodeCount 10000, got %d", c.Cluster.Engine.MaxProcessInstanceFlowNodeCount)
	}
}

func TestEngineMaxProcessInstanceFlowNodeCountFromEnv(t *testing.T) {
	t.Setenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT", "321")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceFlowNodeCount != 321 {
		t.Errorf("expected MaxProcessInstanceFlowNodeCount 321, got %d", c.Cluster.Engine.MaxProcessInstanceFlowNodeCount)
	}
}

func TestEngineMaxProcessInstanceFlowNodeCountFromYAML(t *testing.T) {
	unsetEngineMaxProcessInstanceFlowNodeCountEnv(t)
	configFile := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(configFile, []byte("cluster:\n  engine:\n    maxProcessInstanceFlowNodeCount: -1\n"), 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var c Config
	if err := cleanenv.ReadConfig(configFile, &c); err != nil {
		t.Fatalf("failed to read YAML config: %v", err)
	}
	if c.Cluster.Engine.MaxProcessInstanceFlowNodeCount != -1 {
		t.Errorf("expected MaxProcessInstanceFlowNodeCount -1, got %d", c.Cluster.Engine.MaxProcessInstanceFlowNodeCount)
	}
}

func unsetEngineMaxProcessInstanceNestingDepthEnv(t *testing.T) {
	t.Helper()
	value, wasSet := os.LookupEnv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH")
	t.Cleanup(func() {
		if wasSet {
			if err := os.Setenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH", value); err != nil {
				t.Errorf("failed to restore CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH: %v", err)
			}
			return
		}
		if err := os.Unsetenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH"); err != nil {
			t.Errorf("failed to unset CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH during cleanup: %v", err)
		}
	})
	if err := os.Unsetenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH"); err != nil {
		t.Fatalf("failed to unset CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH: %v", err)
	}
}

func unsetEngineMaxProcessInstanceFlowNodeCountEnv(t *testing.T) {
	t.Helper()
	value, wasSet := os.LookupEnv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT")
	t.Cleanup(func() {
		if wasSet {
			if err := os.Setenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT", value); err != nil {
				t.Errorf("failed to restore CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT: %v", err)
			}
			return
		}
		if err := os.Unsetenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT"); err != nil {
			t.Errorf("failed to unset CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT during cleanup: %v", err)
		}
	})
	if err := os.Unsetenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT"); err != nil {
		t.Fatalf("failed to unset CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_FLOW_NODE_COUNT: %v", err)
	}
}

func TestClusterDesiredPartitionsValidation(t *testing.T) {
	for _, desired := range []uint32{0, 1} {
		if err := (Cluster{DesiredPartitions: desired}).ValidateDesiredPartitions(); err != nil {
			t.Errorf("desiredPartitions=%d must be accepted, got %v", desired, err)
		}
	}
	err := Cluster{DesiredPartitions: 2}.ValidateDesiredPartitions()
	if err == nil {
		t.Fatal("expected validation error: a second partition can never finish bootstrapping")
	}
	if !strings.Contains(err.Error(), "desiredPartitions=2") {
		t.Errorf("error should name the offending value, got %q", err.Error())
	}
}

func TestJobManagerDefaults(t *testing.T) {
	for _, env := range []string{"JOB_MANAGER_DEFAULT_LOCK_DURATION_MS", "JOB_MANAGER_MAX_LOCK_DURATION_MS", "JOB_MANAGER_DEFAULT_MAX_ACTIVE_JOBS", "JOB_MANAGER_MAX_ACTIVE_JOBS_CAP"} {
		t.Setenv(env, "")
		if err := os.Unsetenv(env); err != nil {
			t.Fatalf("failed to unset %s: %v", env, err)
		}
	}

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	expected := JobManager{DefaultLockDurationMs: 30000, MaxLockDurationMs: 86400000, DefaultMaxActiveJobs: 10, MaxActiveJobsCap: 1000}
	if c.JobManager != expected {
		t.Errorf("expected job manager defaults %+v, got %+v", expected, c.JobManager)
	}
	if err := c.JobManager.Validate(); err != nil {
		t.Errorf("the defaults must validate, got %v", err)
	}
}

func TestJobManagerFromEnv(t *testing.T) {
	t.Setenv("JOB_MANAGER_DEFAULT_LOCK_DURATION_MS", "5000")
	t.Setenv("JOB_MANAGER_MAX_LOCK_DURATION_MS", "60000")
	t.Setenv("JOB_MANAGER_DEFAULT_MAX_ACTIVE_JOBS", "4")
	t.Setenv("JOB_MANAGER_MAX_ACTIVE_JOBS_CAP", "40")

	var c Config
	if err := cleanenv.ReadEnv(&c); err != nil {
		t.Fatalf("failed to read config from env: %v", err)
	}
	expected := JobManager{DefaultLockDurationMs: 5000, MaxLockDurationMs: 60000, DefaultMaxActiveJobs: 4, MaxActiveJobsCap: 40}
	if c.JobManager != expected {
		t.Errorf("expected job manager settings %+v, got %+v", expected, c.JobManager)
	}
}

func TestJobManagerFromYAML(t *testing.T) {
	configFile := t.TempDir() + "/config.yaml"
	content := "jobManager:\n  defaultLockDurationMs: 7000\n  maxLockDurationMs: 70000\n  defaultMaxActiveJobs: 7\n  maxActiveJobsCap: 70\n"
	if err := os.WriteFile(configFile, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var c Config
	if err := cleanenv.ReadConfig(configFile, &c); err != nil {
		t.Fatalf("failed to read YAML config: %v", err)
	}
	expected := JobManager{DefaultLockDurationMs: 7000, MaxLockDurationMs: 70000, DefaultMaxActiveJobs: 7, MaxActiveJobsCap: 70}
	if c.JobManager != expected {
		t.Errorf("expected job manager settings %+v, got %+v", expected, c.JobManager)
	}
}

func TestJobManagerValidationNamesFieldAndEnvVariable(t *testing.T) {
	valid := JobManager{DefaultLockDurationMs: 30000, MaxLockDurationMs: 86400000, DefaultMaxActiveJobs: 10, MaxActiveJobsCap: 1000}
	tests := []struct {
		name     string
		mutate   func(*JobManager)
		expected []string
	}{
		{"zero default lock duration", func(j *JobManager) { j.DefaultLockDurationMs = 0 }, []string{"jobManager.defaultLockDurationMs", "JOB_MANAGER_DEFAULT_LOCK_DURATION_MS"}},
		{"negative max lock duration", func(j *JobManager) { j.MaxLockDurationMs = -1 }, []string{"jobManager.maxLockDurationMs", "JOB_MANAGER_MAX_LOCK_DURATION_MS"}},
		{"default lock duration above its cap", func(j *JobManager) { j.DefaultLockDurationMs = j.MaxLockDurationMs + 1 }, []string{"jobManager.defaultLockDurationMs", "JOB_MANAGER_MAX_LOCK_DURATION_MS"}},
		{"zero default active jobs", func(j *JobManager) { j.DefaultMaxActiveJobs = 0 }, []string{"jobManager.defaultMaxActiveJobs", "JOB_MANAGER_DEFAULT_MAX_ACTIVE_JOBS"}},
		{"zero active jobs cap", func(j *JobManager) { j.MaxActiveJobsCap = 0 }, []string{"jobManager.maxActiveJobsCap", "JOB_MANAGER_MAX_ACTIVE_JOBS_CAP"}},
		{"default active jobs above its cap", func(j *JobManager) { j.DefaultMaxActiveJobs = j.MaxActiveJobsCap + 1 }, []string{"jobManager.defaultMaxActiveJobs", "JOB_MANAGER_MAX_ACTIVE_JOBS_CAP"}},
		{"max lock duration the engine cannot represent", func(j *JobManager) { j.MaxLockDurationMs = MaxLockDurationMillis + 1 }, []string{"jobManager.maxLockDurationMs", "JOB_MANAGER_MAX_LOCK_DURATION_MS"}},
		{"max lock duration at the int64 limit", func(j *JobManager) { j.MaxLockDurationMs = math.MaxInt64 }, []string{"jobManager.maxLockDurationMs", "JOB_MANAGER_MAX_LOCK_DURATION_MS"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := valid
			tt.mutate(&conf)
			err := conf.Validate()
			if err == nil {
				t.Fatal("expected a validation error")
			}
			for _, expected := range tt.expected {
				if !strings.Contains(err.Error(), expected) {
					t.Errorf("error %q should name %q", err.Error(), expected)
				}
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("the valid configuration must pass, got %v", err)
	}
	largest := valid
	largest.MaxLockDurationMs = MaxLockDurationMillis
	if err := largest.Validate(); err != nil {
		t.Errorf("the largest representable cap must pass, got %v", err)
	}
}
