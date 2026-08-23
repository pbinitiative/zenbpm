package config

import (
	"os"
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
