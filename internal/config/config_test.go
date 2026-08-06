package config

import (
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
)

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

	if err := c.validate(); err == nil {
		t.Error("expected validation error for non-positive request body limit")
	}
}
