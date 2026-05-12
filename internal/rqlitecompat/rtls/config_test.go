package rtls

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateClientConfigWithoutFiles(t *testing.T) {
	cfg, err := CreateClientConfig("", "", "", "example.com", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServerName != "example.com" {
		t.Fatalf("unexpected server name: %q", cfg.ServerName)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be true")
	}
	if len(cfg.NextProtos) != 2 {
		t.Fatalf("unexpected next protos: %v", cfg.NextProtos)
	}
}

func TestCreateClientConfigRejectsInvalidCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	if _, err := CreateClientConfig("", "", path, "", false); err == nil {
		t.Fatal("expected invalid CA error")
	}
}
