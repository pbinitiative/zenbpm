package errortracking

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestConfiguration(t *testing.T) {
	t.Run("leaves error reporting disabled when the DSN is unset", func(t *testing.T) {
		restoreEnvAfterTest(t, "SENTRY_DSN")
		if err := os.Unsetenv("SENTRY_DSN"); err != nil {
			t.Fatalf("unset SENTRY_DSN: %v", err)
		}

		if got := dsnFromEnv(); got != "" {
			t.Fatalf("DSN = %q, want empty value", got)
		}
	})

	t.Run("reads the DSN from the environment", func(t *testing.T) {
		t.Setenv("SENTRY_DSN", "https://public@example.com/1")

		if got := dsnFromEnv(); got != "https://public@example.com/1" {
			t.Fatalf("DSN = %q, want configured value", got)
		}
	})

	t.Run("enables reporting by default", func(t *testing.T) {
		restoreEnvAfterTest(t, "SENTRY_ENABLED")
		if err := os.Unsetenv("SENTRY_ENABLED"); err != nil {
			t.Fatalf("unset SENTRY_ENABLED: %v", err)
		}

		enabled, err := enabledFromEnv()
		if err != nil {
			t.Fatalf("read SENTRY_ENABLED: %v", err)
		}
		if !enabled {
			t.Fatal("reporting disabled by default, want enabled")
		}
	})

	t.Run("accepts an explicit enabled value", func(t *testing.T) {
		t.Setenv("SENTRY_ENABLED", "true")

		enabled, err := enabledFromEnv()
		if err != nil {
			t.Fatalf("read SENTRY_ENABLED: %v", err)
		}
		if !enabled {
			t.Fatal("reporting disabled, want enabled")
		}
	})

	t.Run("disables reporting even when a DSN is configured", func(t *testing.T) {
		restoreCurrentClientAfterTest(t)
		t.Setenv("SENTRY_ENABLED", "false")
		t.Setenv("SENTRY_DSN", "not-a-valid-dsn")

		if err := Init("test", "TEST"); err != nil {
			t.Fatalf("initialize disabled reporting: %v", err)
		}
		if client := sentry.CurrentHub().Client(); client != nil {
			t.Fatal("Sentry client is configured, want reporting disabled")
		}
		if eventID := CaptureUnexpected(context.Background(), "test.disabled", context.Canceled); eventID != nil {
			t.Fatalf("captured event %v while reporting is disabled", eventID)
		}
		if !Flush(time.Millisecond) {
			t.Fatal("flush reported a timeout while reporting is disabled")
		}
	})

	t.Run("disables reporting when the DSN is empty", func(t *testing.T) {
		restoreCurrentClientAfterTest(t)
		t.Setenv("SENTRY_ENABLED", "true")
		t.Setenv("SENTRY_DSN", "")

		if err := Init("test", "TEST"); err != nil {
			t.Fatalf("initialize reporting without a DSN: %v", err)
		}
		if client := sentry.CurrentHub().Client(); client != nil {
			t.Fatal("Sentry client is configured, want reporting disabled")
		}
	})

	t.Run("rejects an invalid enabled value", func(t *testing.T) {
		restoreCurrentClientAfterTest(t)
		t.Setenv("SENTRY_ENABLED", "sometimes")

		err := Init("test", "TEST")
		if err == nil {
			t.Fatal("expected an invalid SENTRY_ENABLED error")
		}
		if !strings.Contains(err.Error(), "SENTRY_ENABLED") {
			t.Fatalf("error %q does not identify SENTRY_ENABLED", err)
		}
		if client := sentry.CurrentHub().Client(); client != nil {
			t.Fatal("Sentry client is configured after invalid initialization")
		}
	})
}

func restoreEnvAfterTest(t *testing.T, key string) {
	t.Helper()
	value, exists := os.LookupEnv(key)
	t.Cleanup(func() {
		if exists {
			if err := os.Setenv(key, value); err != nil {
				t.Errorf("restore %s: %v", key, err)
			}
			return
		}
		if err := os.Unsetenv(key); err != nil {
			t.Errorf("unset %s: %v", key, err)
		}
	})
}

func restoreCurrentClientAfterTest(t *testing.T) {
	t.Helper()
	client := sentry.CurrentHub().Client()
	t.Cleanup(func() {
		sentry.CurrentHub().BindClient(client)
	})
}
