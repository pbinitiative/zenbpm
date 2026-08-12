// Package errortracking reports application failures to Sentry-compatible backends.
package errortracking

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

const fingerprintNamespace = "zenbpm"

// Init configures error reporting from SENTRY_DSN. SENTRY_ENABLED can disable
// reporting even when a DSN is configured, and SENTRY_ENVIRONMENT overrides the
// supplied default. Error reporting is also disabled when SENTRY_DSN is unset or empty.
func Init(release string, defaultEnvironment string) error {
	enabled, err := enabledFromEnv()
	if err != nil {
		sentry.CurrentHub().BindClient(nil)
		return err
	}
	dsn := strings.TrimSpace(dsnFromEnv())
	if !enabled || dsn == "" {
		sentry.CurrentHub().BindClient(nil)
		return nil
	}

	// Do not leave a client from an earlier initialization bound if the new
	// configuration is invalid.
	sentry.CurrentHub().BindClient(nil)
	return sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Release:          release,
		Environment:      valueOrDefault("SENTRY_ENVIRONMENT", defaultEnvironment),
		AttachStacktrace: true,
		EnableTracing:    false,
		DisableLogs:      true,
		Tags: map[string]string{
			"service": "zenbpm",
		},
	})
}

func Flush(timeout time.Duration) bool {
	if sentry.CurrentHub().Client() == nil {
		return true
	}
	return sentry.Flush(timeout)
}

// CapturePanic reports a recovered panic. Panics keep GlitchTip's default
// stack-based grouping so different crashes are not grouped together merely
// because they occurred in the same component.
func CapturePanic(ctx context.Context, recovered any, code string) *sentry.EventID {
	return capturePanic(ctx, recovered, code, nil)
}

func CaptureGRPCPanic(ctx context.Context, recovered any, method string) *sentry.EventID {
	return capturePanic(ctx, recovered, "grpc.handler", func(scope *sentry.Scope) {
		if method != "" {
			scope.SetTag("rpc.method", method)
		}
	})
}

func capturePanic(ctx context.Context, recovered any, code string, configureScope func(*sentry.Scope)) *sentry.EventID {
	if recovered == nil {
		return nil
	}
	panicErr, ok := recovered.(error)
	if !ok {
		// sentry-go represents non-error panic values as message events and puts
		// their stack trace under the threads interface. GlitchTip does not retain
		// that interface, so normalize them to exceptions with a visible stack.
		panicErr = fmt.Errorf("%v", recovered)
	}

	ctx = nonNilContext(ctx)
	hub := hubForContext(ctx)
	var eventID *sentry.EventID
	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelFatal)
		scope.SetTag("error.kind", "panic")
		if code != "" {
			scope.SetTag("error.code", code)
		}
		if configureScope != nil {
			configureScope(scope)
		}
		eventID = hub.RecoverWithContext(ctx, panicErr)
	})
	return eventID
}

func CaptureUnexpected(ctx context.Context, code string, err error) *sentry.EventID {
	if err == nil {
		return nil
	}

	ctx = nonNilContext(ctx)
	hub := hubForContext(ctx)
	var eventID *sentry.EventID
	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)
		scope.SetTag("error.kind", "unexpected")
		if code != "" {
			scope.SetTag("error.code", code)
			scope.SetFingerprint([]string{fingerprintNamespace, code})
		}
		eventID = hub.CaptureException(err)
	})
	return eventID
}

func HTTPContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub := currentHubForContext(r.Context())
		if hub.Client() == nil {
			next.ServeHTTP(w, r)
			return
		}

		hub = hub.Clone()
		requestForScope := r.Clone(r.Context())
		requestForScope.Body = http.NoBody
		requestForScope.Header.Del("Authorization")
		requestForScope.Header.Del("Proxy-Authorization")
		requestForScope.Header.Del("Cookie")
		requestForScope.Header.Del("X-CSRF-Token")
		requestForScope.Header.Del("X-XSRF-Token")
		requestForScope.Header.Del("Referer")
		hub.Scope().SetRequest(requestForScope)
		ctx := sentry.SetHubOnContext(r.Context(), hub)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func dsnFromEnv() string {
	return os.Getenv("SENTRY_DSN")
}

func enabledFromEnv() (bool, error) {
	value, exists := os.LookupEnv("SENTRY_ENABLED")
	if !exists || strings.TrimSpace(value) == "" {
		return true, nil
	}

	enabled, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("parse SENTRY_ENABLED: %w", err)
	}
	return enabled, nil
}

func valueOrDefault(key string, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func hubForContext(ctx context.Context) *sentry.Hub {
	return currentHubForContext(ctx).Clone()
}

func currentHubForContext(ctx context.Context) *sentry.Hub {
	if hub := sentry.GetHubFromContext(ctx); hub != nil {
		return hub
	}
	return sentry.CurrentHub()
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
