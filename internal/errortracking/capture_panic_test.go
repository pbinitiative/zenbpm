package errortracking

import (
	"context"
	"errors"
	"testing"

	"github.com/getsentry/sentry-go"
)

func TestCapturePanic(t *testing.T) {
	t.Run("captures an error panic with its stack", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)

		recoverErrorPanic(ctx)

		event := transport.singleEvent(t)
		if event.Level != sentry.LevelFatal {
			t.Fatalf("level = %q, want fatal", event.Level)
		}
		if event.Tags["error.kind"] != "panic" || event.Tags["error.code"] != "test.error_panic" {
			t.Fatalf("panic tags = %v", event.Tags)
		}
		if len(event.Exception) != 1 || event.Exception[0].Stacktrace == nil || len(event.Exception[0].Stacktrace.Frames) == 0 {
			t.Fatalf("expected panic stack trace, got %#v", event.Exception)
		}
		if len(event.Fingerprint) != 0 {
			t.Fatalf("panic fingerprint = %v, want default grouping", event.Fingerprint)
		}
	})

	t.Run("captures a string panic as an exception with its stack", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)

		recoverStringPanic(ctx)

		event := transport.singleEvent(t)
		if len(event.Exception) != 1 || event.Exception[0].Value != "boom" {
			t.Fatalf("panic exception = %#v, want one exception with value boom", event.Exception)
		}
		if event.Exception[0].Stacktrace == nil || len(event.Exception[0].Stacktrace.Frames) == 0 {
			t.Fatalf("expected exception stack trace, got %#v", event.Exception[0].Stacktrace)
		}
		if len(event.Threads) != 0 {
			t.Fatalf("panic threads = %#v, want exception-only event", event.Threads)
		}
	})
}

func recoverErrorPanic(ctx context.Context) {
	defer func() {
		CapturePanic(ctx, recover(), "test.error_panic")
	}()
	panic(errors.New("boom"))
}

func recoverStringPanic(ctx context.Context) {
	defer func() {
		CapturePanic(ctx, recover(), "test.string_panic")
	}()
	panic("boom")
}
