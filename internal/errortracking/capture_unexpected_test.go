package errortracking

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestCaptureUnexpected(t *testing.T) {
	t.Run("captures an exception with a stable fingerprint and stack", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)

		eventID := CaptureUnexpected(ctx, "test.unexpected", errors.New("state 99"))
		if eventID == nil {
			t.Fatal("expected an event ID")
		}
		event := transport.singleEvent(t)
		if !slices.Equal(event.Fingerprint, []string{"zenbpm", "test.unexpected"}) {
			t.Fatalf("fingerprint = %v, want stable error code", event.Fingerprint)
		}
		if event.Tags["error.kind"] != "unexpected" {
			t.Fatalf("error.kind = %q, want unexpected", event.Tags["error.kind"])
		}
		if event.Tags["error.code"] != "test.unexpected" {
			t.Fatalf("error.code = %q, want test.unexpected", event.Tags["error.code"])
		}
		if len(event.Exception) != 1 || event.Exception[0].Stacktrace == nil || len(event.Exception[0].Stacktrace.Frames) == 0 {
			t.Fatalf("expected exception stack trace, got %#v", event.Exception)
		}
	})

	t.Run("ignores a nil error", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)

		if eventID := CaptureUnexpected(ctx, "test.nil", nil); eventID != nil {
			t.Fatalf("event ID = %v, want nil", eventID)
		}
		if got := transport.eventCount(); got != 0 {
			t.Fatalf("captured events = %d, want 0", got)
		}
	})
}

func newRecordingContext(t *testing.T) (context.Context, *recordingTransport) {
	t.Helper()
	transport := &recordingTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		AttachStacktrace: true,
		Transport:        transport,
	})
	if err != nil {
		t.Fatalf("create Sentry client: %v", err)
	}
	hub := sentry.NewHub(client, sentry.NewScope())
	return sentry.SetHubOnContext(t.Context(), hub), transport
}

type recordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *recordingTransport) Configure(sentry.ClientOptions) {}

func (r *recordingTransport) SendEvent(event *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recordingTransport) Flush(time.Duration) bool {
	return true
}

func (r *recordingTransport) FlushWithContext(context.Context) bool {
	return true
}

func (r *recordingTransport) Close() {}

func (r *recordingTransport) eventCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func (r *recordingTransport) singleEvent(t *testing.T) *sentry.Event {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) != 1 {
		t.Fatalf("captured events = %d, want 1", len(r.events))
	}
	return r.events[0]
}
