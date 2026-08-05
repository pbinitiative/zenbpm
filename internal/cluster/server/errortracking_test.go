package server

import (
	"slices"
	"testing"

	"github.com/getsentry/sentry-go"
)

func TestErrorTrackingCodes(t *testing.T) {
	t.Run("reports an invalid timer state with the cluster code", func(t *testing.T) {
		nextEvent := bindClusterRecordingClient(t)

		if _, err := timerStateToActivityState(-1); err == nil {
			t.Fatal("expected an invalid timer state error")
		}

		event := nextEvent()
		if event.Tags["error.code"] != errorCodeInvalidTimerState {
			t.Fatalf("error.code = %q, want %q", event.Tags["error.code"], errorCodeInvalidTimerState)
		}
		if !slices.Equal(event.Fingerprint, []string{"zenbpm", errorCodeInvalidTimerState}) {
			t.Fatalf("fingerprint = %v, want the cluster timer-state code", event.Fingerprint)
		}
	})

	t.Run("reports an invalid error state with the cluster code", func(t *testing.T) {
		nextEvent := bindClusterRecordingClient(t)

		if _, err := errorStateToActivityState(-1); err == nil {
			t.Fatal("expected an invalid error state error")
		}

		event := nextEvent()
		if event.Tags["error.code"] != errorCodeInvalidErrorState {
			t.Fatalf("error.code = %q, want %q", event.Tags["error.code"], errorCodeInvalidErrorState)
		}
		if !slices.Equal(event.Fingerprint, []string{"zenbpm", errorCodeInvalidErrorState}) {
			t.Fatalf("fingerprint = %v, want the cluster error-state code", event.Fingerprint)
		}
	})
}

func bindClusterRecordingClient(t *testing.T) func() *sentry.Event {
	t.Helper()
	events := make(chan *sentry.Event, 1)
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		AttachStacktrace: true,
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			events <- event
			return nil
		},
	})
	if err != nil {
		t.Fatalf("create Sentry client: %v", err)
	}
	previousClient := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() {
		sentry.CurrentHub().BindClient(previousClient)
		if remaining := len(events); remaining != 0 {
			t.Errorf("unexpected additional captured events: %d", remaining)
		}
	})

	return func() *sentry.Event {
		t.Helper()
		select {
		case event := <-events:
			return event
		default:
			t.Fatal("expected one captured event")
			return nil
		}
	}
}
