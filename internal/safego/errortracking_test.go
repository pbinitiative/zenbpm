package safego

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/require"
)

func TestErrorTracking(t *testing.T) {
	t.Run("Go emits one panic event from the recovered goroutine", func(t *testing.T) {
		transport := bindSafegoRecordingClient(t)
		logger := &safegoCompletionLogger{}

		Go("async-worker", logger, func() {
			panic("async boom")
		})

		require.Eventually(t, func() bool {
			return logger.logged.Load() && transport.eventCount() == 1
		}, 2*time.Second, 10*time.Millisecond)
		event := transport.singleEvent(t)
		require.Equal(t, "panic", event.Tags["error.kind"])
		require.Equal(t, "safego.async-worker", event.Tags["error.code"])
	})

	t.Run("Run emits one panic event before returning the recovered error", func(t *testing.T) {
		transport := bindSafegoRecordingClient(t)
		logger := &safegoCompletionLogger{}

		err := Run("sync-worker", logger, func() error {
			panic("sync boom")
		})

		require.ErrorContains(t, err, "sync boom")
		require.True(t, logger.logged.Load())
		event := transport.singleEvent(t)
		require.Equal(t, "panic", event.Tags["error.kind"])
		require.Equal(t, "safego.sync-worker", event.Tags["error.code"])
	})
}

func bindSafegoRecordingClient(t *testing.T) *safegoRecordingTransport {
	t.Helper()
	transport := &safegoRecordingTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		AttachStacktrace: true,
		Transport:        transport,
	})
	require.NoError(t, err)

	hub := sentry.CurrentHub()
	previousClient := hub.Client()
	hub.BindClient(client)
	t.Cleanup(func() {
		hub.BindClient(previousClient)
	})
	return transport
}

type safegoCompletionLogger struct {
	logged atomic.Bool
}

func (l *safegoCompletionLogger) Error(string, ...interface{}) {
	l.logged.Store(true)
}

type safegoRecordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (*safegoRecordingTransport) Configure(sentry.ClientOptions) {}

func (r *safegoRecordingTransport) SendEvent(event *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (*safegoRecordingTransport) Flush(time.Duration) bool {
	return true
}

func (*safegoRecordingTransport) FlushWithContext(context.Context) bool {
	return true
}

func (*safegoRecordingTransport) Close() {}

func (r *safegoRecordingTransport) eventCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func (r *safegoRecordingTransport) singleEvent(t *testing.T) *sentry.Event {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Len(t, r.events, 1)
	return r.events[0]
}
