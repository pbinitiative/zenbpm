package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

func TestRecoveryErrorTracking(t *testing.T) {
	t.Run("panic emits one event with request context and returns internal server error", func(t *testing.T) {
		ctx, transport := newRESTRecoveryRecordingContext(t)
		server := newRESTRecoveryTestServer(t, "/test/recovery/panic", func(http.ResponseWriter, *http.Request) {
			panic("rest boom")
		})
		request := httptest.NewRequest(http.MethodPost, "https://zenbpm.example/test/recovery/panic?wait=true", nil).WithContext(ctx)
		request.Header.Set("X-Request-ID", "request-123")
		recorder := httptest.NewRecorder()

		server.server.Handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		var response public.Error
		if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
			t.Fatalf("decode recovery response: %v", err)
		}
		wantResponse := public.Error{Code: "ERROR", Message: "An unexpected error occurred while processing the request"}
		if response != wantResponse {
			t.Fatalf("response = %#v, want %#v", response, wantResponse)
		}

		event := transport.singleEvent(t)
		if event.Tags["error.kind"] != "panic" || event.Tags["error.code"] != "rest.handler" {
			t.Fatalf("panic tags = %v", event.Tags)
		}
		if event.Request == nil || event.Request.URL != "https://zenbpm.example/test/recovery/panic" {
			t.Fatalf("request = %#v, want captured panic request", event.Request)
		}
		if event.Request.Method != http.MethodPost || event.Request.QueryString != "wait=true" {
			t.Fatalf("request = %#v, want POST request with query string", event.Request)
		}
		if event.Request.Headers["X-Request-Id"] != "request-123" {
			t.Fatalf("request headers = %v, want request ID", event.Request.Headers)
		}
	})

	t.Run("abort handler panic is propagated without emitting an event", func(t *testing.T) {
		ctx, transport := newRESTRecoveryRecordingContext(t)
		server := newRESTRecoveryTestServer(t, "/test/recovery/abort", func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		})
		request := httptest.NewRequest(http.MethodGet, "https://zenbpm.example/test/recovery/abort", nil).WithContext(ctx)
		recorder := httptest.NewRecorder()

		var recovered any
		func() {
			defer func() { recovered = recover() }()
			server.server.Handler.ServeHTTP(recorder, request)
		}()

		if recovered != http.ErrAbortHandler {
			t.Fatalf("recovered panic = %v, want http.ErrAbortHandler", recovered)
		}
		if got := transport.eventCount(); got != 0 {
			t.Fatalf("captured events = %d, want 0", got)
		}
	})
}

func newRESTRecoveryTestServer(t *testing.T, pattern string, handler http.HandlerFunc) *Server {
	t.Helper()
	server := NewServer(nil, config.Config{
		HttpServer: config.HttpServer{LogMode: config.LogModeErrors},
	}, buildinfo.Info{})
	router, ok := server.server.Handler.(*chi.Mux)
	if !ok {
		t.Fatalf("server handler type = %T, want *chi.Mux", server.server.Handler)
	}
	router.Handle(pattern, handler)
	return server
}

func newRESTRecoveryRecordingContext(t *testing.T) (context.Context, *restRecoveryRecordingTransport) {
	t.Helper()
	transport := &restRecoveryRecordingTransport{}
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

type restRecoveryRecordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (*restRecoveryRecordingTransport) Configure(sentry.ClientOptions) {}

func (r *restRecoveryRecordingTransport) SendEvent(event *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (*restRecoveryRecordingTransport) Flush(time.Duration) bool {
	return true
}

func (*restRecoveryRecordingTransport) FlushWithContext(context.Context) bool {
	return true
}

func (*restRecoveryRecordingTransport) Close() {}

func (r *restRecoveryRecordingTransport) eventCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func (r *restRecoveryRecordingTransport) singleEvent(t *testing.T) *sentry.Event {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) != 1 {
		t.Fatalf("captured events = %d, want 1", len(r.events))
	}
	return r.events[0]
}
