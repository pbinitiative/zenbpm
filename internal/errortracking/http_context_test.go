package errortracking

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getsentry/sentry-go"
)

func TestHTTPContext(t *testing.T) {
	t.Run("attaches request data to captured errors", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)
		request := httptest.NewRequest(http.MethodGet, "https://zenbpm.example/v1/processes?state=ACTIVE", nil).WithContext(ctx)
		recorder := httptest.NewRecorder()

		HTTPContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			CaptureUnexpected(r.Context(), "rest.test", errors.New("request failed"))
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(recorder, request)

		event := transport.singleEvent(t)
		if event.Request == nil || event.Request.URL != "https://zenbpm.example/v1/processes" {
			t.Fatalf("request = %#v, want captured request URL without query string", event.Request)
		}
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})

	t.Run("does not buffer request bodies", func(t *testing.T) {
		ctx, transport := newRecordingContext(t)
		requestBody := `{"businessKey":"order-123"}`
		body := &testRequestBody{Reader: strings.NewReader(requestBody)}
		request := httptest.NewRequest(http.MethodPost, "https://zenbpm.example/v1/processes", nil).WithContext(ctx)
		request.Body = body
		request.ContentLength = int64(len(requestBody))
		request.Header.Set("X-Request-ID", "request-123")
		recorder := httptest.NewRecorder()

		HTTPContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != body {
				t.Fatal("request body was wrapped by error tracking")
			}
			gotBody, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if string(gotBody) != requestBody {
				t.Fatalf("request body = %q, want %q", gotBody, requestBody)
			}
			CaptureUnexpected(r.Context(), "rest.test", errors.New("request failed"))
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(recorder, request)

		event := transport.singleEvent(t)
		if event.Request == nil {
			t.Fatal("expected request metadata")
		}
		if event.Request.Method != http.MethodPost {
			t.Fatalf("request method = %q, want POST", event.Request.Method)
		}
		if event.Request.Headers["X-Request-Id"] != "request-123" {
			t.Fatalf("request headers = %v, want request ID", event.Request.Headers)
		}
		if event.Request.Data != "" {
			t.Fatalf("captured request body = %q, want empty", event.Request.Data)
		}
	})

	t.Run("bypasses request setup when reporting is disabled", func(t *testing.T) {
		restoreCurrentClientAfterTest(t)
		sentry.CurrentHub().BindClient(nil)
		body := &testRequestBody{Reader: strings.NewReader("request body")}
		request := httptest.NewRequest(http.MethodPost, "https://zenbpm.example/v1/processes", nil)
		request.Body = body
		recorder := httptest.NewRecorder()

		HTTPContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r != request {
				t.Fatal("disabled middleware cloned the request")
			}
			if r.Body != body {
				t.Fatal("disabled middleware changed the request body")
			}
			if hub := sentry.GetHubFromContext(r.Context()); hub != nil {
				t.Fatal("disabled middleware attached a Sentry hub")
			}
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(recorder, request)

		if recorder.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
	})
}

type testRequestBody struct {
	*strings.Reader
}

func (b *testRequestBody) Close() error {
	return nil
}
