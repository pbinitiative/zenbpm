package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/errortracking"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

func TestInternalServerErrorTracking(t *testing.T) {
	t.Run("captures a typed 500 response with request context", func(t *testing.T) {
		ctx, nextEvent := newRESTRecordingContext(t)
		request := httptest.NewRequest(http.MethodPost, "https://zenbpm.example/v1/process-instances?wait=true", nil).WithContext(ctx)
		recorder := httptest.NewRecorder()

		errortracking.HTTPContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := public.CreateProcessInstance500JSONResponse(
				trackInternalServerError(r.Context(), zenerr.TechnicalError(errors.New("database failed"))),
			)
			if err := response.VisitCreateProcessInstanceResponse(w); err != nil {
				t.Fatalf("write typed response: %v", err)
			}
		})).ServeHTTP(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		var response public.Error
		if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if response.Code != "TECHNICAL_ERROR" || response.Message != "database failed" {
			t.Fatalf("response = %#v, want the original technical error", response)
		}

		event := nextEvent()
		if event.Request == nil || event.Request.URL != "https://zenbpm.example/v1/process-instances" {
			t.Fatalf("request = %#v, want captured request URL", event.Request)
		}
		if event.Tags["error.kind"] != "unexpected" {
			t.Fatalf("error.kind = %q, want unexpected", event.Tags["error.kind"])
		}
		if len(event.Fingerprint) != 0 {
			t.Fatalf("fingerprint = %v, want default grouping", event.Fingerprint)
		}
	})

	t.Run("preserves an existing API error response", func(t *testing.T) {
		ctx, nextEvent := newRESTRecordingContext(t)
		response := public.Error{Code: "TODO", Message: "pprof failed"}

		got := trackInternalServerErrorResponse(ctx, errors.New("pprof failed"), response)

		if got != response {
			t.Fatalf("response = %#v, want %#v", got, response)
		}
		nextEvent()
	})

	t.Run("captures an unhandled process instance state", func(t *testing.T) {
		ctx, nextEvent := newRESTRecordingContext(t)
		invalidState := public.GetProcessInstancesParamsState("NEW_STATE")
		server := &Server{}

		response, err := server.GetProcessInstances(ctx, public.GetProcessInstancesRequestObject{
			Params: public.GetProcessInstancesParams{State: &invalidState},
		})

		if err != nil {
			t.Fatalf("get process instances: %v", err)
		}
		if _, ok := response.(public.GetProcessInstances500JSONResponse); !ok {
			t.Fatalf("response type = %T, want a 500 response", response)
		}
		nextEvent()
	})

	t.Run("captures an unhandled child process instance state", func(t *testing.T) {
		ctx, nextEvent := newRESTRecordingContext(t)
		invalidState := public.GetChildProcessInstancesParamsState("NEW_STATE")
		server := &Server{}

		response, err := server.GetChildProcessInstances(ctx, public.GetChildProcessInstancesRequestObject{
			ProcessInstanceKey: 1,
			Params:             public.GetChildProcessInstancesParams{State: &invalidState},
		})

		if err != nil {
			t.Fatalf("get child process instances: %v", err)
		}
		if _, ok := response.(public.GetChildProcessInstances500JSONResponse); !ok {
			t.Fatalf("response type = %T, want a 500 response", response)
		}
		nextEvent()
	})
}

func newRESTRecordingContext(t *testing.T) (context.Context, func() *sentry.Event) {
	t.Helper()
	events := make(chan *sentry.Event, 2)
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
	hub := sentry.NewHub(client, sentry.NewScope())
	t.Cleanup(func() {
		if remaining := len(events); remaining != 0 {
			t.Errorf("unexpected additional captured events: %d", remaining)
		}
	})

	return sentry.SetHubOnContext(t.Context(), hub), func() *sentry.Event {
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
