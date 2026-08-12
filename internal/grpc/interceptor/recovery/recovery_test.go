package recovery

import (
	"context"
	"testing"

	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptors(t *testing.T) {
	t.Run("unary panic includes the RPC method and returns internal", func(t *testing.T) {
		ctx, nextEvent := newRecoveryRecordingContext(t)
		const method = "/test.Panic/Unary"

		_, err := UnaryServerInterceptor()(
			ctx,
			nil,
			&grpc.UnaryServerInfo{FullMethod: method},
			func(context.Context, any) (any, error) { panic("unary boom") },
		)

		assertInternalRecoveryError(t, err)
		assertRPCMethodTag(t, nextEvent(), method)
	})

	t.Run("stream panic includes the RPC method and returns internal", func(t *testing.T) {
		ctx, nextEvent := newRecoveryRecordingContext(t)
		const method = "/test.Panic/Stream"
		stream := &recoveryServerStream{ctx: ctx}

		err := StreamServerInterceptor()(
			nil,
			stream,
			&grpc.StreamServerInfo{FullMethod: method},
			func(any, grpc.ServerStream) error { panic("stream boom") },
		)

		assertInternalRecoveryError(t, err)
		assertRPCMethodTag(t, nextEvent(), method)
	})
}

type recoveryServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *recoveryServerStream) Context() context.Context {
	return s.ctx
}

func newRecoveryRecordingContext(t *testing.T) (context.Context, func() *sentry.Event) {
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

func assertInternalRecoveryError(t *testing.T, err error) {
	t.Helper()
	got := status.Convert(err)
	if got.Code() != codes.Internal {
		t.Fatalf("status code = %v, want %v", got.Code(), codes.Internal)
	}
	if got.Message() != recoveryErrorMessage {
		t.Fatalf("status message = %q, want %q", got.Message(), recoveryErrorMessage)
	}
}

func assertRPCMethodTag(t *testing.T, event *sentry.Event, method string) {
	t.Helper()
	if event.Tags["rpc.method"] != method {
		t.Fatalf("rpc.method = %q, want %q", event.Tags["rpc.method"], method)
	}
	if event.Tags["error.code"] != "grpc.handler" {
		t.Fatalf("error.code = %q, want grpc.handler", event.Tags["error.code"])
	}
}
