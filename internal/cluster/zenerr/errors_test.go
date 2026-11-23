package zenerr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsCanceled(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		canceled bool
	}{
		{name: "context cancellation", err: context.Canceled, canceled: true},
		{name: "wrapped context cancellation", err: fmt.Errorf("recv: %w", context.Canceled), canceled: true},
		{name: "gRPC Canceled status", err: status.Error(codes.Canceled, "context canceled"), canceled: true},
		{name: "gRPC status of a cancelled context", err: status.FromContextError(context.Canceled).Err(), canceled: true},
		{name: "wrapped gRPC Canceled status", err: fmt.Errorf("recv: %w", status.Error(codes.Canceled, "context canceled")), canceled: true},
		{name: "cancellation in a ZenError", err: TechnicalError(status.Error(codes.Canceled, "context canceled")), canceled: true},
		{name: "no error", err: nil},
		{name: "end of stream", err: io.EOF},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "gRPC DeadlineExceeded status", err: status.Error(codes.DeadlineExceeded, "deadline exceeded")},
		{name: "gRPC Unavailable status", err: status.Error(codes.Unavailable, "transport is closing")},
		{name: "unexpected error", err: errors.New("broken pipe")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.canceled, IsCanceled(tt.err))
		})
	}
}

func TestIsSendToGonePeer(t *testing.T) {
	tests := []struct {
		name string
		err  error
		gone bool
	}{
		{name: "context cancellation", err: context.Canceled, gone: true},
		{name: "gRPC Canceled status of a cancelled stream", err: status.Error(codes.Canceled, "context canceled"), gone: true},
		{name: "gRPC Unavailable status of a closed connection", err: status.Error(codes.Unavailable, "transport is closing"), gone: true},
		{name: "wrapped gRPC Unavailable status", err: fmt.Errorf("send: %w", status.Error(codes.Unavailable, "transport is closing")), gone: true},
		{name: "no error", err: nil},
		{name: "message that does not encode", err: status.Error(codes.Internal, "grpc: error while marshaling")},
		{name: "message over the send limit", err: status.Error(codes.ResourceExhausted, "trying to send message larger than max")},
		{name: "gRPC DeadlineExceeded status", err: status.Error(codes.DeadlineExceeded, "deadline exceeded")},
		{name: "unexpected error", err: errors.New("broken pipe")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.gone, IsSendToGonePeer(tt.err))
		})
	}
}
