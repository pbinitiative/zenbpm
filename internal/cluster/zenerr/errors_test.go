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
