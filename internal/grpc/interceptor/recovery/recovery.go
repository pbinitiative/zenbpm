package recovery

import (
	"context"
	"runtime/debug"

	"github.com/pbinitiative/zenbpm/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recoveryErrorMessage = "An unexpected error occurred while processing the request"

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ctx, info.FullMethod, r)
			}
		}()
		return handler(ctx, req)
	}
}

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ss.Context(), info.FullMethod, r)
			}
		}()
		return handler(srv, ss)
	}
}

func handlePanic(ctx context.Context, method string, r any) error {
	log.Errorf(ctx, "panic recovered in gRPC handler %s: %v\n%s", method, r, debug.Stack())
	return status.Error(codes.Internal, recoveryErrorMessage)
}
