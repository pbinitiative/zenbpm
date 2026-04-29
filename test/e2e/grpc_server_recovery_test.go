package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/grpc/interceptor/recovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const gRPCEndpoint = "/test.Panic/Unary"
const recoverBufSizeOneMegaByte = 1 << 20

func TestGRPCServerRecovery(t *testing.T) {

	conn := startRecoverServer(t)

	t.Run("gRPC Server should recover from panic in unary handler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		out := new(emptypb.Empty)

		err := conn.Invoke(ctx, gRPCEndpoint, &emptypb.Empty{}, out)
		assertErrorResponse(t, err)

		// ensure the server still handles requests after recovering from the panic
		err = conn.Invoke(ctx, gRPCEndpoint, &emptypb.Empty{}, out)
		assertErrorResponse(t, err)
	})

	t.Run("gRPC Server should recover from panic in stream handler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		streamDesc := &grpc.StreamDesc{
			StreamName:    "Stream",
			ServerStreams: true,
			ClientStreams: true,
		}

		clientStream, err := conn.NewStream(ctx, streamDesc, "/test.Panic/Stream")
		require.NoError(t, err)
		assertErrorResponse(t, clientStream.RecvMsg(new(emptypb.Empty)))

		// ensure the server still handles requests after recovering from the panic
		clientStream2, err := conn.NewStream(ctx, streamDesc, "/test.Panic/Stream")
		require.NoError(t, err)
		assertErrorResponse(t, clientStream2.RecvMsg(new(emptypb.Empty)))
	})
}

func assertErrorResponse(t *testing.T, err error) {
	t.Helper()

	errorStatus, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got %v", err)
	assert.Equal(t, codes.Internal, errorStatus.Code())
	assert.Equal(t, "An unexpected error occurred while processing the request", errorStatus.Message())
}

var panicService = grpc.ServiceDesc{
	ServiceName: "test.Panic",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Unary",
		Handler: func(server any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(emptypb.Empty)
			if err := dec(in); err != nil {
				return nil, err
			}
			info := &grpc.UnaryServerInfo{Server: server, FullMethod: gRPCEndpoint}
			handler := func(ctx context.Context, req any) (any, error) { panic("unary boom") }
			if interceptor == nil {
				return handler(ctx, in)
			}
			return interceptor(ctx, in, info, handler)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       func(server any, ss grpc.ServerStream) error { panic("stream boom") },
		ServerStreams: true,
		ClientStreams: true,
	}},
}

func startRecoverServer(t *testing.T) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(recoverBufSizeOneMegaByte)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recovery.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(recovery.StreamServerInterceptor()),
	)
	server.RegisterService(&panicService, struct{}{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
