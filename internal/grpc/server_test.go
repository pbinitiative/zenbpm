package grpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRecvClientRequestsSanitizesSubscriptionErrors(t *testing.T) {
	internalErr := errors.New("node-42 at 10.0.0.1 refused the stream")
	tests := []struct {
		name        string
		requestType proto.StreamSubscriptionRequest_Type
		configure   func(*jobStreamTestManager)
		expected    string
	}{
		{
			name:        "subscribe",
			requestType: proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE,
			configure:   func(manager *jobStreamTestManager) { manager.subscribeErr = internalErr },
			expected:    "Failed to subscribe to job type job-a",
		},
		{
			name:        "unsubscribe",
			requestType: proto.StreamSubscriptionRequest_TYPE_UNSUBSCRIBE,
			configure:   func(manager *jobStreamTestManager) { manager.unsubscribeErr = internalErr },
			expected:    "Failed to unsubscribe from job type job-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &jobStreamTestManager{}
			tt.configure(manager)
			stream := newJobStreamTestServer(subscriptionRequest(tt.requestType))
			server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

			server.recvClientRequests(stream, "client-1", &sync.Mutex{})

			require.Len(t, stream.sent, 1)
			require.NotNil(t, stream.sent[0].Error)
			assert.Equal(t, tt.expected, stream.sent[0].Error.GetMessage())
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "node-42")
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "10.0.0.1")
		})
	}
}

func TestRecvClientRequestsSanitizesJobOperationErrors(t *testing.T) {
	internalErr := errors.New("node-42 at 10.0.0.1 refused the request")
	tests := []struct {
		name      string
		request   *proto.JobStreamRequest
		configure func(*jobStreamTestManager)
		expected  string
	}{
		{
			name:     "invalid complete variables",
			request:  completeRequest([]byte("{")),
			expected: "Invalid job variables",
		},
		{
			name:      "completion failure",
			request:   completeRequest(nil),
			configure: func(manager *jobStreamTestManager) { manager.completeErr = internalErr },
			expected:  "Failed to complete job",
		},
		{
			name:     "invalid failure variables",
			request:  failRequest([]byte("{")),
			expected: "Invalid job variables",
		},
		{
			name:      "failure request failure",
			request:   failRequest(nil),
			configure: func(manager *jobStreamTestManager) { manager.failErr = internalErr },
			expected:  "Failed to process job failure request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &jobStreamTestManager{}
			if tt.configure != nil {
				tt.configure(manager)
			}
			stream := newJobStreamTestServer(tt.request)
			server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

			server.recvClientRequests(stream, "client-1", &sync.Mutex{})

			require.Len(t, stream.sent, 1)
			require.NotNil(t, stream.sent[0].Error)
			assert.Equal(t, tt.expected, stream.sent[0].Error.GetMessage())
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "node-42")
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "10.0.0.1")
			assert.NotContains(t, stream.sent[0].Error.GetMessage(), "invalid character")
		})
	}
}

func TestRecvClientRequestsStopsAfterSubscriptionErrorSendFailure(t *testing.T) {
	manager := &jobStreamTestManager{subscribeErr: errors.New("subscription failed")}
	stream := newJobStreamTestServer(
		subscriptionRequest(proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE),
		subscriptionRequest(proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE),
	)
	stream.sendErr = errors.New("stream closed")
	server := &Server{jobManager: manager, logger: hclog.NewNullLogger()}

	server.recvClientRequests(stream, "client-1", &sync.Mutex{})

	assert.Equal(t, 1, stream.recvCalls, "the receive loop must stop after the stream rejects a response")
	assert.Equal(t, 1, manager.subscribeCalls)
}

func TestDecodeVariables(t *testing.T) {
	t.Run("nil payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables(nil)
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("empty payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables([]byte{})
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("null payload returns empty map", func(t *testing.T) {
		vars, err := decodeVariables([]byte("null"))
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("object payload returns map", func(t *testing.T) {
		vars, err := decodeVariables([]byte(`{"testVar":123}`))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"testVar": float64(123)}, vars)
	})

	t.Run("invalid payload returns error", func(t *testing.T) {
		vars, err := decodeVariables([]byte("{"))
		require.Error(t, err)
		assert.Nil(t, vars)
	})
}

func TestUnknownRequestError(t *testing.T) {
	t.Run("unknown subscription type returns error response, no panic", func(t *testing.T) {
		req := &proto.JobStreamRequest{
			Request: &proto.JobStreamRequest_Subscription{
				Subscription: &proto.StreamSubscriptionRequest{
					Type: new(proto.StreamSubscriptionRequest_Type(9999)),
				},
			},
		}
		var resp *proto.JobStreamResponse
		require.NotPanics(t, func() {
			resp = unknownRequestError(req.Request)
		})
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.NotNil(t, resp.Error.Message)
		assert.Contains(t, *resp.Error.Message, "unexpected")
	})

	t.Run("unknown top-level request type returns error response, no panic", func(t *testing.T) {
		var resp *proto.JobStreamResponse
		require.NotPanics(t, func() {
			resp = unknownRequestError(nil)
		})
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.NotNil(t, resp.Error.Message)
		assert.Contains(t, *resp.Error.Message, "unexpected")
	})
}

type jobStreamTestManager struct {
	subscribeErr     error
	unsubscribeErr   error
	completeErr      error
	failErr          error
	subscribeCalls   int
	unsubscribeCalls int
}

func (*jobStreamTestManager) AddClient(context.Context, jobmanager.ClientID, chan jobmanager.Job) error {
	return nil
}

func (*jobStreamTestManager) RemoveClient(context.Context, jobmanager.ClientID) {}

func (m *jobStreamTestManager) AddClientJobSub(context.Context, jobmanager.ClientID, jobmanager.JobType) error {
	m.subscribeCalls++
	return m.subscribeErr
}

func (m *jobStreamTestManager) RemoveClientJobSub(context.Context, jobmanager.ClientID, jobmanager.JobType) error {
	m.unsubscribeCalls++
	return m.unsubscribeErr
}

func (m *jobStreamTestManager) CompleteJobReq(context.Context, jobmanager.ClientID, int64, map[string]any) error {
	return m.completeErr
}

func (m *jobStreamTestManager) FailJobReq(context.Context, jobmanager.ClientID, int64, string, *string, map[string]any) error {
	return m.failErr
}

type jobStreamTestServer struct {
	ctx       context.Context
	requests  []*proto.JobStreamRequest
	recvCalls int
	sent      []*proto.JobStreamResponse
	sendErr   error
}

func newJobStreamTestServer(requests ...*proto.JobStreamRequest) *jobStreamTestServer {
	return &jobStreamTestServer{ctx: context.Background(), requests: requests}
}

func (s *jobStreamTestServer) Recv() (*proto.JobStreamRequest, error) {
	if s.recvCalls >= len(s.requests) {
		return nil, io.EOF
	}
	req := s.requests[s.recvCalls]
	s.recvCalls++
	return req, nil
}

func (s *jobStreamTestServer) Send(response *proto.JobStreamResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, response)
	return nil
}

func (*jobStreamTestServer) SetHeader(metadata.MD) error  { return nil }
func (*jobStreamTestServer) SendHeader(metadata.MD) error { return nil }
func (*jobStreamTestServer) SetTrailer(metadata.MD)       {}
func (s *jobStreamTestServer) Context() context.Context   { return s.ctx }
func (*jobStreamTestServer) SendMsg(any) error            { return nil }
func (*jobStreamTestServer) RecvMsg(any) error            { return nil }

func subscriptionRequest(requestType proto.StreamSubscriptionRequest_Type) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				Type:    requestType.Enum(),
				JobType: new("job-a"),
			},
		},
	}
}

func completeRequest(variables []byte) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Complete{
			Complete: &proto.JobCompleteRequest{
				Key:       new(int64(42)),
				Variables: variables,
			},
		},
	}
}

func failRequest(variables []byte) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Fail{
			Fail: &proto.JobFailRequest{
				Key:       new(int64(42)),
				Variables: variables,
			},
		},
	}
}
