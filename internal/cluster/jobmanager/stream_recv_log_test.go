package jobmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// streamRecvEndings are the ways a job stream receive can end, with the log
// level each one deserves: a stream closed or cancelled on purpose (a peer
// disconnecting, a node shutting down) is no error, whichever form the
// cancellation takes, while a transport failure stays visible as one.
var streamRecvEndings = []struct {
	name  string
	err   error
	level string
}{
	{name: "end of stream", err: io.EOF, level: "debug"},
	{name: "context cancellation", err: context.Canceled, level: "debug"},
	{name: "gRPC Canceled status", err: status.Error(codes.Canceled, "context canceled"), level: "debug"},
	{name: "wrapped gRPC Canceled status", err: fmt.Errorf("recv: %w", status.Error(codes.Canceled, "context canceled")), level: "debug"},
	{name: "gRPC Unavailable status", err: status.Error(codes.Unavailable, "error reading from server: connection reset by peer"), level: "error"},
	{name: "unexpected error", err: errors.New("broken pipe"), level: "error"},
}

func TestJobClientStreamRecvLogsExpectedClosesBelowError(t *testing.T) {
	for _, tt := range streamRecvEndings {
		t.Run(tt.name, func(t *testing.T) {
			client, logs := newRecvTestClient(t)
			stream := &clientNodeStream{
				stream: &recvTestClientStream{subscriptionTestStream: &subscriptionTestStream{ctx: t.Context()}, recvErr: tt.err},
				nodeID: "leader-node",
			}
			client.nodeStreams = []*clientNodeStream{stream}

			client.handleJobStreamRecv(stream)

			assert.Empty(t, client.nodeStreams, "the ended stream is dropped")
			entries := logEntries(t, logs)
			require.Len(t, entries, 1, "log: %s", logs.String())
			assert.Equal(t, tt.level, entries[0]["@level"], "log: %s", logs.String())
			assert.Equal(t, "leader-node", entries[0]["streamNodeId"])
		})
	}
}

// TestJobClientStreamRecvLogsLeaderCloseMessageBelowError covers the empty
// response a partition leader sends to every node stream once it stops
// distributing jobs: a normal shutdown or leadership change, not an error.
func TestJobClientStreamRecvLogsLeaderCloseMessageBelowError(t *testing.T) {
	client, logs := newRecvTestClient(t)
	stream := &clientNodeStream{
		stream: &recvTestClientStream{
			subscriptionTestStream: &subscriptionTestStream{ctx: t.Context()},
			responses:              []*proto.SubscribeJobResponse{{}},
		},
		nodeID: "leader-node",
	}
	client.nodeStreams = []*clientNodeStream{stream}

	client.handleJobStreamRecv(stream)

	assert.Empty(t, client.nodeStreams, "the closed stream is dropped")
	entries := logEntries(t, logs)
	require.Len(t, entries, 1, "log: %s", logs.String())
	assert.Equal(t, "debug", entries[0]["@level"], "log: %s", logs.String())
}

func TestJobServerStreamRecvLogsExpectedClosesBelowError(t *testing.T) {
	for _, tt := range streamRecvEndings {
		t.Run(tt.name, func(t *testing.T) {
			server := newJobServer("node-1", nil, nil, DefaultLockLimits())
			server.logger = hclog.NewNullLogger()
			sub := &nodeSub{nodeID: "node-2", stream: &recvTestServerStream{ctx: t.Context(), recvErr: tt.err}}
			server.nodeSubs[sub.nodeID] = sub
			server.subscribeClient(sub.nodeID, "client-1", "job-a", SubscriptionSettings{})
			require.Contains(t, server.subscriptions["job-a"], ClientID("client-1"))
			lockJobs(server, distributedJob{client: "client-1", jobType: "job-a", jobKey: 42, lockUntil: time.Now().Add(30 * time.Second)})
			var logs bytes.Buffer
			server.logger = hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true, Level: hclog.Trace})

			server.handleJobStreamRecv(sub)

			// however the stream ended, it is not used again
			assert.NotContains(t, server.nodeSubs, sub.nodeID, "the node of the ended stream is dropped")
			assert.NotContains(t, server.subscriptions["job-a"], ClientID("client-1"), "the clients of the ended stream are dropped")
			assert.Contains(t, server.distributedJobs, int64(42), "the locks of its clients' jobs are kept until they lapse")
			entries := logEntries(t, &logs)
			require.Len(t, entries, 1, "log: %s", logs.String())
			assert.Equal(t, tt.level, entries[0]["@level"], "log: %s", logs.String())
			assert.Equal(t, "node-2", entries[0]["streamNodeId"])
		})
	}
}

// TestJobClientStreamRecvDoesNotBlockShutdownOnAReceivedJob verifies that a
// job received while the job client closes does not keep the receive loop
// waiting for a distribution loop that is gone.
func TestJobClientStreamRecvDoesNotBlockShutdownOnAReceivedJob(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := newJobClient(ctx, "local-node", nil, nil)
	var logs bytes.Buffer
	client.logger = hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true, Level: hclog.Trace})
	recvStream := &recvTestClientStream{
		subscriptionTestStream: &subscriptionTestStream{ctx: t.Context()},
		responses:              []*proto.SubscribeJobResponse{{Job: &proto.InternalJob{Key: new(int64(42))}}},
		recvErr:                errors.New("received again after the job client closed"),
	}
	stream := &clientNodeStream{stream: recvStream, nodeID: "leader-node"}
	client.nodeStreams = []*clientNodeStream{stream}
	// nobody reads jobsChan: the distribution loop has exited
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.handleJobStreamRecv(stream)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond, "the receive loop returns once the job client closed")
	assert.Empty(t, client.nodeStreams)
	assert.Equal(t, 1, recvStream.recvCalls, "the loop stops at the job it could not hand over, without receiving again")
	entries := logEntries(t, &logs)
	require.Len(t, entries, 1, "log: %s", logs.String())
	assert.Equal(t, "debug", entries[0]["@level"], "log: %s", logs.String())
}

// TestJobServerSendLogsAStreamEndedOnPurposeBelowError verifies that a job
// send failing because the node went away (gRPC answers a send to a node that
// cancelled its stream with Canceled, and one to a node whose connection
// closed with Unavailable) or because the server stops is no error, while any
// other failure stays one, also though gRPC cancelled the stream when it
// aborted it on the failed send.
func TestJobServerSendLogsAStreamEndedOnPurposeBelowError(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name          string
		ctx           context.Context
		abortOnSend   bool
		serverStopped bool
		sendErr       error
		level         string
	}{
		{name: "gRPC Canceled status", ctx: t.Context(), sendErr: status.Error(codes.Canceled, "context canceled"), level: "debug"},
		{name: "failure gRPC aborted the stream on", ctx: t.Context(), abortOnSend: true, sendErr: status.Error(codes.ResourceExhausted, "trying to send message larger than max"), level: "error"},
		{name: "gRPC Unavailable status of a closed connection", ctx: t.Context(), sendErr: status.Error(codes.Unavailable, "transport is closing"), level: "debug"},
		{name: "failure while the server stops", ctx: t.Context(), serverStopped: true, sendErr: status.Error(codes.Internal, "grpc: error while marshaling"), level: "debug"},
		{name: "message that does not encode", ctx: t.Context(), sendErr: status.Error(codes.Internal, "grpc: error while marshaling"), level: "error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newJobServer("node-1", nil, nil, DefaultLockLimits())
			var logs bytes.Buffer
			server.logger = hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true, Level: hclog.Trace})
			if tt.serverStopped {
				server.ctx = cancelled
			}
			stream := &recvTestServerStream{ctx: tt.ctx, sendErr: tt.sendErr}
			if tt.abortOnSend {
				stream.ctx, stream.abort = context.WithCancel(tt.ctx)
			}
			sub := &nodeSub{nodeID: "node-2", stream: stream}

			sent := server.sendReservedJob(reservedDelivery{job: sql.Job{Key: 42, Type: "job-a"}, client: "client-1", stream: sub})

			assert.False(t, sent)
			entries := logEntries(t, &logs)
			require.Len(t, entries, 1, "log: %s", logs.String())
			assert.Equal(t, tt.level, entries[0]["@level"], "log: %s", logs.String())
		})
	}
}

func newRecvTestClient(t *testing.T) (*jobClient, *bytes.Buffer) {
	t.Helper()
	client := newJobClient(t.Context(), "local-node", nil, nil)
	logs := &bytes.Buffer{}
	client.logger = hclog.New(&hclog.LoggerOptions{Output: logs, JSONFormat: true, Level: hclog.Trace})
	return client, logs
}

func logEntries(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for line := range bytes.Lines(logs.Bytes()) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(line, &entry), "log line: %s", line)
		entries = append(entries, entry)
	}
	return entries
}

// recvTestClientStream is a node-side job stream that hands out the given
// responses and then fails with recvErr.
type recvTestClientStream struct {
	*subscriptionTestStream
	responses []*proto.SubscribeJobResponse
	recvErr   error
	recvCalls int
}

func (s *recvTestClientStream) Recv() (*proto.SubscribeJobResponse, error) {
	s.recvCalls++
	if len(s.responses) > 0 {
		resp := s.responses[0]
		s.responses = s.responses[1:]
		return resp, nil
	}
	return nil, s.recvErr
}

// recvTestServerStream is a leader-side job stream that hands out the given
// requests, then fails its receive with recvErr, and whose send fails with
// sendErr.
type recvTestServerStream struct {
	ctx      context.Context
	requests []*proto.SubscribeJobRequest
	recvErr  error
	sendErr  error
	// abort, if set, cancels ctx on a failed send, as gRPC does
	abort context.CancelFunc
}

func (s *recvTestServerStream) Recv() (*proto.SubscribeJobRequest, error) {
	if len(s.requests) > 0 {
		req := s.requests[0]
		s.requests = s.requests[1:]
		return req, nil
	}
	return nil, s.recvErr
}
func (s *recvTestServerStream) Send(*proto.SubscribeJobResponse) error {
	if s.sendErr != nil && s.abort != nil {
		s.abort()
	}
	return s.sendErr
}
func (*recvTestServerStream) SetHeader(metadata.MD) error  { return nil }
func (*recvTestServerStream) SendHeader(metadata.MD) error { return nil }
func (*recvTestServerStream) SetTrailer(metadata.MD)       {}
func (s *recvTestServerStream) Context() context.Context   { return s.ctx }
func (*recvTestServerStream) SendMsg(any) error            { return nil }
func (*recvTestServerStream) RecvMsg(any) error            { return nil }
