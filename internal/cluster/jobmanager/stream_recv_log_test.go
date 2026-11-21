package jobmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
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
			var logs bytes.Buffer
			server.logger = hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true, Level: hclog.Trace})
			sub := &nodeSub{nodeID: "node-2", stream: &recvTestServerStream{ctx: t.Context(), recvErr: tt.err}}
			server.nodeSubs[sub.nodeID] = sub

			server.handleJobStreamRecv(sub)

			entries := logEntries(t, &logs)
			require.Len(t, entries, 1, "log: %s", logs.String())
			assert.Equal(t, tt.level, entries[0]["@level"], "log: %s", logs.String())
			assert.Equal(t, "node-2", entries[0]["streamNodeId"])
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
}

func (s *recvTestClientStream) Recv() (*proto.SubscribeJobResponse, error) {
	if len(s.responses) > 0 {
		resp := s.responses[0]
		s.responses = s.responses[1:]
		return resp, nil
	}
	return nil, s.recvErr
}

// recvTestServerStream is a leader-side job stream whose receive fails with
// recvErr.
type recvTestServerStream struct {
	ctx     context.Context
	recvErr error
}

func (s *recvTestServerStream) Recv() (*proto.SubscribeJobRequest, error) {
	return nil, s.recvErr
}
func (*recvTestServerStream) Send(*proto.SubscribeJobResponse) error { return nil }
func (*recvTestServerStream) SetHeader(metadata.MD) error            { return nil }
func (*recvTestServerStream) SendHeader(metadata.MD) error           { return nil }
func (*recvTestServerStream) SetTrailer(metadata.MD)                 {}
func (s *recvTestServerStream) Context() context.Context             { return s.ctx }
func (*recvTestServerStream) SendMsg(any) error                      { return nil }
func (*recvTestServerStream) RecvMsg(any) error                      { return nil }
