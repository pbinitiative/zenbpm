package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestJobStreamLogsALocalSendFailureAsAnError runs a job stream over a real
// gRPC server. A job too large for the server's send limit fails to send, and
// gRPC aborts the stream on it, cancelling the stream context before Send
// returns. The failure is the server's own, not a client going away, and the
// log has to say so.
func TestJobStreamLogsALocalSendFailureAsAnError(t *testing.T) {
	logs := &sendErrorTestLog{}
	manager := &capturingJobStreamTestManager{added: make(chan chan jobmanager.Job, 1)}
	server := &Server{
		ctx:        t.Context(),
		jobManager: manager,
		logger:     hclog.New(&hclog.LoggerOptions{Output: logs, JSONFormat: true, Level: hclog.Trace}),
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(grpc.MaxSendMsgSize(256))
	proto.RegisterZenBpmServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stream, err := proto.NewZenBpmClient(conn).JobStream(t.Context())
	require.NoError(t, err)

	var clientCh chan jobmanager.Job
	select {
	case clientCh = <-manager.added:
	case <-time.After(5 * time.Second):
		t.Fatal("the job stream did not register its client")
	}
	clientCh <- jobmanager.Job{Key: 7, Type: "job-a", InputVariables: []byte(`{"payload":"` + strings.Repeat("x", 1024) + `"}`)}

	_, err = stream.Recv()
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "the client sees why the stream ended")
	require.Eventually(t, func() bool {
		return logs.levelOf("Failed to send a job to the job stream") != ""
	}, 5*time.Second, 5*time.Millisecond, "log: %s", logs)
	assert.Equal(t, "error", logs.levelOf("Failed to send a job to the job stream"), "log: %s", logs)
}

// The two messages of an answer that could not be sent: one for a stream whose
// handler still runs, one for a stream whose handler returned.
const (
	sendFailureMessage     = "Failed to send a job stream response"
	lateSendFailureMessage = "Failed to send a job stream response after the stream ended"
)

// TestJobStreamLogsASendToAClientThatClosedItsConnectionBelowError runs a job
// stream over a real gRPC server whose client closes its channel, which is how
// a worker normally shuts down, while the answer to its request is on the way.
// The departure is no error, whatever gRPC makes of the send: on a stream that
// has carried an answer it fails with Unavailable, not with the Canceled of a
// cancelled stream. The first answer of a stream may instead race the status
// gRPC writes once the handler returned, and fail with Internal.
func TestJobStreamLogsASendToAClientThatClosedItsConnectionBelowError(t *testing.T) {
	tests := []struct {
		name string
		// answeredBefore has the stream carry an answer before the client
		// leaves, as that of a worker at work
		answeredBefore bool
	}{
		{name: "a stream that has carried an answer", answeredBefore: true},
		{name: "the first answer of a stream"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := &sendErrorTestLog{}
			manager := &blockingExtendTestManager{extending: make(chan struct{})}
			server := &Server{
				ctx:        t.Context(),
				jobManager: manager,
				logger:     hclog.New(&hclog.LoggerOptions{Output: logs, JSONFormat: true, Level: hclog.Trace}),
			}
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer()
			proto.RegisterZenBpmServer(grpcServer, server)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(grpcServer.Stop)

			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			stream, err := proto.NewZenBpmClient(conn).JobStream(metadata.AppendToOutgoingContext(t.Context(), jobmanager.MetadataClientID, "client-1"))
			require.NoError(t, err)
			if tt.answeredBefore {
				require.NoError(t, stream.Send(extendLockRequest(1, 0)))
				_, err = stream.Recv()
				require.NoError(t, err)
			}
			require.NoError(t, stream.Send(extendLockRequest(blockedExtensionKey, 0)))
			select {
			case <-manager.extending:
			case <-time.After(5 * time.Second):
				t.Fatal("the lock extension did not reach the job manager")
			}

			// the extension returns once the stream ended, so its answer is
			// sent to a client that has left
			require.NoError(t, conn.Close())

			// the handler may or may not have returned when the answer fails
			sendFailure := func() map[string]any {
				if entry := logs.entryOf(sendFailureMessage); entry != nil {
					return entry
				}
				return logs.entryOf(lateSendFailureMessage)
			}
			require.Eventually(t, func() bool { return sendFailure() != nil }, 5*time.Second, 5*time.Millisecond, "log: %s", logs)
			entry := sendFailure()
			if tt.answeredBefore {
				assert.Contains(t, entry["err"], "code = Unavailable", "the send failed on the closed connection; log: %s", logs)
			}
			assert.Equal(t, "debug", entry["@level"], "log: %s", logs)
			assert.Equal(t, "client-1", entry["clientID"])
			assert.NotContains(t, logs.String(), `"@level":"error"`, "a client leaving logs no error")
		})
	}
}

// TestJobStreamLogsAnAnswerSentAfterTheHandlerReturnedBelowError verifies
// that the handler of a job stream marks its return for the receive loop,
// which it does not wait for. The answer the loop sends afterwards goes to a
// stream gRPC has ended, and the Internal status gRPC then fails the first
// send of a stream with says nothing about this server.
func TestJobStreamLogsAnAnswerSentAfterTheHandlerReturnedBelowError(t *testing.T) {
	logs := &sendErrorTestLog{}
	server := &Server{
		ctx:        t.Context(),
		jobManager: &jobStreamTestManager{},
		logger:     hclog.New(&hclog.LoggerOptions{Output: logs, JSONFormat: true, Level: hclog.Trace}),
	}
	// the client has left: the stream is cancelled, and its request is
	// received only once the handler returned
	left, leave := context.WithCancel(t.Context())
	leave()
	stream := &heldRecvTestStream{jobStreamTestServer: newJobStreamTestServer(extendLockRequest(7, 0)), release: make(chan struct{})}
	stream.ctx = left
	stream.sendErr = status.Error(codes.Internal, "transport: SendHeader called multiple times")

	require.NoError(t, server.JobStream(stream))
	close(stream.release)

	require.Eventually(t, func() bool {
		return logs.entryOf(lateSendFailureMessage) != nil
	}, 5*time.Second, 5*time.Millisecond, "log: %s", logs)
	assert.Equal(t, "debug", logs.levelOf(lateSendFailureMessage), "log: %s", logs)
	assert.NotContains(t, logs.String(), `"@level":"error"`, "a client leaving logs no error")
}

// heldRecvTestStream is a job stream whose requests are received only once
// release is closed.
type heldRecvTestStream struct {
	*jobStreamTestServer
	release chan struct{}
}

func (s *heldRecvTestStream) Recv() (*proto.JobStreamRequest, error) {
	<-s.release
	return s.jobStreamTestServer.Recv()
}

// blockedExtensionKey is the job whose lock extension
// blockingExtendTestManager holds.
const blockedExtensionKey = 7

// blockingExtendTestManager holds the lock extension of blockedExtensionKey
// until the stream it came on has ended, so that its answer is sent to a
// client that has left. Any other extension is answered at once.
type blockingExtendTestManager struct {
	jobStreamTestManager
	extending chan struct{}
}

func (m *blockingExtendTestManager) ExtendJobLockReq(ctx context.Context, _ jobmanager.ClientID, key int64, _ time.Duration, _ *int64) (time.Time, error) {
	if key != blockedExtensionKey {
		return time.Now().Add(time.Minute), nil
	}
	close(m.extending)
	<-ctx.Done()
	return time.Now().Add(time.Minute), nil
}

// capturingJobStreamTestManager hands the client channel of every added
// client to the test.
type capturingJobStreamTestManager struct {
	jobStreamTestManager
	added chan chan jobmanager.Job
}

func (m *capturingJobStreamTestManager) AddClient(_ context.Context, _ jobmanager.ClientID, ch chan jobmanager.Job) error {
	m.added <- ch
	return nil
}

func (m *capturingJobStreamTestManager) RemoveClient(context.Context, jobmanager.ClientID) {}

// sendErrorTestLog collects the JSON log of a server whose handler goroutines
// write concurrently with the test.
type sendErrorTestLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *sendErrorTestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *sendErrorTestLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// levelOf returns the level of the first entry with the message, or "".
func (l *sendErrorTestLog) levelOf(message string) string {
	level, _ := l.entryOf(message)["@level"].(string)
	return level
}

// entryOf returns the first entry with the message, or nil.
func (l *sendErrorTestLog) entryOf(message string) map[string]any {
	l.mu.Lock()
	data := bytes.Clone(l.buf.Bytes())
	l.mu.Unlock()
	for line := range bytes.Lines(data) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) == nil && entry["@message"] == message {
			return entry
		}
	}
	return nil
}
