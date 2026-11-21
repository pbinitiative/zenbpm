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
	l.mu.Lock()
	data := bytes.Clone(l.buf.Bytes())
	l.mu.Unlock()
	for line := range bytes.Lines(data) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) == nil && entry["@message"] == message {
			level, _ := entry["@level"].(string)
			return level
		}
	}
	return ""
}
