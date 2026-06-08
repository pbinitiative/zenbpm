package zenclient

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *captureLogger) Error(msg string) {
	l.mu.Lock()
	l.msgs = append(l.msgs, msg)
	l.mu.Unlock()
}

func (l *captureLogger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.msgs))
	copy(out, l.msgs)
	return out
}

type fakeStream struct {
	mu   sync.Mutex
	sent []*proto.JobStreamRequest
}

func (s *fakeStream) record(req *proto.JobStreamRequest) error {
	s.mu.Lock()
	s.sent = append(s.sent, req)
	s.mu.Unlock()
	return nil
}

func (s *fakeStream) failRequests() []*proto.JobFailRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fails []*proto.JobFailRequest
	for _, r := range s.sent {
		if f := r.GetFail(); f != nil {
			fails = append(fails, f)
		}
	}
	return fails
}

func ptrInt64(v int64) *int64 { return &v }

func TestHandleJob_PanicInWorkerFuncFailsJobAndDoesNotCrash(t *testing.T) {
	logger := &captureLogger{}
	stream := &fakeStream{}

	w := &Worker{
		ctx:      context.Background(),
		logger:   logger,
		clientID: "test-client",
		f: func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError) {
			panic("handler boom")
		},
	}

	job := &proto.WaitingJob{Key: ptrInt64(42)}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.handleJob(job, stream.record)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleJob did not return after panic — recover missing")
	}

	fails := stream.failRequests()
	require.Len(t, fails, 1, "panic in handler should produce exactly one fail request")
	require.NotNil(t, fails[0].Message)
	assert.Contains(t, *fails[0].Message, "handler boom")

	msgs := logger.snapshot()
	require.NotEmpty(t, msgs, "expected a logged panic message")
	foundPanic := false
	for _, m := range msgs {
		if strings.Contains(m, "panic") {
			foundPanic = true
		}
	}
	assert.True(t, foundPanic, "expected a logged panic message, got: %v", msgs)
}

func TestHandleJob_SuccessCompletesJob(t *testing.T) {
	logger := &captureLogger{}
	stream := &fakeStream{}

	w := &Worker{
		ctx:      context.Background(),
		logger:   logger,
		clientID: "test-client",
		f: func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError) {
			return map[string]any{"ok": true}, nil
		},
	}
	job := &proto.WaitingJob{Key: ptrInt64(7)}

	w.handleJob(job, stream.record)

	stream.mu.Lock()
	defer stream.mu.Unlock()
	require.Len(t, stream.sent, 1)
	require.NotNil(t, stream.sent[0].GetComplete(), "expected a complete request")
	assert.Equal(t, int64(7), stream.sent[0].GetComplete().GetKey())
}

func TestHandleJob_WorkerErrorWithNilErrFailsJobWithoutPanic(t *testing.T) {
	logger := &captureLogger{}
	stream := &fakeStream{}

	w := &Worker{
		ctx:      context.Background(),
		logger:   logger,
		clientID: "test-client",
		f: func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError) {
			return nil, &WorkerError{ErrorCode: "BUSINESS_ERROR"}
		},
	}
	job := &proto.WaitingJob{Key: ptrInt64(9)}

	require.NotPanics(t, func() {
		w.handleJob(job, stream.record)
	})

	fails := stream.failRequests()
	require.Len(t, fails, 1, "workerErr should produce exactly one fail request")
	require.NotNil(t, fails[0].ErrorCode)
	assert.Equal(t, "BUSINESS_ERROR", *fails[0].ErrorCode)
}
