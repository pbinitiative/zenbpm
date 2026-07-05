package zenclient

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

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

	job := &proto.WaitingJob{Key: new(int64(42))}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.handleJob(context.Background(), job, stream.record)
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
	job := &proto.WaitingJob{Key: new(int64(7))}

	w.handleJob(context.Background(), job, stream.record)

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
	job := &proto.WaitingJob{Key: new(int64(9))}

	require.NotPanics(t, func() {
		w.handleJob(context.Background(), job, stream.record)
	})

	fails := stream.failRequests()
	require.Len(t, fails, 1, "workerErr should produce exactly one fail request")
	require.NotNil(t, fails[0].ErrorCode)
	assert.Equal(t, "BUSINESS_ERROR", *fails[0].ErrorCode)
}

func TestHandleJob_NilJobIsSkippedWithoutPanic(t *testing.T) {
	logger := &captureLogger{}
	stream := &fakeStream{}

	handlerCalled := false
	w := &Worker{
		ctx:      context.Background(),
		logger:   logger,
		clientID: "test-client",
		f: func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError) {
			handlerCalled = true
			return nil, nil
		},
	}

	require.NotPanics(t, func() {
		w.handleJob(context.Background(), nil, stream.record)
	})

	assert.False(t, handlerCalled, "user handler must not be invoked for a nil job")

	stream.mu.Lock()
	defer stream.mu.Unlock()
	assert.Empty(t, stream.sent, "no stream request should be sent for a nil job")

	msgs := logger.snapshot()
	require.NotEmpty(t, msgs, "nil job should be logged")
	assert.Contains(t, msgs[0], "nil job")
}

func TestRegisterWorker_ReconnectsAfterStreamError(t *testing.T) {
	logger := &captureLogger{}
	// stream1 breaks; stream2 is the reconnected stream that delivers a job.
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
		{stream: stream2},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled, _ := registerHandledWorker(ctx, t, client, logger)

	// Break the first stream: the worker must reconnect instead of exiting.
	stream1.pushError(fmt.Errorf("transport is closing"))

	// After reconnecting, deliver a job on the second stream.
	stream2.pushJob(&proto.WaitingJob{Key: new(int64(101))})

	select {
	case key := <-handled:
		assert.Equal(t, int64(101), key)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process a job after reconnecting")
	}

	require.Eventually(t, func() bool {
		return len(subscriptionJobTypes(stream2)) > 0
	}, 5*time.Second, 10*time.Millisecond, "worker did not re-subscribe after reconnecting")
	assert.Contains(t, subscriptionJobTypes(stream2), "test-type")

	assert.True(t, logContains(logger, "reconnect"), "expected reconnection to be logged, got: %v", logger.snapshot())
}

func TestRegisterWorker_CancelsPreviousStreamContextAfterReconnect(t *testing.T) {
	logger := &captureLogger{}
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
		{stream: stream2},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled, _ := registerHandledWorker(ctx, t, client, logger)

	stream1.pushError(fmt.Errorf("transport is closing"))
	stream2.pushJob(&proto.WaitingJob{Key: new(int64(404))})

	select {
	case key := <-handled:
		assert.Equal(t, int64(404), key)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process a job after reconnecting")
	}

	require.Eventually(t, func() bool {
		return stream1.Context().Err() != nil
	}, 5*time.Second, 10*time.Millisecond, "previous stream context must be cancelled after reconnect")
	assert.NoError(t, stream2.Context().Err(), "current stream context must stay active")
}

func TestRegisterWorker_RetriesWhenReconnectKeepsFailing(t *testing.T) {
	logger := &captureLogger{}
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	// First reconnection attempts fail to open a stream; eventually one succeeds.
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
		{err: fmt.Errorf("connection refused")},
		{err: fmt.Errorf("connection refused")},
		{stream: stream2},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled, _ := registerHandledWorker(ctx, t, client, logger)

	stream1.pushError(fmt.Errorf("transport is closing"))
	stream2.pushJob(&proto.WaitingJob{Key: new(int64(202))})

	select {
	case key := <-handled:
		assert.Equal(t, int64(202), key)
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not recover after repeated reconnection failures")
	}

	// Exactly one stream was opened per JobStream call: the initial stream, two
	// failed attempts, and the successful reconnect. This guards against
	// spawning duplicate workers/streams.
	assert.Equal(t, 4, client.callCount())
	assert.True(t, logContains(logger, "failed to reconnect"), "expected failed reconnection attempts to be logged")
}

func TestRegisterWorker_ReplaysAddedSubscriptionAfterReconnect(t *testing.T) {
	logger := &captureLogger{}
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
		{stream: stream2},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled, worker := registerHandledWorker(ctx, t, client, logger)
	require.NoError(t, worker.AddJobSubscription("dynamic-type"))

	stream1.pushError(fmt.Errorf("transport is closing"))
	stream2.pushJob(&proto.WaitingJob{Key: new(int64(303))})

	select {
	case key := <-handled:
		assert.Equal(t, int64(303), key)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process a job after reconnecting")
	}

	require.Eventually(t, func() bool {
		subs := subscriptionJobTypes(stream2)
		return slices.Contains(subs, "test-type") && slices.Contains(subs, "dynamic-type")
	}, 5*time.Second, 10*time.Millisecond, "worker did not replay dynamically added subscription after reconnecting")
}

func TestRegisterWorker_DoesNotReplayRemovedSubscriptionAfterReconnect(t *testing.T) {
	logger := &captureLogger{}
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
		{stream: stream2},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcClient := (&Grpc{Client: client}).WithLogger(logger)
	worker, err := grpcClient.RegisterWorker(ctx, "test-client", func(_ context.Context, _ *proto.WaitingJob) (map[string]any, *WorkerError) {
		return nil, nil
	}, "test-type", "removed-type")
	require.NoError(t, err)
	require.NotNil(t, worker)
	require.NoError(t, worker.RemoveJobSubscription("removed-type"))

	stream1.pushError(fmt.Errorf("transport is closing"))

	require.Eventually(t, func() bool {
		return client.callCount() == 2 && len(subscriptionJobTypes(stream2)) > 0
	}, 5*time.Second, 10*time.Millisecond, "worker did not reconnect and replay subscriptions")

	subs := subscriptionJobTypes(stream2)
	assert.Contains(t, subs, "test-type")
	assert.NotContains(t, subs, "removed-type")
}

func TestRegisterWorker_StopsOnContextCancellation(t *testing.T) {
	logger := &captureLogger{}
	stream1 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{
		{stream: stream1},
	}}

	grpcClient := (&Grpc{Client: client}).WithLogger(logger)

	ctx, cancel := context.WithCancel(context.Background())
	worker, err := grpcClient.RegisterWorker(ctx, "test-client", func(_ context.Context, _ *proto.WaitingJob) (map[string]any, *WorkerError) {
		return nil, nil
	}, "test-type")
	require.NoError(t, err)
	require.NotNil(t, worker)

	// Cancelling the context and breaking the stream must stop the worker
	// without attempting to reconnect (no further JobStream calls).
	cancel()
	stream1.pushError(fmt.Errorf("context canceled"))

	assert.Never(t, func() bool {
		return client.callCount() > 1
	}, 300*time.Millisecond, 10*time.Millisecond, "worker must not reconnect after context cancellation")
}

// --- test helpers ---

type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *captureLogger) Error(msg string) {
	l.mu.Lock()
	l.msgs = append(l.msgs, msg)
	l.mu.Unlock()
}

// Info implements the optional InfoLogger extension; messages are captured in
// the same slice so assertions can search all log output at once.
func (l *captureLogger) Info(msg string) {
	l.Error(msg)
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

func logContains(l *captureLogger, substr string) bool {
	for _, m := range l.snapshot() {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

func subscriptionJobTypes(s *fakeBidiStream) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var types []string
	for _, r := range s.sent {
		if sub := r.GetSubscription(); sub != nil {
			types = append(types, sub.GetJobType())
		}
	}
	return types
}

// jobStreamResult is a scripted outcome of a single JobStream call: either a
// stream is returned or an error, mimicking connection failures.
type jobStreamResult struct {
	stream *fakeBidiStream
	err    error
}

// fakeZenBpmClient is a test double for proto.ZenBpmClient that hands out
// pre-scripted streams/errors on successive JobStream calls.
type fakeZenBpmClient struct {
	mu      sync.Mutex
	results []jobStreamResult
	calls   int
}

func (c *fakeZenBpmClient) JobStream(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.calls
	c.calls++
	if idx >= len(c.results) {
		return nil, fmt.Errorf("fakeZenBpmClient: no scripted result for call %d", idx)
	}
	res := c.results[idx]
	if res.err != nil {
		return nil, res.err
	}
	res.stream.ctx = ctx
	return res.stream, nil
}

func (c *fakeZenBpmClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// fakeBidiStream implements grpc.BidiStreamingClient for tests. Recv blocks on a
// channel so the test drives exactly when jobs/errors are delivered.
type fakeBidiStream struct {
	ctx    context.Context
	recvCh chan recvResult
	mu     sync.Mutex
	sent   []*proto.JobStreamRequest
}

type recvResult struct {
	resp *proto.JobStreamResponse
	err  error
}

func newFakeBidiStream() *fakeBidiStream {
	return &fakeBidiStream{
		ctx:    context.Background(),
		recvCh: make(chan recvResult, 8),
	}
}

func (s *fakeBidiStream) pushJob(job *proto.WaitingJob) {
	s.recvCh <- recvResult{resp: &proto.JobStreamResponse{Job: job}}
}

func (s *fakeBidiStream) pushError(err error) {
	s.recvCh <- recvResult{err: err}
}

func (s *fakeBidiStream) Send(req *proto.JobStreamRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, req)
	return nil
}

func (s *fakeBidiStream) Recv() (*proto.JobStreamResponse, error) {
	select {
	case r := <-s.recvCh:
		return r.resp, r.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeBidiStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeBidiStream) Trailer() metadata.MD         { return nil }
func (s *fakeBidiStream) CloseSend() error             { return nil }
func (s *fakeBidiStream) Context() context.Context     { return s.ctx }
func (s *fakeBidiStream) SendMsg(_ any) error          { return nil }
func (s *fakeBidiStream) RecvMsg(_ any) error          { return nil }

// registerHandledWorker creates a Grpc client backed by client+logger, registers a worker
// that forwards every received job key to the returned channel, and returns the channel
// together with the registered Worker. Both require assertions are made inline so callers
// can skip error-checking boilerplate.
func registerHandledWorker(ctx context.Context, t *testing.T, client *fakeZenBpmClient, logger *captureLogger) (chan int64, *Worker) {
	t.Helper()
	handled := make(chan int64, 1)
	grpcClient := (&Grpc{Client: client}).WithLogger(logger)
	worker, err := grpcClient.RegisterWorker(ctx, "test-client", func(_ context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError) {
		handled <- job.GetKey()
		return nil, nil
	}, "test-type")
	require.NoError(t, err)
	require.NotNil(t, worker)
	return handled, worker
}
