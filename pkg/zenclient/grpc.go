package zenclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	MetadataClientID string = "client_id"

	// reconnectInitialBackoff is the delay before the first reconnection
	// attempt after the job stream is interrupted.
	reconnectInitialBackoff = 100 * time.Millisecond
	// reconnectMaxBackoff caps the exponential backoff between reconnection
	// attempts so the worker keeps retrying at a steady, bounded pace.
	reconnectMaxBackoff = 30 * time.Second
	// backoffResetThreshold is how long a reconnected stream has to stay
	// healthy before the backoff is reset to reconnectInitialBackoff. A
	// reconnect can look successful locally (JobStream and Send succeed)
	// while the server rejects the stream asynchronously; without this
	// threshold every such failed cycle would restart the backoff from the
	// initial value and hammer the server in a tight loop.
	backoffResetThreshold = 1 * time.Minute
)

type WorkerFunc func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *WorkerError)

type WorkerError struct {
	Err       error
	ErrorCode string
	Variables map[string]any
}

func (e *WorkerError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("error with code:%s, variables:%v", e.ErrorCode, e.Variables)
	}
	return fmt.Sprintf("error :%v, with code:%s, variables:%v", e.Err, e.ErrorCode, e.Variables)
}

func (e *WorkerError) Unwrap() error { return e.Err }

type Worker struct {
	subMu    sync.Mutex
	jobTypes map[string]struct{}
	f        WorkerFunc
	ctx      context.Context
	client   proto.ZenBpmClient
	stream   grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse]
	// streamCancel cancels the RPC context of the current stream. It is
	// guarded by sendMu together with stream and is invoked whenever the
	// stream is replaced or discarded, so abandoned streams are fully torn
	// down and the server releases the client registration immediately.
	streamCancel context.CancelFunc
	sendMu       sync.Mutex
	logger       Logger
	clientID     string

	// connectedAt and lastBackoff track reconnection backoff state across
	// reconnect cycles. They are only accessed from the performWork
	// goroutine (and once before it starts), so they need no locking.
	connectedAt time.Time
	lastBackoff time.Duration
}

type Grpc struct {
	conn   *grpc.ClientConn
	Client proto.ZenBpmClient
	logger Logger
}

func NewGrpc(conn *grpc.ClientConn) *Grpc {
	client := proto.NewZenBpmClient(conn)
	return &Grpc{
		conn:   conn,
		Client: client,
		logger: &DefLogger{
			logger: slog.Default(),
		},
	}
}

func (c *Grpc) WithLogger(logger Logger) *Grpc {
	c.logger = logger
	return c
}

func (c *Grpc) RegisterWorker(ctx context.Context, clientID string, f WorkerFunc, jobTypes ...string) (*Worker, error) {
	jobTypeSet := make(map[string]struct{}, len(jobTypes))
	for _, jobType := range jobTypes {
		jobTypeSet[jobType] = struct{}{}
	}
	worker := &Worker{
		jobTypes: jobTypeSet,
		f:        f,
		ctx:      ctx,
		client:   c.Client,
		logger:   c.logger,
		clientID: clientID,
	}
	if err := worker.connect(); err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				worker.logger.Error(fmt.Sprintf("zenclient: panic in worker recv loop: %v\n%s", r, debug.Stack()))
			}
		}()
		worker.performWork()
	}()
	return worker, nil
}

// connect opens a fresh job stream for the worker and re-establishes all of its
// job subscriptions. It is used both for the initial registration and for every
// reconnection attempt, so that after a broken stream the worker resumes
// receiving the same job types it was originally subscribed to. Each stream
// gets its own cancellable context derived from the worker context: cancelling
// it fully tears down the RPC (a plain CloseSend would only half-close it),
// which makes the server drop the client registration for w.clientID right
// away instead of waiting for the transport to notice the dead stream. The
// stream swap is guarded by sendMu so that concurrent job handlers calling
// send never race with the replacement of w.stream.
func (w *Worker) connect() error {
	md := metadata.New(map[string]string{
		MetadataClientID: w.clientID,
	})
	streamCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(w.ctx, md))
	stream, err := w.client.JobStream(streamCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to open stream: %w", err)
	}
	w.subMu.Lock()
	defer w.subMu.Unlock()
	for jobType := range w.jobTypes {
		if err := sendSubscriptionToStream(stream, jobType, proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE); err != nil {
			// Cancel the RPC context so the half-initialized stream is fully
			// closed and the server releases the clientID registration;
			// otherwise the next reconnect attempt would be rejected with a
			// duplicate-client error.
			cancel()
			return fmt.Errorf("failed to subscribe worker to job type %s: %w", jobType, err)
		}
	}
	w.sendMu.Lock()
	if w.streamCancel != nil {
		// Release the previous (dead) stream's resources and let the server
		// clean up its side of the old stream immediately.
		w.streamCancel()
	}
	w.stream = stream
	w.streamCancel = cancel
	w.sendMu.Unlock()
	w.connectedAt = time.Now()
	return nil
}

func (w *Worker) performWork() {
	for {
		w.sendMu.Lock()
		stream := w.stream
		w.sendMu.Unlock()
		if stream == nil {
			return
		}
		jobToComplete, err := stream.Recv()
		if err != nil {
			if w.handleRecvError(err) {
				continue
			}
			return
		}
		w.processMessage(stream, jobToComplete)
	}
}

// processMessage handles a single, error-free message received from the stream:
// it logs server-reported job errors, skips empty responses, and dispatches
// real jobs to a handler goroutine.
func (w *Worker) processMessage(stream grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], jobToComplete *proto.JobStreamResponse) {
	if jobToComplete.Error != nil {
		w.logger.Error(fmt.Sprintf("Failed to receive job from stream: %s", jobToComplete.Error.GetMessage()))
		return
	}
	if jobToComplete.Job == nil {
		w.logger.Error("received job stream response with no job and no error; skipping")
		return
	}
	go w.handleJob(stream.Context(), jobToComplete.Job, w.send)
}

// handleRecvError reacts to an error returned by stream.Recv. It returns true
// when the worker successfully reconnected and should keep receiving jobs, and
// false when the worker must stop (its context was cancelled or reconnection was aborted).
func (w *Worker) handleRecvError(err error) bool {
	// The worker was intentionally stopped (context cancelled); do not attempt to reconnect.
	if w.ctx.Err() != nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		w.logger.Error("zenclient: job stream closed by server (EOF); attempting to reconnect")
	} else {
		w.logger.Error(fmt.Sprintf("zenclient: failed to receive message from stream: %s; attempting to reconnect", err))
	}
	// reconnect blocks until the stream is re-established or the worker context is cancelled.
	return w.reconnect()
}

// reconnect repeatedly tries to re-establish the job stream using an exponential
// backoff (capped at reconnectMaxBackoff) until it succeeds or the worker
// context is cancelled. Because it runs on the single performWork goroutine, it
// can never spawn a duplicate worker or overlapping recv loops. It returns true
// once a new stream is established and false when the worker should stop.
//
// A reconnect can appear successful locally while the server rejects the
// stream asynchronously (the failure only surfaces on the next Recv). To avoid
// restarting from reconnectInitialBackoff on every such cycle, the backoff is
// carried over from the previous cycle unless the last stream stayed healthy
// for at least backoffResetThreshold.
func (w *Worker) reconnect() bool {
	backoff := reconnectInitialBackoff
	if w.lastBackoff > 0 && time.Since(w.connectedAt) < backoffResetThreshold {
		backoff = w.lastBackoff
	}
	for attempt := 1; ; attempt++ {
		if w.ctx.Err() != nil {
			return false
		}
		w.logInfo(fmt.Sprintf("zenclient: reconnecting job stream (attempt %d)", attempt))
		err := w.connect()
		if err == nil {
			w.lastBackoff = backoff
			w.logInfo(fmt.Sprintf("zenclient: job stream reconnected after %d attempt(s)", attempt))
			return true
		}
		w.logger.Error(fmt.Sprintf("zenclient: failed to reconnect job stream (attempt %d): %s", attempt, err))
		select {
		case <-w.ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
		w.lastBackoff = backoff
	}
}

// logInfo logs an informational message via the optional InfoLogger extension
// of the configured logger, falling back to Error level for loggers that only
// implement the base Logger interface (backward compatibility).
func (w *Worker) logInfo(msg string) {
	if il, ok := w.logger.(InfoLogger); ok {
		il.Info(msg)
		return
	}
	w.logger.Error(msg)
}

// handleJob executes the user-supplied worker function for a single job and
// reports the result back through send. ctx is the gRPC stream context so the
// handler is cancelled when the stream dies (server disconnect, transport
// failure). It recovers from panics in the user handler so that a faulty
// handler cannot crash the client: the panic is logged and the job is failed
// back to the server (graceful degradation).
func (w *Worker) handleJob(ctx context.Context, job *proto.WaitingJob, send func(*proto.JobStreamRequest) error) {
	if job == nil {
		w.logger.Error("zenclient: handleJob called with nil job; skipping")
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.failPanickedJob(job, r, send)
		}
	}()

	vars, workerErr := w.f(ctx, job)
	if workerErr != nil {
		w.failWorkerJob(job, workerErr, send)
		return
	}
	w.completeWorkerJob(job, vars, send)
}

func (w *Worker) failPanickedJob(job *proto.WaitingJob, recovered any, send func(*proto.JobStreamRequest) error) {
	w.logger.Error(fmt.Sprintf("zenclient: panic in worker handler: %v\n%s", recovered, debug.Stack()))
	if err := send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Fail{
			Fail: &proto.JobFailRequest{
				Key:     job.Key,
				Message: new(fmt.Sprintf("handler panicked: %v", recovered)),
			},
		},
	}); err != nil {
		w.logger.Error(fmt.Sprintf("failed to inform server about panicked job: %s", err))
	}
}

func (w *Worker) failWorkerJob(job *proto.WaitingJob, workerErr *WorkerError, send func(*proto.JobStreamRequest) error) {
	errVars, err := json.Marshal(workerErr.Variables)
	if err != nil {
		w.logger.Error(fmt.Sprintf("failed to marshal variables from job result: %s", err))
	}

	if err = send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Fail{
			Fail: &proto.JobFailRequest{
				Key:       job.Key,
				Message:   new(fmt.Sprintf("failed to complete job: %s", workerErr.Error())),
				ErrorCode: &workerErr.ErrorCode,
				Variables: errVars,
			},
		},
	}); err != nil {
		w.logger.Error(fmt.Sprintf("failed to inform server about failed job: %s", err))
	}
}

func (w *Worker) completeWorkerJob(job *proto.WaitingJob, vars map[string]any, send func(*proto.JobStreamRequest) error) {
	varsMarshaled, err := json.Marshal(vars)
	if err != nil {
		w.logger.Error(fmt.Sprintf("failed to marshal variables from job result: %s", err))
	}
	if err = send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Complete{
			Complete: &proto.JobCompleteRequest{
				Key:       job.Key,
				Variables: varsMarshaled,
			},
		},
	}); err != nil {
		w.logger.Error(fmt.Sprintf("failed to complete job %d: %s", job.Key, err))
	}
}

func (w *Worker) send(req *proto.JobStreamRequest) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if w.stream == nil {
		return fmt.Errorf("worker stream is not connected")
	}
	return w.stream.Send(req)
}

func (w *Worker) AddJobSubscription(jobType string) error {
	w.subMu.Lock()
	defer w.subMu.Unlock()
	if err := w.sendSubscription(jobType, proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE); err != nil {
		return fmt.Errorf("failed to add worker subscription: %w", err)
	}
	w.jobTypes[jobType] = struct{}{}
	return nil
}

func (w *Worker) RemoveJobSubscription(jobType string) error {
	w.subMu.Lock()
	defer w.subMu.Unlock()
	if err := w.sendSubscription(jobType, proto.StreamSubscriptionRequest_TYPE_UNSUBSCRIBE); err != nil {
		return fmt.Errorf("failed to remove worker subscription: %w", err)
	}
	delete(w.jobTypes, jobType)
	return nil
}

func (w *Worker) sendSubscription(jobType string, typ proto.StreamSubscriptionRequest_Type) error {
	return w.send(subscriptionRequest(jobType, typ))
}

func sendSubscriptionToStream(stream grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], jobType string, typ proto.StreamSubscriptionRequest_Type) error {
	return stream.Send(subscriptionRequest(jobType, typ))
}

func subscriptionRequest(jobType string, typ proto.StreamSubscriptionRequest_Type) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType: new(jobType),
				Type:    new(typ),
			},
		},
	}
}
