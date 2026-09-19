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

var (
	// ErrLockNotHeld is returned by Worker.ExtendLock when the job is not
	// locked for this worker any more: the lock lapsed, the job was completed
	// or failed, or it was never delivered to this worker.
	ErrLockNotHeld = errors.New("job lock is not held")
	// ErrLockHeldByOtherClient is returned by Worker.ExtendLock when the job
	// is currently locked for a different client id.
	ErrLockHeldByOtherClient = errors.New("job lock is held by another client")
)

// subscriptionSettings is what the worker asks the engine for per job type;
// zero means the engine's default.
type subscriptionSettings struct {
	lockDuration  time.Duration
	maxActiveJobs int
}

// SubscriptionOption tunes one job type subscription of a worker.
type SubscriptionOption func(*subscriptionSettings)

// WithLockDuration sets how long a delivered job of the type stays locked for
// this worker. The engine caps it at its configured maximum and reports the
// effective deadline in WaitingJob.LockUntil.
func WithLockDuration(d time.Duration) SubscriptionOption {
	return func(s *subscriptionSettings) {
		s.lockDuration = d
	}
}

// WithMaxActiveJobs sets how many jobs of the type this worker may hold at
// once. The engine caps it at its configured maximum.
func WithMaxActiveJobs(n int) SubscriptionOption {
	return func(s *subscriptionSettings) {
		s.maxActiveJobs = n
	}
}

// WorkerOption configures a worker created by RegisterWorkerWithOptions.
type WorkerOption func(*Worker)

// WithJobType subscribes the worker to a job type with the given settings.
func WithJobType(jobType string, subOpts ...SubscriptionOption) WorkerOption {
	return func(w *Worker) {
		settings := subscriptionSettings{}
		for _, opt := range subOpts {
			opt(&settings)
		}
		w.jobTypes[jobType] = settings
	}
}

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
	jobTypes map[string]subscriptionSettings
	f        WorkerFunc
	ctx      context.Context
	client   proto.ZenBpmClient
	stream   grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse]
	// streamCancel cancels the RPC context of the current stream. It is
	// guarded by sendSlot together with stream and is invoked whenever the
	// stream is replaced or discarded, so abandoned streams are fully torn
	// down and the server releases the client registration immediately.
	streamCancel context.CancelFunc
	// sendSlot is a one-place semaphore guarding stream, streamCancel and
	// every Send on the stream. A channel rather than a mutex, so that a
	// caller with a deadline can stop waiting for its turn (see ExtendLock).
	sendSlot chan struct{}
	logger   Logger
	clientID string

	// lockWaiters holds, per job key, the callers of ExtendLock whose request
	// went out on the current stream and who have no answer yet, in send
	// order. An abandoned call keeps its place until its answer arrives, so
	// that the answer is never handed to a later call for the same key.
	lockWaitersMu sync.Mutex
	lockWaiters   map[int64][]chan lockExtension

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

// RegisterWorker opens a job stream for clientID and subscribes it to jobTypes
// with the engine's default lock duration and active-job cap.
func (c *Grpc) RegisterWorker(ctx context.Context, clientID string, f WorkerFunc, jobTypes ...string) (*Worker, error) {
	opts := make([]WorkerOption, 0, len(jobTypes))
	for _, jobType := range jobTypes {
		opts = append(opts, WithJobType(jobType))
	}
	return c.RegisterWorkerWithOptions(ctx, clientID, f, opts...)
}

// RegisterWorkerWithOptions opens a job stream for clientID and subscribes it
// to the job types named by the options, each with its own lock duration and
// active-job cap.
func (c *Grpc) RegisterWorkerWithOptions(ctx context.Context, clientID string, f WorkerFunc, opts ...WorkerOption) (*Worker, error) {
	worker := &Worker{
		jobTypes:    map[string]subscriptionSettings{},
		f:           f,
		ctx:         ctx,
		client:      c.Client,
		logger:      c.logger,
		clientID:    clientID,
		sendSlot:    make(chan struct{}, 1),
		lockWaiters: map[int64][]chan lockExtension{},
	}
	for _, opt := range opts {
		opt(worker)
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
// stream swap is guarded by sendSlot so that concurrent job handlers calling
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
	for jobType, settings := range w.jobTypes {
		if err := stream.Send(subscriptionRequest(jobType, proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE, settings)); err != nil {
			// Cancel the RPC context so the half-initialized stream is fully
			// closed and the server releases the clientID registration;
			// otherwise the next reconnect attempt would be rejected with a
			// duplicate-client error.
			cancel()
			return fmt.Errorf("failed to subscribe worker to job type %s: %w", jobType, err)
		}
	}
	w.sendSlot <- struct{}{}
	if w.streamCancel != nil {
		// Release the previous (dead) stream's resources and let the server
		// clean up its side of the old stream immediately.
		w.streamCancel()
	}
	// Every pending lock extension went out on the old stream, so its answer
	// never arrives; fail them before the new stream is published, while the
	// slot is held, so no request of the new stream can be failed with them.
	w.failLockWaiters(fmt.Errorf("job stream was reconnected before the engine answered"))
	w.stream = stream
	w.streamCancel = cancel
	w.releaseSend()
	w.connectedAt = time.Now()
	return nil
}

func (w *Worker) performWork() {
	for {
		w.sendSlot <- struct{}{}
		stream := w.stream
		w.releaseSend()
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
	if jobToComplete.LockExtended != nil {
		w.answerLockWaiter(jobToComplete)
		return
	}
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
		if !w.waitBackoff(backoff) {
			return false
		}
		if backoff *= 2; backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
		w.lastBackoff = backoff
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
		if !w.waitBackoff(backoff) {
			return false
		}
		if backoff *= 2; backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
		w.lastBackoff = backoff
	}
}

// waitBackoff sleeps for d or until the worker context is cancelled. It returns false when the worker should stop.
func (w *Worker) waitBackoff(d time.Duration) bool {
	select {
	case <-w.ctx.Done():
		return false
	case <-time.After(d):
		return true
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
	if ctx.Err() != nil {
		w.logInfo(fmt.Sprintf("zenclient: job %d finished after its stream was closed; discarding result, job will be redelivered", job.GetKey()))
		return
	}
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

// lockExtension is the engine's answer to one ExtendLock call.
type lockExtension struct {
	lockUntil time.Time
	err       error
}

// ExtendLock moves the lock deadline of a job this worker holds to now plus d
// (zero: the lock duration of the subscription the job was delivered under)
// and returns the new deadline on the clock of the partition leader. A lock
// which lapsed answers ErrLockNotHeld, one held by another client
// ErrLockHeldByOtherClient. The worker does not renew locks by itself: a
// handler which needs longer than its lock calls this before the deadline.
//
// A cancelled ctx ends the wait, both for the turn to send and for the
// answer; a request already sent is still applied by the engine, only its
// answer is discarded. The Send itself is not interruptible. Pass a ctx with
// a deadline: an engine which predates lock extension answers the request
// with a generic stream error and never with a lock answer, so without a
// deadline the call waits until the worker stops or reconnects.
func (w *Worker) ExtendLock(ctx context.Context, jobKey int64, d time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if err := w.acquireSend(ctx); err != nil {
		return time.Time{}, err
	}
	if w.stream == nil {
		w.releaseSend()
		return time.Time{}, fmt.Errorf("worker stream is not connected")
	}
	// registered and sent while holding the slot, so that the waiter order
	// is the send order, which is the order the engine answers in
	answer := make(chan lockExtension, 1)
	w.lockWaitersMu.Lock()
	w.lockWaiters[jobKey] = append(w.lockWaiters[jobKey], answer)
	w.lockWaitersMu.Unlock()
	err := w.stream.Send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_ExtendLock{
			ExtendLock: &proto.JobExtendLockRequest{
				Key:            new(jobKey),
				LockDurationMs: new(d.Milliseconds()),
			},
		},
	})
	if err != nil {
		// nothing reached the engine, so no answer will come for this waiter
		w.dropLockWaiter(jobKey, answer)
	}
	w.releaseSend()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to request lock extension of job %d: %w", jobKey, err)
	}
	// on cancellation the waiter stays registered: its answer is on the way
	// and must be consumed by this entry, not by the next call for the key
	select {
	case extension := <-answer:
		return extension.lockUntil, extension.err
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-w.ctx.Done():
		return time.Time{}, w.ctx.Err()
	}
}

// acquireSend takes the send slot, giving up when ctx or the worker ends.
func (w *Worker) acquireSend(ctx context.Context) error {
	select {
	case w.sendSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

func (w *Worker) releaseSend() {
	<-w.sendSlot
}

// answerLockWaiter hands the engine's answer to the oldest ExtendLock call
// waiting for the job key. Answers are delivered in request order on one
// stream, so the oldest waiter is the one the answer belongs to.
func (w *Worker) answerLockWaiter(resp *proto.JobStreamResponse) {
	key := resp.LockExtended.GetKey()
	w.lockWaitersMu.Lock()
	waiters := w.lockWaiters[key]
	if len(waiters) == 0 {
		w.lockWaitersMu.Unlock()
		w.logger.Error(fmt.Sprintf("zenclient: received lock extension answer for job %d nobody waits for", key))
		return
	}
	answer := waiters[0]
	if len(waiters) == 1 {
		delete(w.lockWaiters, key)
	} else {
		w.lockWaiters[key] = waiters[1:]
	}
	w.lockWaitersMu.Unlock()
	answer <- lockExtensionFromResponse(resp)
}

func lockExtensionFromResponse(resp *proto.JobStreamResponse) lockExtension {
	if resp.Error == nil {
		return lockExtension{lockUntil: time.UnixMilli(resp.LockExtended.GetLockUntil())}
	}
	switch proto.JobStreamErrorCode(resp.Error.GetCode()) {
	case proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD:
		return lockExtension{err: ErrLockNotHeld}
	case proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_HELD_BY_OTHER_CLIENT:
		return lockExtension{err: ErrLockHeldByOtherClient}
	default:
		return lockExtension{err: fmt.Errorf("lock extension of job %d refused: %s", resp.LockExtended.GetKey(), resp.Error.GetMessage())}
	}
}

// failLockWaiters answers every pending ExtendLock call with err. Every
// waiter channel holds one answer, so this never blocks on an abandoned call.
func (w *Worker) failLockWaiters(err error) {
	w.lockWaitersMu.Lock()
	defer w.lockWaitersMu.Unlock()
	for _, waiters := range w.lockWaiters {
		for _, waiter := range waiters {
			waiter <- lockExtension{err: err}
		}
	}
	w.lockWaiters = map[int64][]chan lockExtension{}
}

// dropLockWaiter forgets a waiter whose request never reached the engine.
func (w *Worker) dropLockWaiter(jobKey int64, answer chan lockExtension) {
	w.lockWaitersMu.Lock()
	defer w.lockWaitersMu.Unlock()
	remaining := make([]chan lockExtension, 0, len(w.lockWaiters[jobKey]))
	for _, waiter := range w.lockWaiters[jobKey] {
		if waiter != answer {
			remaining = append(remaining, waiter)
		}
	}
	if len(remaining) == 0 {
		delete(w.lockWaiters, jobKey)
		return
	}
	w.lockWaiters[jobKey] = remaining
}

func (w *Worker) send(req *proto.JobStreamRequest) error {
	w.sendSlot <- struct{}{}
	defer w.releaseSend()
	if w.stream == nil {
		return fmt.Errorf("worker stream is not connected")
	}
	return w.stream.Send(req)
}

// AddJobSubscription subscribes the running worker to another job type. A
// subscription of a type the worker already has replaces its settings.
func (w *Worker) AddJobSubscription(jobType string, opts ...SubscriptionOption) error {
	w.subMu.Lock()
	defer w.subMu.Unlock()
	settings := subscriptionSettings{}
	for _, opt := range opts {
		opt(&settings)
	}
	w.jobTypes[jobType] = settings
	if err := w.send(subscriptionRequest(jobType, proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE, settings)); err != nil {
		return fmt.Errorf("failed to add worker subscription (it will be replayed on the next reconnect): %w", err)
	}
	return nil
}

func (w *Worker) RemoveJobSubscription(jobType string) error {
	w.subMu.Lock()
	defer w.subMu.Unlock()
	if err := w.send(subscriptionRequest(jobType, proto.StreamSubscriptionRequest_TYPE_UNSUBSCRIBE, subscriptionSettings{})); err != nil {
		return fmt.Errorf("failed to remove worker subscription: %w", err)
	}
	delete(w.jobTypes, jobType)
	return nil
}

func subscriptionRequest(jobType string, typ proto.StreamSubscriptionRequest_Type, settings subscriptionSettings) *proto.JobStreamRequest {
	return &proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType:        new(jobType),
				Type:           new(typ),
				LockDurationMs: new(settings.lockDuration.Milliseconds()),
				MaxActiveJobs:  new(int32(settings.maxActiveJobs)),
			},
		},
	}
}
