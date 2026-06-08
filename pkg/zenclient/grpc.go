package zenclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	MetadataClientID string = "client_id"
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
	jobTypes []string
	f        WorkerFunc
	ctx      context.Context
	stream   grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse]
	sendMu   sync.Mutex
	logger   Logger
	clientID string
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
	worker := &Worker{
		jobTypes: jobTypes,
		f:        f,
		ctx:      ctx,
		logger:   c.logger,
		clientID: clientID,
	}
	md := metadata.New(map[string]string{
		MetadataClientID: clientID,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)
	stream, err := c.Client.JobStream(ctx)
	if err != nil {
		return worker, fmt.Errorf("failed to open stream: %w", err)
	}
	worker.stream = stream
	for _, jobType := range jobTypes {
		err = worker.AddJobSubscription(jobType)
		if err != nil {
			return nil, fmt.Errorf("failed to subscribe worker to job type %s: %w", jobType, err)
		}
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

func (w *Worker) performWork() {
	for {
		jobToComplete, err := w.stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			w.logger.Error(fmt.Sprintf("Failed to receive message from stream: %s", err))
			// TODO try reconnecting
			return
		}
		if jobToComplete.Error != nil {
			w.logger.Error(fmt.Sprintf("Failed to receive job from stream: %s", jobToComplete.Error.GetMessage()))
			continue
		}
		if jobToComplete.Job == nil {
			w.logger.Error("received job stream response with no job and no error; skipping")
			continue
		}
		go w.handleJob(jobToComplete.Job, w.send)
	}
}

// handleJob executes the user-supplied worker function for a single job and
// reports the result back through send. It recovers from panics in the user
// handler so that a faulty handler cannot crash the client: the panic is
// logged and the job is failed back to the server (graceful degradation).
func (w *Worker) handleJob(job *proto.WaitingJob, send func(*proto.JobStreamRequest) error) {
	if job == nil {
		w.logger.Error("zenclient: handleJob called with nil job; skipping")
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error(fmt.Sprintf("zenclient: panic in worker handler: %v\n%s", r, debug.Stack()))
			// Guard job.Key: the panic may have originated from a nil/invalid
			// job, and dereferencing it here would panic a second time inside
			var jobKey *int64
			if job != nil {
				jobKey = job.Key
			}
			err := send(&proto.JobStreamRequest{
				Request: &proto.JobStreamRequest_Fail{
					Fail: &proto.JobFailRequest{
						Key:     jobKey,
						Message: ptr.To(fmt.Sprintf("handler panicked: %v", r)),
					},
				},
			})
			if err != nil {
				w.logger.Error(fmt.Sprintf("failed to inform server about panicked job: %s", err))
			}
		}
	}()

	vars, workerErr := w.f(w.ctx, job)
	if workerErr != nil {
		errVars, err := json.Marshal(workerErr.Variables)
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to marshal variables from job result: %s", err))
		}

		err = send(&proto.JobStreamRequest{
			Request: &proto.JobStreamRequest_Fail{
				Fail: &proto.JobFailRequest{
					Key:       job.Key,
					Message:   ptr.To(fmt.Sprintf("failed to complete job: %s", workerErr.Error())),
					ErrorCode: &workerErr.ErrorCode,
					Variables: errVars,
				},
			},
		})
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to inform server about failed job: %s", err))
		}
	} else {
		varsMarshaled, err := json.Marshal(vars)
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to marshal variables from job result: %s", err))
		}
		err = send(&proto.JobStreamRequest{
			Request: &proto.JobStreamRequest_Complete{
				Complete: &proto.JobCompleteRequest{
					Key:       job.Key,
					Variables: varsMarshaled,
				},
			},
		})
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to complete job %d: %s", job.Key, err))
		}
	}
}

func (w *Worker) send(req *proto.JobStreamRequest) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.stream.Send(req)
}

func (w *Worker) AddJobSubscription(jobType string) error {
	err := w.send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType: ptr.To(jobType),
				Type:    ptr.To(proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add worker subscription: %w", err)
	}
	return nil
}

func (w *Worker) RemoveJobSubscription(jobType string) error {
	err := w.send(&proto.JobStreamRequest{
		Request: &proto.JobStreamRequest_Subscription{
			Subscription: &proto.StreamSubscriptionRequest{
				JobType: ptr.To(jobType),
				Type:    ptr.To(proto.StreamSubscriptionRequest_TYPE_UNSUBSCRIBE),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add worker subscription: %w", err)
	}
	return nil
}
