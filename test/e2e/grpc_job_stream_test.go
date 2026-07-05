package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcJobStream(t *testing.T) {
	var instance zenclient.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	completeType := fmt.Sprintf("%s-complete", randomID)
	definition, err := deployDefinitionWithJobType(t, "long-task-chain.bpmn", randomID, map[string]string{
		"TestType":         randomID,
		"TestCompleteType": completeType,
	})
	assert.NoError(t, err)

	instances := 10
	startedInstances := make([]int64, 0, instances)
	for range instances {
		instance, err = createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
		startedInstances = append(startedInstances, instance.Key)
	}

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, conn.Close()) }()

	zenClient := zenclient.NewGrpc(conn)

	// Counters are mutated from worker callback goroutines spawned by the gRPC client,
	// so they must be accessed atomically to avoid data races with the assertions below.
	var count atomic.Int64
	var completed atomic.Int64
	start := time.Now()
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		count.Add(1)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	_, err = zenClient.RegisterWorker(t.Context(), completeType, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, completeType, job.GetType())
		completed.Add(1)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, completeType)
	assert.NoError(t, err)

	// Wait until every started instance has completed together with all of its jobs.
	// Checking the instance state (not just the jobs) avoids a false positive in the window between one job completing
	// and the next being created, where all currently persisted jobs would momentarily be completed.
	require.Eventually(t, func() bool {
		for _, instanceKey := range startedInstances {
			inst, err := getProcessInstance(t, instanceKey)
			if err != nil || inst.State != zenclient.ProcessInstanceStateCompleted {
				return false
			}
			jobs, err := getProcessInstanceJobs(t, instanceKey)
			if err != nil {
				return false
			}
			for _, job := range jobs {
				if job.State != public.JobStateCompleted {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "all process instances should have all jobs completed")

	fmt.Println(time.Since(start).String())
	assert.Positive(t, count.Load(), "the task worker should have processed at least one job")
	assert.Positive(t, completed.Load(), "the complete worker should have processed at least one job")
}

func TestGrpcJobStreamFailjob(t *testing.T) {
	var instance zenclient.ProcessInstance
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, map[string]string{
		"TestType": randomID,
	})
	assert.NoError(t, err)
	instance, err = createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	failConn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	zenClient := zenclient.NewGrpc(failConn)

	fmt.Println("registering worker")
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		return nil, &zenclient.WorkerError{
			Err:       fmt.Errorf("job fail test"),
			ErrorCode: "fail-test",
			Variables: nil,
		}
	}, randomID)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)

		if err != nil {
			return false
		}

		if len(jobs) != 1 {
			return false
		}

		return jobs[0].State == public.JobStateFailed
	}, 30*time.Second, 100*time.Millisecond, "job should have failed")

	// now setup for resolve
	require.NoError(t, failConn.Close())

	resolveConn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	defer func() { assert.NoError(t, resolveConn.Close()) }()
	zenClient = zenclient.NewGrpc(resolveConn)

	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, randomID, job.GetType())
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	assert.NoError(t, err)

	completeType := fmt.Sprintf("%s-complete", randomID)
	_, err = zenClient.RegisterWorker(t.Context(), completeType, func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		assert.Equal(t, completeType, job.Type)
		return map[string]any{
			"testVar": 456,
		}, nil
	}, completeType)
	assert.NoError(t, err)

	// Now testing that we can resolve
	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)

	resolveIncident(t, incidents[0].Key)

	assert.Eventually(t, func() bool {
		jobs, err := getProcessInstanceJobs(t, instance.Key)

		if err != nil {
			return false
		}

		if len(jobs) != 1 {
			return false
		}
		return jobs[0].State == public.JobStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "job should have completed")

	assert.Eventually(t, func() bool {
		instance, err = getProcessInstance(t, instance.Key)

		if err != nil {
			return false
		}
		return instance.State == zenclient.ProcessInstanceStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "job should have completed")

}

func TestGrpcJobStreamAddJobSubscription(t *testing.T) {
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	completeType := fmt.Sprintf("%s-complete", randomID)
	definition, err := deployDefinitionWithJobType(t, "long-task-chain.bpmn", randomID, map[string]string{
		"TestType":         randomID,
		"TestCompleteType": completeType,
	})
	require.NoError(t, err)

	instances := 5
	startedInstances := make([]int64, 0, instances)
	for range instances {
		instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
			"testVar": 123,
		})
		require.NoError(t, err)
		require.NotEmpty(t, instance.Key)
		startedInstances = append(startedInstances, instance.Key)
	}

	worker, taskCount, completeCount := registerDualTypeWorker(t, randomID, completeType)

	// Dynamically subscribe the same worker to the complete job type. This exercises
	// AddJobSubscription so a single worker can drive the whole task chain to completion.
	require.NoError(t, worker.AddJobSubscription(completeType))

	require.Eventually(t, func() bool {
		for _, instanceKey := range startedInstances {
			inst, err := getProcessInstance(t, instanceKey)
			if err != nil || inst.State != zenclient.ProcessInstanceStateCompleted {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "all instances should complete once the worker is subscribed to both job types")

	assert.Positive(t, taskCount.Load(), "the worker should have processed at least one task job")
	assert.Positive(t, completeCount.Load(), "the worker should have processed the complete job after AddJobSubscription")
}

func TestGrpcJobStreamRemoveJobSubscription(t *testing.T) {
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	completeType := fmt.Sprintf("%s-complete", randomID)
	definition, err := deployDefinitionWithJobType(t, "long-task-chain.bpmn", randomID, map[string]string{
		"TestType":         randomID,
		"TestCompleteType": completeType,
	})
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{
		"testVar": 123,
	})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)

	worker, taskCount, completeCount := registerDualTypeWorker(t, randomID, completeType)

	// Subscribe to the complete job type and immediately unsubscribe again. The task
	// jobs run long before the final complete job is created, so once the removal is
	// processed the complete job can never be delivered and the instance cannot finish.
	require.NoError(t, worker.AddJobSubscription(completeType))
	require.NoError(t, worker.RemoveJobSubscription(completeType))

	require.Eventually(t, func() bool {
		return taskCount.Load() >= 10
	}, 30*time.Second, 100*time.Millisecond, "all task-type jobs should be processed")

	assert.Never(t, func() bool {
		inst, err := getProcessInstance(t, instance.Key)
		return err == nil && inst.State == zenclient.ProcessInstanceStateCompleted
	}, 3*time.Second, 200*time.Millisecond, "instance must not complete while unsubscribed from the complete job type")
	assert.Zero(t, completeCount.Load(), "no complete job should be delivered after RemoveJobSubscription")

	require.NoError(t, worker.AddJobSubscription(completeType))
	require.Eventually(t, func() bool {
		inst, err := getProcessInstance(t, instance.Key)
		return err == nil && inst.State == zenclient.ProcessInstanceStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "instance should complete after re-subscribing to the complete job type")
	assert.Positive(t, completeCount.Load(), "the complete job should be processed after re-subscribing")
}

func TestGrpcJobStreamReconnectsAfterConnectionLoss(t *testing.T) {
	randomID := fmt.Sprintf("test-process-%d", rand.Int63())
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", randomID, map[string]string{
		"TestType": randomID,
	})
	require.NoError(t, err)

	// Put a TCP proxy between the worker and the gRPC server so the test can
	// sever the connection and force the job stream to break mid-flight.
	proxy := startTCPProxy(t, app.grpcAddr)

	conn, err := grpc.NewClient(proxy.address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	zenClient := zenclient.NewGrpc(conn)

	var processed atomic.Int64
	_, err = zenClient.RegisterWorker(t.Context(), randomID, func(_ context.Context, _ *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		processed.Add(1)
		return map[string]any{"testVar": 456}, nil
	}, randomID)
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{"testVar": 123})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	require.Eventually(t, func() bool {
		inst, err := getProcessInstance(t, instance.Key)
		return err == nil && inst.State == zenclient.ProcessInstanceStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "instance should complete before the connection loss")

	// Sever every active connection: the job stream breaks with a transport
	// error and the worker must reconnect on its own through the same address.
	proxy.dropActiveConnections()

	processedBefore := processed.Load()
	instance2, err := createProcessInstance(t, &definition.ProcessDefinitionKey, map[string]any{"testVar": 123})
	require.NoError(t, err)
	require.NotEmpty(t, instance2.Key)

	require.Eventually(t, func() bool {
		inst, err := getProcessInstance(t, instance2.Key)
		return err == nil && inst.State == zenclient.ProcessInstanceStateCompleted
	}, 30*time.Second, 100*time.Millisecond, "instance should complete after the worker reconnects")
	assert.Greater(t, processed.Load(), processedBefore, "the worker should have processed jobs after reconnecting")
}

func deployDefinitionWithJobType(t testing.TB, filename string, processId string, jobTypeMap map[string]string) (public.CreateProcessDefinition201JSONResponse, error) {
	t.Helper()

	result := public.CreateProcessDefinition201JSONResponse{}
	wd, err := os.Getwd()
	if err != nil {
		return result, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := path.Join(wd, "pkg", "bpmn", "test-cases", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return result, fmt.Errorf("failed to read file: %w", err)
	}
	// this is dumb but works for now
	re, err := regexp.Compile("bpmn:process id=\"\\w*\"")
	assert.NoError(t, err)
	file = re.ReplaceAll(file, fmt.Appendf([]byte{}, "bpmn:process id=\"%s\"", processId))

	for k, v := range jobTypeMap {
		template := `taskDefinition type="%s"`
		oldBytes := fmt.Appendf([]byte{}, template, k)
		newBytes := fmt.Appendf([]byte{}, template, v)
		file = bytes.ReplaceAll(file, oldBytes, newBytes)
	}

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return result, fmt.Errorf("failed to create form file: %w", err)
	}
	_, err = part.Write(file)
	if err != nil {
		return result, fmt.Errorf("failed to write file to multipart form: %w", err)
	}
	err = writer.Close()
	if err != nil {
		return result, fmt.Errorf("failed to close multipart writer: %w", err)
	}
	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	if err != nil {
		return result, fmt.Errorf("failed to deploy process definition: %w", err)
	}
	if resp.StatusCode() >= 400 {
		return result, fmt.Errorf("failed to deploy process definition: %s", string(resp.Body))
	}
	err = json.Unmarshal(resp.Body, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return result, nil
}

// registerDualTypeWorker opens a gRPC connection to the test server, registers a worker
// subscribed to randomID job type whose handler increments taskCount for jobs of type
// randomID and completeCount for jobs of type completeType. The connection is closed
// automatically when the test ends via t.Cleanup.
func registerDualTypeWorker(t testing.TB, randomID, completeType string) (*zenclient.Worker, *atomic.Int64, *atomic.Int64) {
	t.Helper()

	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	zenClient := zenclient.NewGrpc(conn)

	var taskCount atomic.Int64
	var completeCount atomic.Int64
	worker, err := zenClient.RegisterWorker(t.Context(), randomID, func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		switch job.GetType() {
		case completeType:
			completeCount.Add(1)
		case randomID:
			taskCount.Add(1)
		default:
			t.Errorf("unexpected job type: %s", job.GetType())
		}
		return map[string]any{
			"testVar": 456,
		}, nil
	}, randomID)
	require.NoError(t, err)
	require.NotNil(t, worker)
	return worker, &taskCount, &completeCount
}

// tcpProxy is a minimal TCP forwarder used to simulate network failures
// between a gRPC client and the test server. It keeps listening after
// dropActiveConnections, so clients can transparently reconnect through the
// same address.
type tcpProxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

// startTCPProxy starts a TCP proxy forwarding to target and registers cleanup
// via t.Cleanup.
func startTCPProxy(t testing.TB, target string) *tcpProxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &tcpProxy{
		listener: listener,
		target:   target,
		conns:    make(map[net.Conn]struct{}),
	}
	go p.acceptLoop()
	t.Cleanup(func() {
		_ = p.listener.Close()
		p.dropActiveConnections()
	})
	return p
}

func (p *tcpProxy) address() string {
	return p.listener.Addr().String()
}

func (p *tcpProxy) acceptLoop() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			// Listener closed: the accept loop has a deterministic exit path.
			return
		}
		serverConn, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = clientConn.Close()
			continue
		}
		p.track(clientConn)
		p.track(serverConn)
		go p.pipe(clientConn, serverConn)
		go p.pipe(serverConn, clientConn)
	}
}

// pipe copies bytes from src to dst and tears both connections down when
// either direction fails, so the copy goroutines always terminate.
func (p *tcpProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	p.untrack(dst)
	p.untrack(src)
}

func (p *tcpProxy) track(conn net.Conn) {
	p.mu.Lock()
	p.conns[conn] = struct{}{}
	p.mu.Unlock()
}

func (p *tcpProxy) untrack(conn net.Conn) {
	p.mu.Lock()
	delete(p.conns, conn)
	p.mu.Unlock()
	_ = conn.Close()
}

// dropActiveConnections severs every currently proxied connection while the
// proxy keeps accepting new ones, simulating a transient network failure.
func (p *tcpProxy) dropActiveConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, conn)
	}
}
