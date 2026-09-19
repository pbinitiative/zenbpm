package e2e

import (
	"context"
	"fmt"
	"math/rand"
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

// TestJobStreamLockDurationIsRespected subscribes two workers with a one second
// lock; the job goes to one of them, which holds it without completing, and
// reaches the other only once that second has passed.
func TestJobStreamLockDurationIsRespected(t *testing.T) {
	jobType := fmt.Sprintf("lock-duration-%d", rand.Int63())
	zenClient := newLockTestGrpcClient(t)
	receipts := &jobReceipts{}
	for _, clientID := range []string{jobType + "-worker-a", jobType + "-worker-b"} {
		_, err := zenClient.RegisterWorkerWithOptions(t.Context(), clientID, holdJobUntilStreamCloses(clientID, receipts),
			zenclient.WithJobType(jobType, zenclient.WithLockDuration(time.Second)))
		require.NoError(t, err)
	}
	instance := deployAndStartLockTestInstance(t, jobType)

	require.Eventually(t, func() bool {
		return receipts.count() == 2
	}, 10*time.Second, 50*time.Millisecond, "the job must reach the second worker once the lock lapsed")

	first, second := receipts.get(0), receipts.get(1)
	assert.NotEqual(t, first.clientID, second.clientID, "the redelivery goes to the other worker")
	assert.GreaterOrEqual(t, second.at.Sub(first.at), 900*time.Millisecond,
		"the job must not be redelivered before the one second lock of the first delivery lapsed")

	// neither worker ever completes the job, so the test does it through REST
	// (completion is not bound to the lock holder) and leaves no active
	// instance behind in the shared node
	require.NoError(t, completeJob(t, second.jobKey, nil))
	waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)
}

// TestJobStreamLockExtensionPreventsRedelivery holds a job for three seconds
// under a two second lock, renewing the lock every 300 ms, and shows the second
// worker on the type never sees it. The lock is deliberately far longer than
// the renewal cadence: a renewal travels three hops, and a stall on a loaded
// runner must not hand the job to the bystander for a reason unrelated to
// the feature.
func TestJobStreamLockExtensionPreventsRedelivery(t *testing.T) {
	jobType := fmt.Sprintf("lock-extension-%d", rand.Int63())
	zenClient := newLockTestGrpcClient(t)
	var holder atomic.Pointer[zenclient.Worker]
	var holderReceived atomic.Bool
	var extensions atomic.Int64
	var extensionErr atomic.Pointer[error]
	worker, err := zenClient.RegisterWorkerWithOptions(t.Context(), jobType+"-holder",
		func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			holderReceived.Store(true)
			// this handler IS the slow handler under test: it works for three
			// seconds and renews its lock while doing so
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return nil, nil
				case <-time.After(300 * time.Millisecond):
				}
				if _, err := holder.Load().ExtendLock(ctx, job.GetKey(), 0); err != nil {
					extensionErr.Store(&err)
					return nil, &zenclient.WorkerError{Err: err}
				}
				extensions.Add(1)
			}
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(2*time.Second)))
	require.NoError(t, err)
	holder.Store(worker)
	instance := deployAndStartLockTestInstance(t, jobType)
	require.Eventually(t, holderReceived.Load, 10*time.Second, 50*time.Millisecond, "the holder must receive the job")
	// the bystander subscribes only now, so that the first delivery is the
	// holder's and any later one can only be a redelivery
	var bystanderDeliveries atomic.Int64
	_, err = zenClient.RegisterWorkerWithOptions(t.Context(), jobType+"-bystander",
		func(context.Context, *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			bystanderDeliveries.Add(1)
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(2*time.Second)))
	require.NoError(t, err)

	waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)

	if failure := extensionErr.Load(); failure != nil {
		t.Fatalf("lock extension failed: %v", *failure)
	}
	assert.GreaterOrEqual(t, extensions.Load(), int64(5), "the holder renewed its lock throughout the three seconds")
	assert.Zero(t, bystanderDeliveries.Load(), "a renewed lock is never redelivered to another worker")
}

// TestJobStreamMaxActiveJobsPerType caps a worker at one active job of its type
// and shows the next job is delivered only after the previous one completed.
func TestJobStreamMaxActiveJobsPerType(t *testing.T) {
	jobType := fmt.Sprintf("max-active-%d", rand.Int63())
	zenClient := newLockTestGrpcClient(t)
	release := make(chan struct{})
	var received atomic.Int64
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), jobType+"-worker",
		func(ctx context.Context, _ *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			received.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithMaxActiveJobs(1)))
	require.NoError(t, err)
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", jobType, map[string]string{"TestType": jobType})
	require.NoError(t, err)
	instances := make([]zenclient.ProcessInstance, 0, 3)
	for range 3 {
		instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, nil)
		require.NoError(t, err)
		instances = append(instances, instance)
	}

	for expected := int64(1); expected <= 3; expected++ {
		require.Eventually(t, func() bool {
			return received.Load() == expected
		}, 10*time.Second, 50*time.Millisecond, "job %d must be delivered", expected)
		assert.Never(t, func() bool {
			return received.Load() > expected
		}, 500*time.Millisecond, 50*time.Millisecond, "no further job may be delivered while one is held")
		release <- struct{}{}
	}

	for _, instance := range instances {
		waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)
	}
}

// TestWaitingJobCarriesLockUntil shows every delivery reports when its lock
// lapses, five seconds after delivery for a five second subscription.
func TestWaitingJobCarriesLockUntil(t *testing.T) {
	jobType := fmt.Sprintf("lock-until-%d", rand.Int63())
	zenClient := newLockTestGrpcClient(t)
	var lockUntil atomic.Int64
	var receivedAt atomic.Int64
	_, err := zenClient.RegisterWorkerWithOptions(t.Context(), jobType+"-worker",
		func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			receivedAt.Store(time.Now().UnixMilli())
			lockUntil.Store(job.GetLockUntil())
			return nil, nil
		}, zenclient.WithJobType(jobType, zenclient.WithLockDuration(5*time.Second)))
	require.NoError(t, err)
	instance := deployAndStartLockTestInstance(t, jobType)

	waitForProcessInstanceJobByElementId(t, instance.Key, "id", public.JobStateCompleted)

	require.NotZero(t, lockUntil.Load(), "lock_until must be set on the delivered job")
	assert.WithinDuration(t, time.UnixMilli(receivedAt.Load()).Add(5*time.Second), time.UnixMilli(lockUntil.Load()), time.Second)
}

type jobReceipt struct {
	clientID string
	jobKey   int64
	at       time.Time
}

type jobReceipts struct {
	mu       sync.Mutex
	receipts []jobReceipt
}

func (r *jobReceipts) add(clientID string, jobKey int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipts = append(r.receipts, jobReceipt{clientID: clientID, jobKey: jobKey, at: time.Now()})
}

func (r *jobReceipts) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.receipts)
}

func (r *jobReceipts) get(index int) jobReceipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.receipts[index]
}

// holdJobUntilStreamCloses records the delivery and then keeps the job open
// for as long as the worker's stream lives, never completing it.
func holdJobUntilStreamCloses(clientID string, receipts *jobReceipts) zenclient.WorkerFunc {
	return func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
		receipts.add(clientID, job.GetKey())
		<-ctx.Done()
		return nil, nil
	}
}

func newLockTestGrpcClient(t *testing.T) *zenclient.Grpc {
	t.Helper()
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	return zenclient.NewGrpc(conn)
}

// deployAndStartLockTestInstance deploys simple_task.bpmn with its service task
// bound to jobType and starts one instance of it.
func deployAndStartLockTestInstance(t *testing.T, jobType string) zenclient.ProcessInstance {
	t.Helper()
	definition, err := deployDefinitionWithJobType(t, "simple_task.bpmn", jobType, map[string]string{"TestType": jobType})
	require.NoError(t, err)
	instance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	return instance
}
