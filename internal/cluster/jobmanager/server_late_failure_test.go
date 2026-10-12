package jobmanager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAFailureNamingNoDeliveryFromAnotherClientLeavesTheHoldersLock shows a
// failure which names no delivery, reported by a client after the lock was
// given to another one, keeps the job reserved: the rounds still skip the job
// while its holder works on it, and only the holder's own failure releases it.
func TestAFailureNamingNoDeliveryFromAnotherClientLeavesTheHoldersLock(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	server.subscribeClient("node-2", "client-c", "test-job", SubscriptionSettings{})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	server.distributedJobsMu.Lock()
	server.distributedJobs[job.Key] = &distributedJob{
		client:       "client-b",
		jobKey:       job.Key,
		jobType:      "test-job",
		lockUntil:    time.Now().Add(time.Minute),
		lockDuration: time.Minute,
	}
	server.distributedJobsMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "late", nil, nil, nil, nil, nil))

	assert.Equal(t, ClientID("client-b"), lockHolder(server, job.Key), "the holder keeps its lock")
	assert.Never(t, func() bool {
		return stream.totalSent() > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "the job stays reserved for its holder")

	require.NoError(t, server.failJob(t.Context(), "client-b", job.Key, "down", nil, nil, nil, nil, nil))

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-c") >= 1
	}, 5*time.Second, 10*time.Millisecond, "the holder's own failure releases the job")
}

// TestAFailureOverRESTLeavesAStreamClientsLock shows the same for a failure
// reported by a client which holds no lock at all and names no delivery.
func TestAFailureOverRESTLeavesAStreamClientsLock(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, _ := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	job := generateJobs(1)[0]
	server.distributedJobsMu.Lock()
	server.distributedJobs[job.Key] = &distributedJob{client: "client-b", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)}
	server.distributedJobsMu.Unlock()

	require.NoError(t, server.failJob(t.Context(), "", job.Key, "down", nil, nil, nil, nil, nil))

	assert.Equal(t, ClientID("client-b"), lockHolder(server, job.Key))
}

// TestALateFailureOfALapsedDeliveryLeavesTheLockOfTheNextDelivery shows the
// most common deployment, one worker process subscribed to the type: its lock
// lapses and the job comes back to it. The late failure of the first delivery
// names that delivery, so it does not release the lock of the second one: were
// it released, the next round would hand the job out a third time while the
// worker still works on the second delivery.
func TestALateFailureOfALapsedDeliveryLeavesTheLockOfTheNextDelivery(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: 100 * time.Millisecond})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	require.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 1
	}, 5*time.Second, 10*time.Millisecond, "the first delivery")
	lapsed := stream.deliveredToken()
	// the second delivery is locked for long enough to tell a third one apart
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	require.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 2
	}, 5*time.Second, 10*time.Millisecond, "the job comes back once the first lock lapsed")
	running := stream.deliveredToken()
	require.Greater(t, running, lapsed)

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "late, from the first delivery", nil, nil, nil, nil, &lapsed))

	assert.Equal(t, ClientID("client-a"), lockHolder(server, job.Key), "the second delivery keeps its lock")
	assert.Never(t, func() bool {
		return stream.sentTo("client-a") > 2
	}, 500*time.Millisecond, 10*time.Millisecond, "no third delivery while the second is locked")

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "from the second delivery", nil, nil, nil, nil, &running))

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 3
	}, 5*time.Second, 10*time.Millisecond, "the failure of the second delivery releases the job")
}

// TestAFailureReleasesTheLockOfTheDeliveryItNames shows which failures the
// engine accepted end the delivery the lock was handed out for: one naming that
// delivery, whoever reports it, and one naming no delivery reported by the lock
// holder. A failure of another delivery, answered as the repeat of a recorded
// one, leaves the lock, whatever it carries.
func TestAFailureReleasesTheLockOfTheDeliveryItNames(t *testing.T) {
	tests := []struct {
		name          string
		client        ClientID
		errorCode     *string
		deliveryToken *int64
		holder        ClientID
	}{
		{"the locked delivery, by its holder", "client-a", nil, new(int64(3)), ""},
		{"the locked delivery, by a worker sending its commands over REST", "client-b", nil, new(int64(3)), ""},
		{"the locked delivery, over REST without a client id", "", nil, new(int64(3)), ""},
		{"the locked delivery, with an error code", "client-a", new("PAYMENT_REFUSED"), new(int64(3)), ""},
		{"an earlier delivery, by the holder", "client-a", nil, new(int64(2)), "client-a"},
		{"an earlier delivery, with an error code", "client-a", new("PAYMENT_REFUSED"), new(int64(2)), "client-a"},
		{"no delivery, by the holder", "client-a", nil, nil, ""},
		{"no delivery, by another client", "client-b", nil, nil, "client-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestJobServer(t, &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}, &retryAtOnceCompleter{})
			job := generateJobs(1)[0]
			server.distributedJobsMu.Lock()
			server.distributedJobs[job.Key] = &distributedJob{client: "client-a", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute), deliveryToken: 3}
			server.distributedJobsMu.Unlock()

			require.NoError(t, server.failJob(t.Context(), tt.client, job.Key, "down", tt.errorCode, nil, nil, nil, tt.deliveryToken))

			assert.Equal(t, tt.holder, lockHolder(server, job.Key))
		})
	}
}

// TestARefusedReportOfAJobWhichNoLongerWaitsReleasesTheLock shows a completion
// or failure the engine refuses because the job ended meanwhile, say by an
// interrupting boundary event, frees the client's slot at once instead of
// when the lock lapses: nobody works on the job any more.
func TestARefusedReportOfAJobWhichNoLongerWaitsReleasesTheLock(t *testing.T) {
	refusals := map[string]error{
		"terminated": fmt.Errorf("cannot fail: %w", bpmn.ErrJobInTerminalState),
		"deleted":    fmt.Errorf("cannot fail: %w", storage.ErrNotFound),
	}
	reports := map[string]func(server *jobServer, jobKey int64) error{
		"completion": func(server *jobServer, jobKey int64) error {
			return server.completeJob(t.Context(), "client-a", jobKey, nil)
		},
		"failure": func(server *jobServer, jobKey int64) error {
			return server.failJob(t.Context(), "client-a", jobKey, "down", nil, nil, nil, nil, nil)
		},
	}
	for refusalName, refusal := range refusals {
		for reportName, report := range reports {
			t.Run(reportName+" of a "+refusalName+" job", func(t *testing.T) {
				server, _ := newTestJobServer(t, &testLoader{mu: &sync.RWMutex{}}, refusingCompleter{refusal: refusal})
				job := generateJobs(1)[0]
				server.distributedJobsMu.Lock()
				server.distributedJobs[job.Key] = &distributedJob{client: "client-a", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)}
				server.distributedJobsMu.Unlock()

				require.ErrorIs(t, report(server, job.Key), refusal)

				assert.Empty(t, lockHolder(server, job.Key))
			})
		}
	}
	keptBy := map[string]error{
		"an invalid request":    fmt.Errorf("cannot fail: %w", bpmn.ErrInvalidJobRequest),
		"a superseded delivery": fmt.Errorf("cannot fail: %w", bpmn.ErrDeliverySuperseded),
	}
	for name, refusal := range keptBy {
		t.Run("a refusal of "+name+" keeps the lock", func(t *testing.T) {
			server, _ := newTestJobServer(t, &testLoader{mu: &sync.RWMutex{}}, refusingCompleter{refusal: refusal})
			job := generateJobs(1)[0]
			server.distributedJobsMu.Lock()
			server.distributedJobs[job.Key] = &distributedJob{client: "client-a", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute), deliveryToken: 2}
			server.distributedJobsMu.Unlock()

			require.ErrorIs(t, server.failJob(t.Context(), "client-a", job.Key, "down", nil, nil, nil, nil, new(int64(1))), refusal)

			assert.Equal(t, ClientID("client-a"), lockHolder(server, job.Key))
		})
	}
}

func lockHolder(server *jobServer, jobKey int64) ClientID {
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	if job, ok := server.distributedJobs[jobKey]; ok {
		return job.client
	}
	return ""
}

// refusingCompleter refuses every completion and failure with the same error.
type refusingCompleter struct {
	refusal error
}

func (c refusingCompleter) JobCompleteByKey(context.Context, int64, map[string]any) error {
	return c.refusal
}

func (c refusingCompleter) JobFailByKey(context.Context, int64, string, *string, map[string]any, *int32, *time.Duration, *int64) error {
	return c.refusal
}

func (c refusingCompleter) JobUpdateRetriesByKey(context.Context, int64, int32, *time.Time) error {
	return c.refusal
}
