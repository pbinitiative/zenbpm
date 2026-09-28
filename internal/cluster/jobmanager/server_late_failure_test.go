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

// TestALateFailureFromAClientWhoseLockLapsedLeavesTheNewHoldersLock shows a
// failure reported by a client after its lock was given to another one spends
// the attempt but keeps the job reserved: the rounds still skip the job while
// its holder works on it, and only the holder's own failure releases it.
func TestALateFailureFromAClientWhoseLockLapsedLeavesTheNewHoldersLock(t *testing.T) {
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

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "late", nil, nil, nil, nil))

	assert.Equal(t, ClientID("client-b"), lockHolder(server, job.Key), "the holder keeps its lock")
	assert.Never(t, func() bool {
		return stream.totalSent() > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "the job stays reserved for its holder")

	require.NoError(t, server.failJob(t.Context(), "client-b", job.Key, "down", nil, nil, nil, nil))

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-c") >= 1
	}, 5*time.Second, 10*time.Millisecond, "the holder's own failure releases the job")
}

// TestAFailureOverRESTLeavesAStreamClientsLock shows the same for a failure
// reported by a client which holds no lock at all.
func TestAFailureOverRESTLeavesAStreamClientsLock(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, _ := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	job := generateJobs(1)[0]
	server.distributedJobsMu.Lock()
	server.distributedJobs[job.Key] = &distributedJob{client: "client-b", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)}
	server.distributedJobsMu.Unlock()

	require.NoError(t, server.failJob(t.Context(), "", job.Key, "down", nil, nil, nil, nil))

	assert.Equal(t, ClientID("client-b"), lockHolder(server, job.Key))
}

// TestALateFailureFromTheSameClientLeavesItsLockAfterItsOwnLapse shows the
// same for the most common deployment, one worker process subscribed to the
// type: its lock lapses and the job comes back to it. The job server cannot
// tell which of the two deliveries the first failure belongs to, so it keeps
// the job reserved until the client reports again or the lock lapses; were
// it released, the next round would hand the job out a third time while the
// client still works on the second delivery.
func TestALateFailureFromTheSameClientLeavesItsLockAfterItsOwnLapse(t *testing.T) {
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
	// the second delivery is locked for long enough to tell a third one apart
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	require.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 2
	}, 5*time.Second, 10*time.Millisecond, "the job comes back once the first lock lapsed")

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "late, from the first delivery", nil, nil, nil, nil))

	assert.Equal(t, ClientID("client-a"), lockHolder(server, job.Key), "the second delivery keeps its lock")
	assert.Never(t, func() bool {
		return stream.sentTo("client-a") > 2
	}, 500*time.Millisecond, 10*time.Millisecond, "no third delivery while the second is locked")

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "from the second delivery", nil, nil, nil, nil))

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 3
	}, 5*time.Second, 10*time.Millisecond, "the next failure releases the job")
}

// TestAFailureBeforeTheJobCameBackReleasesTheNextDeliveryAsUsual shows the
// reservation above costs nothing when the client whose lock lapsed reported
// before the job was handed out again: no report can be the earlier one's.
func TestAFailureBeforeTheJobCameBackReleasesTheNextDeliveryAsUsual(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, _ := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	job := generateJobs(1)[0]
	server.distributedJobsMu.Lock()
	server.lockLapsedLocked(&distributedJob{client: "client-a", jobKey: job.Key, jobType: "test-job"}, time.Now())
	server.distributedJobsMu.Unlock()

	require.NoError(t, server.failJob(t.Context(), "client-a", job.Key, "late", nil, nil, nil, nil))

	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	assert.NotContains(t, server.lapsedLocks, job.Key)
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
			return server.failJob(t.Context(), "client-a", jobKey, "down", nil, nil, nil, nil)
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
	t.Run("a refusal for another reason keeps the lock", func(t *testing.T) {
		refusal := fmt.Errorf("cannot fail: %w", bpmn.ErrInvalidJobRequest)
		server, _ := newTestJobServer(t, &testLoader{mu: &sync.RWMutex{}}, refusingCompleter{refusal: refusal})
		job := generateJobs(1)[0]
		server.distributedJobsMu.Lock()
		server.distributedJobs[job.Key] = &distributedJob{client: "client-a", jobKey: job.Key, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)}
		server.distributedJobsMu.Unlock()

		require.ErrorIs(t, server.failJob(t.Context(), "client-a", job.Key, "down", nil, nil, new(int32(-1)), nil), refusal)

		assert.Equal(t, ClientID("client-a"), lockHolder(server, job.Key))
	})
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

func (c refusingCompleter) JobFailByKey(context.Context, int64, string, *string, map[string]any, *int32, *time.Duration) error {
	return c.refusal
}

func (c refusingCompleter) JobUpdateRetriesByKey(context.Context, int64, int32, *time.Time) error {
	return c.refusal
}
