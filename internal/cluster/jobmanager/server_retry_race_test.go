package jobmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadedJobIsNotDeliveredAfterAFailureWasCommitted shows a failure which
// commits a backoff while the leader already holds the job in a loaded batch,
// here reported over REST by a client which holds no lock, keeps that batch
// from delivering the job: the snapshot predates the backoff.
func TestLoadedJobIsNotDeliveredAfterAFailureWasCommitted(t *testing.T) {
	server, stream, loader, job, release := startServerHoldingALoadedBatch(t)

	require.NoError(t, server.failJob(t.Context(), "rest-client", job.Key, "down", nil, nil, nil, new(time.Hour), nil))
	release()

	assert.Never(t, func() bool {
		return stream.sentTo("client-1") > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "a job failed into a backoff after it was loaded must not be delivered")
	assert.Empty(t, loaderJobs(loader), "the completer committed the backoff")
}

// TestLoadedJobIsNotDeliveredAfterItsRetriesWereUpdated shows the same for an
// operator moving the job into a backoff.
func TestLoadedJobIsNotDeliveredAfterItsRetriesWereUpdated(t *testing.T) {
	server, stream, _, job, release := startServerHoldingALoadedBatch(t)

	require.NoError(t, server.updateJobRetries(t.Context(), job.Key, 3, new(time.Now().Add(time.Hour))))
	release()

	assert.Never(t, func() bool {
		return stream.sentTo("client-1") > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "a job moved into a backoff after it was loaded must not be delivered")
}

// TestAJobFailedWhileAnotherSendBlocksIsNotDelivered shows the check holds for
// the rest of a batch whose first send is blocked by a slow node.
func TestAJobFailedWhileAnotherSendBlocksIsNotDelivered(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	completer := &testCompleter{loader: loader}
	server, stream := newTestJobServer(t, loader, completer)
	stream.sendGate = make(chan struct{})
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{MaxActiveJobs: 10})
	jobs := generateJobs(2)
	loader.addJobs(jobs...)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	require.Eventually(t, func() bool {
		return stream.sendAttempts() == 1
	}, 5*time.Second, 10*time.Millisecond, "the first send must be in progress")

	require.NoError(t, server.failJob(t.Context(), "rest-client", jobs[1].Key, "down", nil, nil, nil, new(time.Hour), nil))
	close(stream.sendGate)

	require.Eventually(t, func() bool {
		return stream.sentTo("client-1") == 1
	}, 5*time.Second, 10*time.Millisecond, "the job whose send was in progress is delivered")
	assert.Never(t, func() bool {
		return stream.sentTo("client-1") > 1
	}, 500*time.Millisecond, 10*time.Millisecond, "the job failed into a backoff meanwhile must not follow it")
}

// TestAJobSkippedForAMutationIsDeliveredByALaterRound shows a skipped job is
// not lost: once the mutation is over, a later round loads it again when it is
// still deliverable, as after a failure which asks for no backoff.
func TestAJobSkippedForAMutationIsDeliveredByALaterRound(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	loaded, release := holdFirstLoad(t, loader)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	awaitLoad(t, loaded)

	require.NoError(t, server.failJob(t.Context(), "rest-client", job.Key, "down", nil, nil, nil, new(time.Duration(0)), nil))
	release()

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-1") == 1
	}, 5*time.Second, 10*time.Millisecond, "the job must be delivered by a later round")
}

// TestFinishedChangesAreForgottenWithoutSubscribers shows the record of a
// finished change does not outlive the round it was made in: a leader whose
// jobs are completed over REST alone never loads a batch, and must not
// remember every job it ever completed.
func TestFinishedChangesAreForgottenWithoutSubscribers(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, _ := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	for _, job := range generateJobs(100) {
		require.NoError(t, server.completeJob(t.Context(), "rest-client", job.Key, nil))
	}

	assert.Eventually(t, func() bool {
		return changedJobs(server) == 0
	}, 5*time.Second, 10*time.Millisecond, "a round without a batch to hand out forgets the changes which ended")
}

func changedJobs(server *jobServer) int {
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	return len(server.mutations.changedAt)
}

// startServerHoldingALoadedBatch starts a server with one job and a client,
// and returns once the distribution loop has read the job from the loader but
// before the batch is handed back; release lets the loop continue.
func startServerHoldingALoadedBatch(t *testing.T) (*jobServer, *captureStream, *testLoader, sql.Job, func()) {
	t.Helper()
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	completer := &testCompleter{loader: loader}
	server, stream := newTestJobServer(t, loader, completer)
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	loaded, release := holdFirstLoad(t, loader)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	server.startServer(ctx)
	awaitLoad(t, loaded)
	return server, stream, loader, job, release
}

// holdFirstLoad makes the first load which reads a job wait, after reading,
// until release is called. release may be called more than once and is called
// when the test ends, and the held load gives up when the test's context ends,
// so a failed assertion leaves no loop blocked.
func holdFirstLoad(t *testing.T, loader *testLoader) (loaded <-chan struct{}, release func()) {
	t.Helper()
	loadedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var first, released sync.Once
	loader.afterLoad = func(jobs []sql.Job) {
		if len(jobs) == 0 {
			return
		}
		held := false
		first.Do(func() { held = true })
		if !held {
			return
		}
		close(loadedCh)
		select {
		case <-releaseCh:
		case <-t.Context().Done():
		}
	}
	release = func() { released.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)
	return loadedCh, release
}

// awaitLoad waits for the held load, and fails the test instead of hanging
// when a regression keeps the loop from loading.
func awaitLoad(t *testing.T, loaded <-chan struct{}) {
	t.Helper()
	select {
	case <-loaded:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the distribution loop did not load the batch")
	}
}

func loaderJobs(loader *testLoader) []sql.Job {
	loader.mu.RLock()
	defer loader.mu.RUnlock()
	return append([]sql.Job(nil), loader.jobsToSend...)
}

// retryAtOnceCompleter accepts every failure and leaves the job deliverable,
// as the engine does for a failure with retries left and no backoff.
type retryAtOnceCompleter struct{}

func (retryAtOnceCompleter) JobCompleteByKey(context.Context, int64, map[string]any) error {
	return nil
}

func (retryAtOnceCompleter) JobFailByKey(context.Context, int64, string, *string, map[string]any, *int32, *time.Duration, *int32) error {
	return nil
}

func (retryAtOnceCompleter) JobUpdateRetriesByKey(context.Context, int64, int32, *time.Time) error {
	return nil
}
