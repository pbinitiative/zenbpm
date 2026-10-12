package jobmanager

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerReleasesJobAfterSubscriptionLockDuration(t *testing.T) {
	skipListMu := sync.Mutex{}
	skipLists := make([][]int64, 0)
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(_ []string, idsToSkip []int64, _ int64) {
			skipListMu.Lock()
			skipLists = append(skipLists, slices.Clone(idsToSkip))
			skipListMu.Unlock()
		},
	}
	server, stream := newTestJobServer(t, loader, nil)
	// the lock outlasts the test, which lets it lapse by hand: a lock lapsing on
	// its own on a slow runner would fail the check that it holds
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	server.subscribeClient("node-2", "client-2", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	job := generateJobs(1)[0]
	loader.addJobs(job)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentAt := time.Now()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.totalSent() == 1
	}, 5*time.Second, 10*time.Millisecond, "the job must be delivered once")
	server.distributedJobsMu.Lock()
	require.Len(t, server.distributedJobs, 1)
	assert.WithinRange(t, theLockedJob(t, server).lockUntil, sentAt.Add(time.Minute), time.Now().Add(time.Minute),
		"the delivery is locked for the subscription's duration")
	assert.Equal(t, time.Minute, theLockedJob(t, server).lockDuration)
	server.distributedJobsMu.Unlock()

	require.Eventually(t, func() bool {
		skipListMu.Lock()
		defer skipListMu.Unlock()
		return len(skipLists) > 0 && slices.Contains(skipLists[len(skipLists)-1], job.Key)
	}, 5*time.Second, 10*time.Millisecond, "a locked job stays in the skip list")
	assert.Never(t, func() bool {
		return stream.totalSent() > 1
	}, 300*time.Millisecond, 10*time.Millisecond, "no other client gets the job while its lock holds")

	// let the lock lapse without waiting for it
	server.distributedJobsMu.Lock()
	theLockedJob(t, server).lockUntil = time.Now().Add(-time.Millisecond)
	server.distributedJobsMu.Unlock()

	require.Eventually(t, func() bool {
		return stream.totalSent() == 2
	}, 5*time.Second, 10*time.Millisecond, "a job whose lock lapsed is loadable and delivered again")
	cancel()
	assert.Equal(t, 1, stream.sentTo("client-1"))
	assert.Equal(t, 1, stream.sentTo("client-2"), "the redelivery goes to the next client in the round robin")
}

// TestCompetingClientsNeverHoldTheSameJob shows the exclusivity of a delivery
// under contention: seven clients of one job type, each capped at three jobs,
// compete for sixty jobs. While nobody completes a job, every client is handed
// exactly its cap and nothing more. Then they complete the jobs they hold, each
// after a while of work, so that the clients keep running full: every job
// reaches exactly one client, and no client ever holds more than its cap.
func TestCompetingClientsNeverHoldTheSameJob(t *testing.T) {
	const clients, maxActiveJobs, jobCount = 7, 3, 60
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &testCompleter{loader: loader})
	clientIDs := make([]ClientID, clients)
	for i := range clients {
		clientIDs[i] = ClientID(fmt.Sprintf("client-%d", i))
		server.subscribeClient("node-2", clientIDs[i], "test-job",
			SubscriptionSettings{LockDuration: time.Minute, MaxActiveJobs: maxActiveJobs})
	}
	loader.addJobs(generateJobs(jobCount)...)

	var mu sync.Mutex
	deliveries := map[int64][]ClientID{}
	held := map[ClientID]int{}
	mostHeld := 0
	workStarts := make(chan struct{})
	var completions sync.WaitGroup
	completed := atomic.Int64{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream.onSent = func(client ClientID, jobKey int64) {
		mu.Lock()
		deliveries[jobKey] = append(deliveries[jobKey], client)
		held[client]++
		mostHeld = max(mostHeld, held[client])
		mu.Unlock()
		completions.Go(func() {
			// the worker's processing time, which keeps its slots taken
			select {
			case <-workStarts:
			case <-ctx.Done():
				return
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return
			}
			// the slot is given up before the completion, which is the earliest
			// the server may hand the client another job
			mu.Lock()
			held[client]--
			mu.Unlock()
			if err := server.completeJob(ctx, client, jobKey, nil); err == nil {
				completed.Add(1)
			}
		})
	}
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.totalSent() == clients*maxActiveJobs
	}, 5*time.Second, 10*time.Millisecond, "every client is handed its cap")
	assert.Never(t, func() bool {
		return stream.totalSent() > clients*maxActiveJobs
	}, 300*time.Millisecond, 10*time.Millisecond, "a client whose slots are all taken is handed nothing more")
	for _, client := range clientIDs {
		assert.Equal(t, maxActiveJobs, stream.sentTo(client), "%s holds its cap", client)
	}
	close(workStarts)

	require.Eventually(t, func() bool {
		return completed.Load() == jobCount
	}, 10*time.Second, 10*time.Millisecond, "every job is completed")
	cancel()
	completions.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, deliveries, jobCount)
	for jobKey, receivers := range deliveries {
		assert.Len(t, receivers, 1, "job %d reached more than one client: %v", jobKey, receivers)
	}
	assert.Equal(t, maxActiveJobs, mostHeld, "no client holds more than its cap")
}

func TestServerExtendLockMovesTheDeadline(t *testing.T) {
	limits := DefaultLockLimits()
	limits.MaxLockDuration = 10 * time.Second
	server := newJobServer("node-1", nil, nil, limits)
	jobKey := gen.Generate().Int64()
	lockJobs(server, distributedJob{
		client:       "client-1",
		jobKey:       jobKey,
		jobType:      "test-job",
		lockUntil:    time.Now().Add(time.Hour),
		lockDuration: 2 * time.Second,
	})

	before := time.Now()
	lockUntil, err := server.extendLock("client-1", jobKey, 5*time.Second, nil)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(5*time.Second), time.Now().Add(5*time.Second),
		"the deadline is now plus the requested duration, whatever it was before")
	assert.Equal(t, lockUntil, theLockedJob(t, server).lockUntil, "the answer is what the entry holds")

	before = time.Now()
	lockUntil, err = server.extendLock("client-1", jobKey, 0, nil)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(2*time.Second), time.Now().Add(2*time.Second),
		"no duration means the lock duration the job was delivered with")

	before = time.Now()
	lockUntil, err = server.extendLock("client-1", jobKey, time.Hour, nil)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(10*time.Second), time.Now().Add(10*time.Second),
		"a duration above the cap is capped")
}

func TestServerExtendLockOfUnknownOrForeignJob(t *testing.T) {
	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	heldKey := gen.Generate().Int64()
	deadline := time.Now().Add(time.Minute)
	lockJobs(server, distributedJob{client: "client-1", jobKey: heldKey, jobType: "test-job", lockUntil: deadline})

	_, err := server.extendLock("client-1", gen.Generate().Int64(), time.Second, nil)
	assert.ErrorIs(t, err, ErrLockNotHeld)

	_, err = server.extendLock("client-2", heldKey, time.Second, nil)
	assert.ErrorIs(t, err, ErrLockHeldByOtherClient)

	assert.Equal(t, deadline, theLockedJob(t, server).lockUntil, "a refused extension leaves the lock untouched")
	assert.Len(t, server.distributedJobs, 1)
}

// TestServerExtendLockNamingADeliveryExtendsOnlyThatDelivery shows an extension
// which names its delivery is refused as not held for the lock of another
// delivery of the job to the same client, as after the named delivery's lock
// lapsed and the job came back, and for a delivery still being written; an
// extension naming none extends whatever the client holds.
func TestServerExtendLockNamingADeliveryExtendsOnlyThatDelivery(t *testing.T) {
	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	key := gen.Generate().Int64()
	deadline := time.Now().Add(time.Minute)
	lockJobs(server, distributedJob{client: "client-1", jobKey: key, jobType: "test-job", lockUntil: deadline, lockDuration: time.Minute, deliveryToken: 3})

	for _, token := range []int64{2, 0, -1} {
		_, err := server.extendLock("client-1", key, time.Hour, new(token))
		assert.ErrorIs(t, err, ErrLockNotHeld, "token %d", token)
	}
	assert.Equal(t, deadline, theLockedJob(t, server).lockUntil, "a refused extension leaves the lock untouched")

	_, err := server.extendLock("client-1", key, time.Hour, new(int64(3)))
	require.NoError(t, err)
	_, err = server.extendLock("client-1", key, time.Hour, nil)
	require.NoError(t, err)

	being := gen.Generate().Int64()
	lockJobs(server, distributedJob{client: "client-1", jobKey: being, jobType: "test-job", lockUntil: deadline})
	_, err = server.extendLock("client-1", being, time.Hour, new(int64(1)))
	assert.ErrorIs(t, err, ErrLockNotHeld, "a delivery whose token is still being written reached nobody yet")
}

func TestServerExtendLockOfLapsedEntryIsRefused(t *testing.T) {
	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	lapsedKey := gen.Generate().Int64()
	lockJobs(server, distributedJob{client: "client-1", jobKey: lapsedKey, jobType: "test-job", lockUntil: time.Now().Add(-time.Millisecond)})

	_, err := server.extendLock("client-1", lapsedKey, time.Minute, nil)

	assert.ErrorIs(t, err, ErrLockNotHeld, "the published deadline decides, not the next cleanup round")
	assert.Empty(t, server.distributedJobs, "a lapsed entry is dropped so the job is loadable again")

	lockJobs(server, distributedJob{client: "client-1", jobKey: lapsedKey, jobType: "test-job", lockUntil: time.Now().Add(-time.Millisecond)})
	_, err = server.extendLock("client-2", lapsedKey, time.Minute, nil)
	assert.ErrorIs(t, err, ErrLockNotHeld, "a lapsed lock is held by nobody, not by another client")
}

func TestServerLoweredCapBindsTheBatchBeingLoaded(t *testing.T) {
	var server *jobServer
	var stream *captureStream
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(_ []string, _ []int64, _ int64) {
			// the client lowers its cap while the batch is being loaded
			server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{MaxActiveJobs: 1})
		},
	}
	server, stream = newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{MaxActiveJobs: 3})
	loader.addJobs(generateJobs(3)...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.sentTo("client-1") == 1
	}, 5*time.Second, 10*time.Millisecond, "one job fits the lowered cap")
	assert.Never(t, func() bool {
		return stream.sentTo("client-1") > 1
	}, 300*time.Millisecond, 10*time.Millisecond, "the batch loaded under the old cap must not exceed the new one")
}

func TestDurationFromMillisNeverWraps(t *testing.T) {
	assert.Equal(t, 5*time.Second, DurationFromMillis(5000))
	assert.Zero(t, DurationFromMillis(0))
	assert.Zero(t, DurationFromMillis(-1), "a negative count asks for the default like zero does")
	assert.Equal(t, time.Duration(config.MaxLockDurationMillis)*time.Millisecond, DurationFromMillis(config.MaxLockDurationMillis))
	assert.Equal(t, time.Duration(math.MaxInt64), DurationFromMillis(config.MaxLockDurationMillis+1), "one above the boundary saturates")
	assert.Equal(t, time.Duration(math.MaxInt64), DurationFromMillis(math.MaxInt64))

	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	effective := server.effectiveSettings(SubscriptionSettings{LockDuration: DurationFromMillis(math.MaxInt64)})
	assert.Equal(t, 24*time.Hour, effective.LockDuration, "an unrepresentable request is capped, not turned into the default")
}

func TestServerCapacityIsCountedPerJobType(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	server.subscribeClient("node-2", "client-1", "job-a", SubscriptionSettings{MaxActiveJobs: 1})
	server.subscribeClient("node-2", "client-1", "job-b", SubscriptionSettings{MaxActiveJobs: 2})
	// client-1 already holds its only job-a slot
	lockJobs(server, distributedJob{
		client: "client-1", jobKey: gen.Generate().Int64(), jobType: "job-a", lockUntil: time.Now().Add(time.Minute),
	})
	loader.addJobs(generateJobsOfType(1, "job-a")...)
	loader.addJobs(generateJobsOfType(2, "job-b")...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.sentOfType("job-b") == 2
	}, 5*time.Second, 10*time.Millisecond, "the held job-a job must not reduce the job-b capacity")
	assert.Never(t, func() bool {
		return stream.sentOfType("job-a") > 0
	}, 300*time.Millisecond, 10*time.Millisecond, "a second job-a job exceeds the job-a cap")
}

func TestServerDefaultsAndCapsApply(t *testing.T) {
	limits := LockLimits{
		DefaultLockDuration:  30 * time.Second,
		MaxLockDuration:      time.Minute,
		DefaultMaxActiveJobs: 10,
		MaxActiveJobsCap:     20,
	}
	server := newJobServer("node-1", nil, nil, limits)
	server.nodeSubs["node-2"] = &nodeSub{nodeID: "node-2"}

	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{})
	assert.Equal(t, SubscriptionSettings{LockDuration: 30 * time.Second, MaxActiveJobs: 10},
		server.settings["test-job"]["client-1"], "zero values take the defaults")

	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: time.Hour, MaxActiveJobs: 500})
	assert.Equal(t, SubscriptionSettings{LockDuration: time.Minute, MaxActiveJobs: 20},
		server.settings["test-job"]["client-1"], "values above the caps are capped and a resubscription replaces the settings")
	assert.Equal(t, []ClientID{"client-1"}, server.jobTypes["test-job"].clients, "a resubscription does not duplicate the client")

	deliveredUntil := time.Now().Add(time.Minute)
	lockJobs(server, distributedJob{client: "client-1", jobKey: 1, jobType: "test-job", lockUntil: deliveredUntil, lockDuration: time.Minute})
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: 5 * time.Second, MaxActiveJobs: 3})
	assert.Equal(t, SubscriptionSettings{LockDuration: 5 * time.Second, MaxActiveJobs: 3}, server.settings["test-job"]["client-1"])
	assert.Equal(t, deliveredUntil, theLockedJob(t, server).lockUntil, "a job already delivered keeps the deadline it was delivered with")
	assert.Equal(t, time.Minute, theLockedJob(t, server).lockDuration, "and renews with the lock duration it was delivered under")

	server.unsubscribeClient("client-1", "test-job")
	assert.Empty(t, server.settings["test-job"], "unsubscribing drops the settings")
}

func TestServerCompletionByAnotherClientReleasesTheLock(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	completer := &testCompleter{completedJobs: []int64{}, loader: loader}
	server := newJobServer("node-1", loader, completer, DefaultLockLimits())
	jobKey := gen.Generate().Int64()
	lockJobs(server, distributedJob{client: "client-1", jobKey: jobKey, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)})

	require.NoError(t, server.completeJob(t.Context(), "rest-client", jobKey, nil))

	assert.Contains(t, completer.completedJobs, jobKey, "completion is not bound to the lock holder")
	assert.Empty(t, server.distributedJobs)
}
