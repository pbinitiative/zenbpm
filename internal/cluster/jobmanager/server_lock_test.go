package jobmanager

import (
	"context"
	"math"
	"slices"
	"sync"
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
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: 2 * time.Second})
	server.subscribeClient("node-2", "client-2", "test-job", SubscriptionSettings{LockDuration: 2 * time.Second})
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
	assert.WithinRange(t, server.distributedJobs[0].lockUntil, sentAt.Add(2*time.Second), time.Now().Add(2*time.Second),
		"the delivery is locked for the subscription's duration")
	assert.Equal(t, 2*time.Second, server.distributedJobs[0].lockDuration)
	server.distributedJobsMu.Unlock()

	require.Eventually(t, func() bool {
		skipListMu.Lock()
		defer skipListMu.Unlock()
		return len(skipLists) > 0 && slices.Contains(skipLists[len(skipLists)-1], job.Key)
	}, 5*time.Second, 10*time.Millisecond, "a locked job stays in the skip list")

	// let the lock lapse without waiting for it
	server.distributedJobsMu.Lock()
	server.distributedJobs[0].lockUntil = time.Now().Add(-time.Millisecond)
	server.distributedJobsMu.Unlock()

	require.Eventually(t, func() bool {
		return stream.totalSent() == 2
	}, 5*time.Second, 10*time.Millisecond, "a job whose lock lapsed is loadable and delivered again")
	cancel()
	assert.Equal(t, 1, stream.sentTo("client-1"))
	assert.Equal(t, 1, stream.sentTo("client-2"), "the redelivery goes to the next client in the round robin")
}

func TestServerExtendLockMovesTheDeadline(t *testing.T) {
	limits := DefaultLockLimits()
	limits.MaxLockDuration = 10 * time.Second
	server := newJobServer("node-1", nil, nil, limits)
	jobKey := gen.Generate().Int64()
	server.distributedJobs = []distributedJob{{
		client:       "client-1",
		jobKey:       jobKey,
		jobType:      "test-job",
		lockUntil:    time.Now().Add(time.Hour),
		lockDuration: 2 * time.Second,
	}}

	before := time.Now()
	lockUntil, err := server.extendLock("client-1", jobKey, 5*time.Second)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(5*time.Second), time.Now().Add(5*time.Second),
		"the deadline is now plus the requested duration, whatever it was before")
	assert.Equal(t, lockUntil, server.distributedJobs[0].lockUntil, "the answer is what the entry holds")

	before = time.Now()
	lockUntil, err = server.extendLock("client-1", jobKey, 0)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(2*time.Second), time.Now().Add(2*time.Second),
		"no duration means the lock duration the job was delivered with")

	before = time.Now()
	lockUntil, err = server.extendLock("client-1", jobKey, time.Hour)
	require.NoError(t, err)
	assert.WithinRange(t, lockUntil, before.Add(10*time.Second), time.Now().Add(10*time.Second),
		"a duration above the cap is capped")
}

func TestServerExtendLockOfUnknownOrForeignJob(t *testing.T) {
	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	heldKey := gen.Generate().Int64()
	deadline := time.Now().Add(time.Minute)
	server.distributedJobs = []distributedJob{{client: "client-1", jobKey: heldKey, jobType: "test-job", lockUntil: deadline}}

	_, err := server.extendLock("client-1", gen.Generate().Int64(), time.Second)
	assert.ErrorIs(t, err, ErrLockNotHeld)

	_, err = server.extendLock("client-2", heldKey, time.Second)
	assert.ErrorIs(t, err, ErrLockHeldByOtherClient)

	assert.Equal(t, deadline, server.distributedJobs[0].lockUntil, "a refused extension leaves the lock untouched")
	assert.Len(t, server.distributedJobs, 1)
}

func TestServerExtendLockOfLapsedEntryIsRefused(t *testing.T) {
	server := newJobServer("node-1", nil, nil, DefaultLockLimits())
	lapsedKey := gen.Generate().Int64()
	server.distributedJobs = []distributedJob{{client: "client-1", jobKey: lapsedKey, jobType: "test-job", lockUntil: time.Now().Add(-time.Millisecond)}}

	_, err := server.extendLock("client-1", lapsedKey, time.Minute)

	assert.ErrorIs(t, err, ErrLockNotHeld, "the published deadline decides, not the next cleanup round")
	assert.Empty(t, server.distributedJobs, "a lapsed entry is dropped so the job is loadable again")

	server.distributedJobs = []distributedJob{{client: "client-1", jobKey: lapsedKey, jobType: "test-job", lockUntil: time.Now().Add(-time.Millisecond)}}
	_, err = server.extendLock("client-2", lapsedKey, time.Minute)
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
	server.distributedJobs = []distributedJob{{
		client: "client-1", jobKey: gen.Generate().Int64(), jobType: "job-a", lockUntil: time.Now().Add(time.Minute),
	}}
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
	server.distributedJobs = []distributedJob{{client: "client-1", jobKey: 1, jobType: "test-job", lockUntil: deliveredUntil, lockDuration: time.Minute}}
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: 5 * time.Second, MaxActiveJobs: 3})
	assert.Equal(t, SubscriptionSettings{LockDuration: 5 * time.Second, MaxActiveJobs: 3}, server.settings["test-job"]["client-1"])
	assert.Equal(t, deliveredUntil, server.distributedJobs[0].lockUntil, "a job already delivered keeps the deadline it was delivered with")
	assert.Equal(t, time.Minute, server.distributedJobs[0].lockDuration, "and renews with the lock duration it was delivered under")

	server.unsubscribeClient("client-1", "test-job")
	assert.Empty(t, server.settings["test-job"], "unsubscribing drops the settings")
}

func TestServerCompletionByAnotherClientReleasesTheLock(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	completer := &testCompleter{completedJobs: []int64{}, loader: loader}
	server := newJobServer("node-1", loader, completer, DefaultLockLimits())
	jobKey := gen.Generate().Int64()
	server.distributedJobs = []distributedJob{{client: "client-1", jobKey: jobKey, jobType: "test-job", lockUntil: time.Now().Add(time.Minute)}}

	require.NoError(t, server.completeJob(t.Context(), "rest-client", jobKey, nil))

	assert.Contains(t, completer.completedJobs, jobKey, "completion is not bound to the lock holder")
	assert.Empty(t, server.distributedJobs)
}
