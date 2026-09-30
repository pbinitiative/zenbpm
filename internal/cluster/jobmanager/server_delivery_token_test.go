package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEveryDeliveryIsRecordedBeforeItIsSent shows a job is handed out only once
// its delivery token is written, and that every delivery carries a token above
// the one before it, the redelivery after a lapsed lock included: that is what
// tells the failure of one delivery from the failure of the next.
func TestEveryDeliveryIsRecordedBeforeItIsSent(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	sentWhileRecording := atomic.Int64{}
	loader.onRecord = func([]sql.Job) {
		sentWhileRecording.Store(int64(stream.totalSent()))
	}
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: 100 * time.Millisecond})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 1
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), stream.deliveredToken())
	// the redelivery is locked for long enough that no third one follows while the test looks
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	require.Eventually(t, func() bool {
		return stream.sentTo("client-a") >= 2
	}, 5*time.Second, 10*time.Millisecond, "the job comes back once the lock lapsed")

	assert.Equal(t, 2, stream.sentTo("client-a"))
	assert.Equal(t, int64(2), stream.deliveredToken(), "the redelivery carries a higher token")
	assert.Equal(t, int64(2), loaderJobs(loader)[0].DeliveryToken, "the token sent is the one written")
	assert.Equal(t, int64(1), sentWhileRecording.Load(), "the redelivery was written before it was sent")
}

// TestAJobWhoseDeliveryWasNotRecordedIsNotHandedOut shows a reservation whose
// delivery the write did not record is dropped without a send: the job changed
// since it was loaded, as when another leader handed it out meanwhile, or the
// write failed. A later round hands it out once it can be recorded.
func TestAJobWhoseDeliveryWasNotRecordedIsNotHandedOut(t *testing.T) {
	t.Run("another delivery was recorded since the job was loaded", func(t *testing.T) {
		loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
		server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
		recordedElsewhere := sync.Once{}
		loader.afterLoad = func([]sql.Job) {
			recordedElsewhere.Do(func() {
				loader.mu.Lock()
				defer loader.mu.Unlock()
				loader.jobsToSend[0].DeliveryToken++
			})
		}
		server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
		loader.addJobs(generateJobs(1)...)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		server.startServer(ctx)

		require.Eventually(t, func() bool {
			return stream.sentTo("client-a") >= 1
		}, 5*time.Second, 10*time.Millisecond)
		assert.Equal(t, 1, stream.sendAttempts(), "the stale copy was never sent")
		assert.Equal(t, int64(2), stream.deliveredToken(), "the delivery after the one recorded elsewhere")
	})
	t.Run("the write failed", func(t *testing.T) {
		loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
		server, stream := newTestJobServer(t, loader, &retryAtOnceCompleter{})
		loader.refuseRecording(errors.New("not the leader any more"))
		server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
		job := generateJobs(1)[0]
		loader.addJobs(job)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		server.startServer(ctx)

		assert.Never(t, func() bool {
			return stream.sendAttempts() > 0
		}, 300*time.Millisecond, 10*time.Millisecond)
		assert.Empty(t, lockHolder(server, job.Key), "the reservation is dropped")

		loader.refuseRecording(nil)
		require.Eventually(t, func() bool {
			return stream.sentTo("client-a") >= 1
		}, 5*time.Second, 10*time.Millisecond)
		assert.Equal(t, int64(1), stream.deliveredToken())
	})
}

// TestAFailureWaitsForTheDeliveryOfItsJobBeingRecorded shows a failure which
// arrives while the delivery of its job is being written reaches the engine
// only once the write is done, and then sees the token of a delivery which
// reached a worker: the delivery was sent, or the failure withdrew it before
// it was sent and the token went back. Read during the write, the job would
// still carry the token of the delivery before; taken over a token which never
// reached a worker, the failure of the delivery before would be refused.
func TestAFailureWaitsForTheDeliveryOfItsJobBeingRecorded(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	var server *jobServer
	var stream *captureStream
	type engineView struct{ token, sent, sentToken int64 }
	seenByTheEngine := make(chan engineView, 1)
	completer := &observingCompleter{onFail: func(int64) {
		seenByTheEngine <- engineView{
			token:     loaderJobs(loader)[0].DeliveryToken,
			sent:      int64(stream.sentTo("client-a")),
			sentToken: stream.deliveredToken(),
		}
	}}
	server, stream = newTestJobServer(t, loader, completer)
	recording := make(chan struct{})
	written := make(chan struct{})
	recordingStarted := sync.Once{}
	loader.onRecord = func([]sql.Job) {
		recordingStarted.Do(func() { close(recording) })
		<-written
	}
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	select {
	case <-recording:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the delivery was not recorded")
	}

	failed := make(chan error, 1)
	go func() {
		failed <- server.failJob(t.Context(), "client-b", job.Key, "late", nil, nil, nil, nil, nil)
	}()
	assert.Never(t, func() bool {
		return len(failed) > 0
	}, 200*time.Millisecond, 10*time.Millisecond, "the failure waits for the write")
	close(written)

	require.NoError(t, <-failed)
	seen := <-seenByTheEngine
	if seen.sent > 0 {
		assert.Equal(t, seen.sentToken, seen.token, "the engine saw the token of the delivery which was sent")
	} else {
		assert.Zero(t, seen.token, "the delivery was withdrawn and its token taken back")
	}
}

// TestAFailureOfTheOnlyDeliveryIsNotRefusedForADeliveryNeverSent shows the
// late failure of a delivery whose lock lapsed, arriving after the next
// delivery of its job was written but before it was sent, here behind the
// blocked send of another job of the round, is judged against the delivery
// which reached a worker: the next one is withdrawn, its token goes back, and
// the failure counts instead of being refused as superseded by a delivery
// nobody received.
func TestAFailureOfTheOnlyDeliveryIsNotRefusedForADeliveryNeverSent(t *testing.T) {
	for name, withdrawalFails := range map[string]bool{"the token goes back": false, "the token cannot go back": true} {
		t.Run(name, func(t *testing.T) {
			loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
			if withdrawalFails {
				loader.withdrawalRefusal = errors.New("not the leader any more")
			}
			server, stream := newTestJobServer(t, loader, &tokenCheckingCompleter{loader: loader})
			stream.sendGate = make(chan struct{})
			server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
			jobs := generateJobs(2)
			jobs[1].DeliveryToken = 1 // its first delivery's lock lapsed, its worker still works on it
			loader.addJobs(jobs...)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			server.startServer(ctx)
			require.Eventually(t, func() bool {
				return stream.sendAttempts() == 1 && loaderJobs(loader)[1].DeliveryToken == 2
			}, 5*time.Second, 10*time.Millisecond, "the second job is written and waits behind the blocked send of the first")

			err := server.failJob(t.Context(), "client-a", jobs[1].Key, "late", nil, nil, new(int32(0)), nil, new(int64(1)))
			close(stream.sendGate)

			if withdrawalFails {
				require.ErrorIs(t, err, bpmn.ErrDeliverySuperseded, "the bounded fallback: refused, and the job is handed out again")
				return
			}
			require.NoError(t, err, "the delivery which reached a worker is the first one")
			assert.Equal(t, int64(1), loaderJobs(loader)[1].DeliveryToken, "the token of the delivery never sent went back")
			require.Eventually(t, func() bool {
				return stream.sentTo("client-a") == 2
			}, 5*time.Second, 10*time.Millisecond, "a later round hands the job out again")
			assert.Equal(t, int64(2), stream.deliveredToken(), "with the token which was never used")
		})
	}
}

// TestAFailureWaitsForTheSendOfItsJob shows a failure which arrives while its
// job is being sent waits for the send, and then sees the token of the
// delivery which was sent.
func TestAFailureWaitsForTheSendOfItsJob(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, &tokenCheckingCompleter{loader: loader})
	stream.sendGate = make(chan struct{})
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	job := generateJobs(1)[0]
	job.DeliveryToken = 1
	loader.addJobs(job)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	require.Eventually(t, func() bool {
		return stream.sendAttempts() == 1
	}, 5*time.Second, 10*time.Millisecond, "the send must be in progress")

	failed := make(chan error, 1)
	go func() {
		failed <- server.failJob(t.Context(), "client-a", job.Key, "late", nil, nil, nil, nil, new(int64(1)))
	}()
	assert.Never(t, func() bool {
		return len(failed) > 0
	}, 200*time.Millisecond, 10*time.Millisecond, "the failure waits for the send")
	close(stream.sendGate)

	require.ErrorIs(t, <-failed, bpmn.ErrDeliverySuperseded, "the next delivery reached a worker")
	assert.Equal(t, int64(2), stream.deliveredToken())
	assert.Equal(t, ClientID("client-a"), lockHolder(server, job.Key), "the running delivery keeps its lock")
}

// TestAFailureWaitingForARecordingWhichPanickedGoesOn shows a write of delivery
// tokens which panics lets the changes waiting for it go on, and drops its
// reservations, instead of holding them until their requests time out.
func TestAFailureWaitingForARecordingWhichPanickedGoesOn(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, _ := newTestJobServer(t, loader, &retryAtOnceCompleter{})
	recording := make(chan struct{})
	written := make(chan struct{})
	loader.onRecord = func([]sql.Job) {
		close(recording)
		<-written
		panic("the write went wrong")
	}
	server.subscribeClient("node-2", "client-a", "test-job", SubscriptionSettings{LockDuration: time.Minute})
	job := generateJobs(1)[0]
	loader.addJobs(job)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)
	select {
	case <-recording:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the delivery was not recorded")
	}

	failed := make(chan error, 1)
	go func() {
		failed <- server.failJob(t.Context(), "client-a", job.Key, "down", nil, nil, nil, nil, nil)
	}()
	close(written)

	select {
	case err := <-failed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the failure still waits for the write which panicked")
	}
	assert.Empty(t, lockHolder(server, job.Key), "the reservation is dropped")
}

// TestAFailureWaitingForARecordingGivesUpWithItsContext shows a failure waiting
// for the delivery of its job to be written stops waiting when its request is
// cancelled, and never reaches the engine.
func TestAFailureWaitingForARecordingGivesUpWithItsContext(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	reachedTheEngine := atomic.Bool{}
	server, _ := newTestJobServer(t, loader, &observingCompleter{onFail: func(int64) { reachedTheEngine.Store(true) }})
	job := generateJobs(1)[0]
	round := &distributionRound{recorded: make(chan struct{})}
	defer close(round.recorded)
	server.distributedJobsMu.Lock()
	server.handingOut[job.Key] = &handOut{round: round, sent: make(chan struct{})}
	server.distributedJobsMu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())

	failed := make(chan error, 1)
	go func() {
		failed <- server.failJob(ctx, "client-a", job.Key, "down", nil, nil, nil, nil, new(int64(1)))
	}()
	cancel()

	require.ErrorIs(t, <-failed, context.Canceled)
	assert.False(t, reachedTheEngine.Load())
}

// observingCompleter accepts everything, and tells onFail about every failure.
type observingCompleter struct {
	retryAtOnceCompleter
	onFail func(jobKey int64)
}

func (c *observingCompleter) JobFailByKey(_ context.Context, jobKey int64, _ string, _ *string, _ map[string]any, _ *int32, _ *time.Duration, _ *int64) error {
	c.onFail(jobKey)
	return nil
}

// tokenCheckingCompleter answers failures naming a delivery as the engine
// does: a token below the job's latest is superseded, any other is recorded.
type tokenCheckingCompleter struct {
	retryAtOnceCompleter
	loader *testLoader
}

func (c *tokenCheckingCompleter) JobFailByKey(_ context.Context, jobKey int64, _ string, _ *string, _ map[string]any, _ *int32, _ *time.Duration, deliveryToken *int64) error {
	for _, job := range loaderJobs(c.loader) {
		if job.Key == jobKey && deliveryToken != nil && *deliveryToken < job.DeliveryToken {
			return fmt.Errorf("job %d: %w", jobKey, bpmn.ErrDeliverySuperseded)
		}
	}
	return nil
}
