package zenclient

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterWorkerWithOptions_SendsSubscriptionSettings(t *testing.T) {
	stream := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{{stream: stream}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, err := (&Grpc{Client: client}).WithLogger(&captureLogger{}).RegisterWorkerWithOptions(ctx, "test-client",
		func(context.Context, *proto.WaitingJob) (map[string]any, *WorkerError) { return nil, nil },
		WithJobType("slow-type", WithLockDuration(5*time.Minute), WithMaxActiveJobs(2)),
		WithJobType("plain-type"),
	)
	require.NoError(t, err)
	require.NotNil(t, worker)

	subscriptions := map[string]*proto.StreamSubscriptionRequest{}
	for _, req := range stream.sentRequests() {
		if sub := req.GetSubscription(); sub != nil {
			subscriptions[sub.GetJobType()] = sub
		}
	}
	require.Contains(t, subscriptions, "slow-type")
	assert.Equal(t, int64(300000), subscriptions["slow-type"].GetLockDurationMs())
	assert.Equal(t, int32(2), subscriptions["slow-type"].GetMaxActiveJobs())
	require.Contains(t, subscriptions, "plain-type")
	assert.Zero(t, subscriptions["plain-type"].GetLockDurationMs(), "no option means the engine's default")
	assert.Zero(t, subscriptions["plain-type"].GetMaxActiveJobs())
}

func TestRegisterWorker_KeepsSendingDefaultSettings(t *testing.T) {
	stream := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{{stream: stream}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registerHandledWorker(ctx, t, client, &captureLogger{})

	requests := stream.sentRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "test-type", requests[0].GetSubscription().GetJobType())
	assert.Zero(t, requests[0].GetSubscription().GetLockDurationMs())
	assert.Zero(t, requests[0].GetSubscription().GetMaxActiveJobs())
}

func TestExtendLock_ReturnsTheDeadlineTheEngineAnswers(t *testing.T) {
	stream := newFakeBidiStream()
	worker := lockTestWorker(t, stream)
	lockUntil := time.Now().Add(time.Minute).Truncate(time.Millisecond)

	go func() {
		assert.Eventually(t, func() bool { return len(stream.sentRequests()) == 1 }, time.Second, 5*time.Millisecond)
		stream.recvCh <- recvResult{resp: &proto.JobStreamResponse{
			LockExtended: &proto.LockExtended{Key: new(int64(7)), LockUntil: new(lockUntil.UnixMilli())},
		}}
	}()

	got, err := worker.ExtendLock(context.Background(), 7, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, lockUntil, got)
	sent := stream.sentRequests()[0].GetExtendLock()
	require.NotNil(t, sent)
	assert.Equal(t, int64(7), sent.GetKey())
	assert.Equal(t, int64(30000), sent.GetLockDurationMs())
	assert.Zero(t, pendingLockWaiters(worker), "an answered call leaves no waiter behind")
}

func TestExtendLock_MapsRefusalCodesToErrors(t *testing.T) {
	tests := []struct {
		name     string
		code     proto.JobStreamErrorCode
		expected error
	}{
		{"not held", proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD, ErrLockNotHeld},
		{"held by other client", proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_HELD_BY_OTHER_CLIENT, ErrLockHeldByOtherClient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newFakeBidiStream()
			worker := lockTestWorker(t, stream)
			go func() {
				assert.Eventually(t, func() bool { return len(stream.sentRequests()) == 1 }, time.Second, 5*time.Millisecond)
				stream.recvCh <- recvResult{resp: &proto.JobStreamResponse{
					Error:        &proto.ErrorResult{Code: new(uint32(tt.code)), Message: new("refused")},
					LockExtended: &proto.LockExtended{Key: new(int64(7))},
				}}
			}()

			_, err := worker.ExtendLock(context.Background(), 7, 0)

			assert.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestExtendLock_GivesUpWhenTheContextEnds(t *testing.T) {
	stream := newFakeBidiStream()
	worker := lockTestWorker(t, stream)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := worker.ExtendLock(ctx, 7, 0)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, stream.sentRequests(), "a call cancelled before its turn sends nothing")
	assert.Zero(t, pendingLockWaiters(worker), "a call which sent nothing leaves no waiter behind")
}

func TestExtendLock_CancelledCallDoesNotTakeTheNextCallsAnswer(t *testing.T) {
	stream := newFakeBidiStream()
	worker := lockTestWorker(t, stream)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := worker.ExtendLock(firstCtx, 7, 0)
		firstDone <- err
	}()
	require.Eventually(t, func() bool { return len(stream.sentRequests()) == 1 }, time.Second, 5*time.Millisecond)
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	secondDeadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	go func() {
		assert.Eventually(t, func() bool { return len(stream.sentRequests()) == 2 }, time.Second, 5*time.Millisecond)
		// the engine answers in request order: first the abandoned call, then the live one
		stream.recvCh <- recvResult{resp: &proto.JobStreamResponse{
			LockExtended: &proto.LockExtended{Key: new(int64(7)), LockUntil: new(int64(1000))},
		}}
		stream.recvCh <- recvResult{resp: &proto.JobStreamResponse{
			LockExtended: &proto.LockExtended{Key: new(int64(7)), LockUntil: new(secondDeadline.UnixMilli())},
		}}
	}()

	got, err := worker.ExtendLock(context.Background(), 7, 0)

	require.NoError(t, err)
	assert.Equal(t, secondDeadline, got, "the second call gets its own answer, not the abandoned call's")
	assert.Eventually(t, func() bool { return pendingLockWaiters(worker) == 0 }, time.Second, 5*time.Millisecond)
}

func TestExtendLock_CancelledWhileWaitingForItsTurnReturnsPromptly(t *testing.T) {
	stream := newFakeBidiStream()
	worker := lockTestWorker(t, stream)
	// another sender holds the slot, as a blocked completion send would
	worker.sendSlot <- struct{}{}
	defer worker.releaseSend()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := worker.ExtendLock(ctx, 7, 0)
		result <- err
	}()
	cancel()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("ExtendLock kept waiting for the send slot after its context was cancelled")
	}
	assert.Empty(t, stream.sentRequests())
	assert.Zero(t, pendingLockWaiters(worker))
}

func TestExtendLock_FailsPendingCallWhenTheStreamReconnects(t *testing.T) {
	stream1 := newFakeBidiStream()
	stream2 := newFakeBidiStream()
	client := &fakeZenBpmClient{results: []jobStreamResult{{stream: stream1}, {stream: stream2}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, err := (&Grpc{Client: client}).WithLogger(&captureLogger{}).RegisterWorkerWithOptions(ctx, "test-client",
		func(context.Context, *proto.WaitingJob) (map[string]any, *WorkerError) { return nil, nil })
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := worker.ExtendLock(context.Background(), 7, 0)
		result <- err
	}()
	require.Eventually(t, func() bool { return len(stream1.sentRequests()) == 1 }, time.Second, 5*time.Millisecond)

	stream1.pushError(fmt.Errorf("transport is closing"))

	select {
	case err := <-result:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reconnected")
	case <-time.After(5 * time.Second):
		t.Fatal("ExtendLock kept waiting for an answer the old stream can never deliver")
	}
	assert.Zero(t, pendingLockWaiters(worker))
}

// pendingLockWaiters counts the registered ExtendLock waiters under the lock
// the worker protects them with.
func pendingLockWaiters(w *Worker) int {
	w.lockWaitersMu.Lock()
	defer w.lockWaitersMu.Unlock()
	pending := 0
	for _, waiters := range w.lockWaiters {
		pending += len(waiters)
	}
	return pending
}

// lockTestWorker registers a worker on the given stream and returns it once
// its receive loop runs, so answers pushed into the stream reach ExtendLock.
func lockTestWorker(t *testing.T, stream *fakeBidiStream) *Worker {
	t.Helper()
	client := &fakeZenBpmClient{results: []jobStreamResult{{stream: stream}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	worker, err := (&Grpc{Client: client}).WithLogger(&captureLogger{}).RegisterWorkerWithOptions(ctx, "test-client",
		func(context.Context, *proto.WaitingJob) (map[string]any, *WorkerError) { return nil, nil })
	require.NoError(t, err)
	return worker
}
