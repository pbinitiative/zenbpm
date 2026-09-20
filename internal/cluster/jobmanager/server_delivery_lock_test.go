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

// TestServerLockStartsWhenTheSendCompletes shows a send blocked by a slow node
// does not eat into the lock: the leader's deadline is restarted once the
// delivery left, while the deadline reported to the worker is the earlier,
// conservative one taken before the send.
func TestServerLockStartsWhenTheSendCompletes(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	stream.sendGate = make(chan struct{})
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{LockDuration: time.Second})
	job := generateJobs(1)[0]
	loader.addJobs(job)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.sendAttempts() == 1
	}, 5*time.Second, 10*time.Millisecond, "the send must be in progress")
	server.distributedJobsMu.Lock()
	require.Len(t, server.distributedJobs, 1, "the job is reserved while its send is blocked")
	reservedUntil := server.distributedJobs[0].lockUntil
	server.distributedJobsMu.Unlock()

	close(stream.sendGate)

	require.Eventually(t, func() bool {
		return stream.totalSent() == 1
	}, 5*time.Second, 10*time.Millisecond, "the send must complete")
	sendCompletedAt := time.Now()
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	require.Len(t, server.distributedJobs, 1)
	assert.True(t, server.distributedJobs[0].lockUntil.After(reservedUntil),
		"the leader's deadline restarts once the send completed instead of keeping the one taken before it")
	assert.WithinDuration(t, sendCompletedAt.Add(time.Second), server.distributedJobs[0].lockUntil, 200*time.Millisecond,
		"the restarted deadline is the lock duration from the end of the send")
	assert.Equal(t, reservedUntil.UnixMilli(), stream.deliveredLockUntil(),
		"the worker is told the deadline taken before the send, which is never later than the leader's")
}
