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
	reservedUntil := theLockedJob(t, server).lockUntil
	server.distributedJobsMu.Unlock()

	gateOpenedAt := time.Now()
	close(stream.sendGate)

	// the send counter ticks inside the stream before the server restarts the
	// deadline, so the test waits for the restart itself, not for the send
	require.Eventually(t, func() bool {
		return leaderLockUntil(server).After(reservedUntil)
	}, 5*time.Second, 10*time.Millisecond,
		"the leader's deadline must restart once the send completed instead of keeping the one taken before it")
	assert.WithinRange(t, leaderLockUntil(server), gateOpenedAt.Add(time.Second), time.Now().Add(time.Second),
		"the restarted deadline is the lock duration from the end of the send")
	assert.Equal(t, reservedUntil.UnixMilli(), stream.deliveredLockUntil(),
		"the worker is told the deadline taken before the send, which is never later than the leader's")
}

// leaderLockUntil is the deadline of the single distributed job.
func leaderLockUntil(server *jobServer) time.Time {
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	for _, job := range server.distributedJobs {
		return job.lockUntil
	}
	return time.Time{}
}
