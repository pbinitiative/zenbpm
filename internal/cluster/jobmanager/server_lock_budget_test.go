package jobmanager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerNeverLocksMoreJobsThanOneQueryCanExclude shows the leader stops
// delivering once the locked keys, the job types and the limit would exceed
// the parameters one query may carry, whatever the subscriptions ask for,
// and resumes as jobs complete. A query above the limit fails, which would
// stall every delivery of the leader.
func TestServerNeverLocksMoreJobsThanOneQueryCanExclude(t *testing.T) {
	var oversizedQueries atomic.Int32
	const maxQueryParameters = 8
	loader := &testLoader{
		jobsToSend: []sql.Job{},
		mu:         &sync.RWMutex{},
		onLoad: func(jobTypes []string, idsToSkip []int64, _ int64) {
			if len(jobTypes)+len(idsToSkip)+1 > maxQueryParameters {
				oversizedQueries.Add(1)
			}
		},
	}
	completer := &testCompleter{completedJobs: []int64{}, loader: loader}
	server, stream := newTestJobServer(t, loader, completer)
	server.maxQueryParameters = maxQueryParameters
	// the job type and the limit take two parameters, six remain for locked keys
	const lockBudget = maxQueryParameters - 2
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{MaxActiveJobs: 100})
	jobs := generateJobs(20)
	loader.addJobs(jobs...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	require.Eventually(t, func() bool {
		return stream.sentTo("client-1") == lockBudget
	}, 5*time.Second, 10*time.Millisecond, "the leader must deliver up to the query budget")
	assert.Never(t, func() bool {
		return stream.sentTo("client-1") > lockBudget
	}, 300*time.Millisecond, 10*time.Millisecond, "with every slot of the query taken, nothing more may be delivered")

	completed := 0
	locked := lockedJobs(server)
	for _, job := range locked[:3] {
		require.NoError(t, server.completeJob(ctx, "client-1", job.jobKey, nil))
		completed++
	}

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-1") == lockBudget+completed
	}, 5*time.Second, 10*time.Millisecond, "completed jobs free their slot in the query for new deliveries")
	assert.Zero(t, oversizedQueries.Load(), "no query may carry more parameters than SQLite accepts")
}

// TestServerBudgetCountsOnlyTheJobTypesItQueries shows job types a client
// subscribed to and left again do not eat into the query budget: only the
// types a round asks for are parameters of its query.
func TestServerBudgetCountsOnlyTheJobTypesItQueries(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server, stream := newTestJobServer(t, loader, nil)
	server.maxQueryParameters = 8
	for i := range 6 {
		retired := JobType(fmt.Sprintf("retired-job-%d", i))
		server.subscribeClient("node-2", "client-1", retired, SubscriptionSettings{})
		server.unsubscribeClient("client-1", retired)
	}
	assert.Empty(t, server.jobTypes, "a job type without clients is forgotten")
	server.subscribeClient("node-2", "client-1", "test-job", SubscriptionSettings{})
	loader.addJobs(generateJobs(1)...)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.startServer(ctx)

	assert.Eventually(t, func() bool {
		return stream.sentTo("client-1") == 1
	}, 5*time.Second, 10*time.Millisecond, "subscription churn must not stall delivery")
}
