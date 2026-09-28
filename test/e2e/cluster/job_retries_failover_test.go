//go:build cluster_e2e

package cluster

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestRetryStateSurvivesPartitionLeaderFailover fails a job into a one hour
// backoff, kills the leader of its partition and shows the new leader knows
// the retries, the attempts, the deadline and the failure history, and keeps
// the job from its workers until an operator makes it deliverable.
func TestRetryStateSurvivesPartitionLeaderFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "job_retries/service-task-retries.bpmn")
	instanceKey := CreateInstanceOnNode(t, leader, GetFirstDefinitionKey(t, leader), nil)
	jobKey := activeJobOf(t, leader, instanceKey)

	failed, err := leader.RestClient.FailJobWithResponse(context.Background(), jobKey, zenclient.FailJobJSONRequestBody{
		Message:      new("payment service unavailable"),
		RetryBackoff: new("PT1H"),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, failed.StatusCode(), "body: %s", string(failed.Body))
	before := jobOn(t, leader, jobKey)
	require.NotNil(t, before.RetryAt)

	oldLeaderID := partitionLeaderID(t, tc)
	tc.KillNode(t, oldLeaderID)
	survivor := tc.RunningNodes()[0]

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		response, err := survivor.RestClient.GetJobWithResponse(context.Background(), jobKey)
		if !assert.NoError(c, err) || !assert.NotNil(c, response.JSON200) {
			return
		}
		job := response.JSON200
		assert.Equal(c, new(int32(2)), job.Retries)
		assert.Equal(c, new(int32(1)), job.Attempts)
		assert.Equal(c, new("payment service unavailable"), job.LastFailureMessage)
		if assert.NotNil(c, job.RetryAt) {
			assert.True(c, before.RetryAt.Equal(*job.RetryAt), "the deadline survives the failover")
		}
	}, 60*time.Second, 500*time.Millisecond, "the retry state must be readable after the failover")
	// every read is routed to a follower chosen from the cluster state, which
	// may still name the killed node or one whose partition is not open yet,
	// so one request succeeding says nothing about the next
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		failures, err := survivor.RestClient.GetJobFailuresWithResponse(context.Background(), jobKey, &zenclient.GetJobFailuresParams{})
		if !assert.NoError(c, err) || !assert.NotNil(c, failures.JSON200, "body: %s", string(failures.Body)) {
			return
		}
		assert.Len(c, failures.JSON200.Items, 1, "the failure history survives the failover")
	}, 60*time.Second, 500*time.Millisecond, "the failure history must be readable after the failover")

	var deliveries sync.Map // job key -> attempt of its latest delivery
	conn, err := grpc.NewClient(survivor.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { assert.NoError(t, conn.Close()) }()
	_, err = zenclient.NewGrpc(conn).RegisterWorkerWithOptions(t.Context(), "retry-failover-worker",
		func(_ context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
			deliveries.Store(job.GetKey(), job.GetAttempt())
			return map[string]any{}, nil
		}, zenclient.WithJobType("charge-card"))
	require.NoError(t, err)
	// a job of the same type deliverable at once proves the worker's stream
	// reaches the new leader, or the check below would pass for a stream
	// which is not up yet
	require.Eventually(t, func() bool {
		created, err := survivor.RestClient.CreateProcessInstanceWithResponse(context.Background(), zenclient.CreateProcessInstanceJSONRequestBody{
			ProcessDefinitionKey: new(GetFirstDefinitionKey(t, survivor)),
		})
		return err == nil && created.JSON201 != nil
	}, 60*time.Second, 500*time.Millisecond, "the new leader must start a probe instance")
	require.Eventually(t, func() bool {
		probeDelivered := false
		deliveries.Range(func(key, _ any) bool {
			probeDelivered = key.(int64) != jobKey
			return !probeDelivered
		})
		return probeDelivered
	}, 30*time.Second, 100*time.Millisecond, "the worker must receive jobs from the new leader")
	assert.Never(t, func() bool {
		_, delivered := deliveries.Load(jobKey)
		return delivered
	}, 3*time.Second, 100*time.Millisecond, "the new leader must keep the job waiting out its backoff")

	require.Eventually(t, func() bool {
		response, err := survivor.RestClient.UpdateJobRetriesWithResponse(context.Background(), jobKey, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 2})
		return err == nil && response.StatusCode() == http.StatusNoContent
	}, 60*time.Second, 500*time.Millisecond, "the new leader must accept the retry update")
	require.Eventually(t, func() bool {
		attempt, delivered := deliveries.Load(jobKey)
		return delivered && attempt.(int32) == 2
	}, 30*time.Second, 100*time.Millisecond, "the job is delivered as the second attempt once it is deliverable")
}

// TestOperatorRetriesOfAFailedJobSurvivePartitionLeaderFailover exhausts the
// retries of a job, lets an operator set new ones while its incident is open,
// kills the leader of its partition and shows that resolving the incident on
// the new leader keeps the operator's retries instead of the definition's.
func TestOperatorRetriesOfAFailedJobSurvivePartitionLeaderFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "job_retries/service-task-retries.bpmn")
	instanceKey := CreateInstanceOnNode(t, leader, GetFirstDefinitionKey(t, leader), nil)
	jobKey := activeJobOf(t, leader, instanceKey)

	failed, err := leader.RestClient.FailJobWithResponse(context.Background(), jobKey, zenclient.FailJobJSONRequestBody{
		Message: new("card declined"),
		Retries: new(int32(0)),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, failed.StatusCode(), "body: %s", string(failed.Body))
	updated, err := leader.RestClient.UpdateJobRetriesWithResponse(context.Background(), jobKey, zenclient.UpdateJobRetriesJSONRequestBody{Retries: 5})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, updated.StatusCode(), "body: %s", string(updated.Body))

	tc.KillNode(t, partitionLeaderID(t, tc))
	survivor := tc.RunningNodes()[0]

	// reads and the resolution are routed by a cluster state which may still
	// name the killed node, so each is retried until the new leader answers;
	// a resolution whose answer was lost to the failover committed all the
	// same, which the incident then shows
	require.Eventually(t, func() bool {
		incidents, err := survivor.RestClient.GetIncidentsWithResponse(context.Background(), instanceKey, &zenclient.GetIncidentsParams{})
		if err != nil || incidents.JSON200 == nil || len(incidents.JSON200.Items) != 1 {
			return false
		}
		if incidents.JSON200.Items[0].ResolvedAt != nil {
			return true
		}
		resolved, err := survivor.RestClient.ResolveIncidentWithResponse(context.Background(), incidents.JSON200.Items[0].Key)
		return err == nil && resolved.StatusCode() == http.StatusCreated
	}, 60*time.Second, 500*time.Millisecond, "the new leader must resolve the incident")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		response, err := survivor.RestClient.GetJobWithResponse(context.Background(), jobKey)
		if !assert.NoError(c, err) || !assert.NotNil(c, response.JSON200) {
			return
		}
		assert.Equal(c, zenclient.JobStateActive, response.JSON200.State)
		assert.Equal(c, new(int32(5)), response.JSON200.Retries, "the operator's retries survive the failover")
	}, 60*time.Second, 500*time.Millisecond, "the resolved job must be readable after the failover")
}

func activeJobOf(t *testing.T, n *TestNode, instanceKey int64) int64 {
	t.Helper()
	var jobKey int64
	require.Eventually(t, func() bool {
		response, err := n.RestClient.GetProcessInstanceJobsWithResponse(context.Background(), instanceKey, &zenclient.GetProcessInstanceJobsParams{})
		if err != nil || response.JSON200 == nil || len(response.JSON200.Items) == 0 {
			return false
		}
		jobKey = response.JSON200.Items[0].Key
		return true
	}, 10*time.Second, 100*time.Millisecond, "the instance must create its job")
	return jobKey
}

func jobOn(t *testing.T, n *TestNode, jobKey int64) zenclient.Job {
	t.Helper()
	var job zenclient.Job
	require.Eventually(t, func() bool {
		response, err := n.RestClient.GetJobWithResponse(context.Background(), jobKey)
		if err != nil || response.JSON200 == nil || response.JSON200.RetryAt == nil {
			return false
		}
		job = *response.JSON200
		return true
	}, 10*time.Second, 100*time.Millisecond, "the job must show its backoff")
	return job
}

func partitionLeaderID(t *testing.T, tc *TestCluster) string {
	t.Helper()
	for _, n := range tc.RunningNodes() {
		s, err := getStatus(n)
		if err != nil {
			continue
		}
		for _, p := range s.Partitions {
			if p.LeaderID != "" {
				return p.LeaderID
			}
		}
	}
	require.FailNow(t, "no partition leader found")
	return ""
}
