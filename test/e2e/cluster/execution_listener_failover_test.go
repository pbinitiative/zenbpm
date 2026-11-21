//go:build cluster_e2e

package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartListenersGoOnWhereTheyStoodAfterPartitionLeaderFailover completes
// the first of two start listeners of a task, kills the leader of the
// partition while the second one waits, and shows that the new leader goes on
// from the scope the element instance kept: the completed listener is not run
// again, and what it was completed with reaches the jobs which follow.
func TestStartListenersGoOnWhereTheyStoodAfterPartitionLeaderFailover(t *testing.T) {
	tc, leader := clusterWithTaskListeners(t)
	defer tc.Teardown(t)
	instanceKey := CreateInstanceOnNode(t, leader, GetFirstDefinitionKey(t, leader), map[string]any{"given": "g"})
	completeListenerTaskJob(t, leader, instanceKey, "before-first", map[string]any{"fromBefore": "b1"})
	waitingBeforeTheFailover := jobOfType(t, leader, instanceKey, "before-second")

	tc.KillNode(t, partitionLeaderID(t, tc))
	survivor := tc.RunningNodes()[0]

	waitingAfterTheFailover := jobOfType(t, survivor, instanceKey, "before-second")
	assert.Equal(t, waitingBeforeTheFailover.Key, waitingAfterTheFailover.Key, "the listener which waited is not created again")
	assert.Equal(t, "b1", waitingAfterTheFailover.InputVariables["fromBefore"])
	completeListenerTaskJob(t, survivor, instanceKey, "before-second", nil)
	work := jobOfType(t, survivor, instanceKey, "listened-work")
	assert.Equal(t, "g", work.InputVariables["taskInput"], "the results of the input mappings survive the failover")
	assert.Equal(t, "b1", work.InputVariables["fromBefore"], "what a start listener was completed with before the failover reaches the task's job")
	completeListenerTaskJob(t, survivor, instanceKey, "listened-work", map[string]any{"result": "r"})
	completeListenerTaskJob(t, survivor, instanceKey, "after-first", nil)
	completeListenerTaskJob(t, survivor, instanceKey, "after-second", nil)

	jobOfType(t, survivor, instanceKey, "following-work")
	assertEveryJobOfTheListenedTaskRanOnce(t, survivor, instanceKey)
	variables := variablesOfInstance(t, survivor, instanceKey)
	assert.Equal(t, "r", variables["taskResult"])
	assert.Equal(t, "b1", variables["mappedFromBefore"], "an output mapping reads what the start listener was completed with before the failover")
}

// TestEndListenersGoOnWhereTheyStoodAfterPartitionLeaderFailover completes the
// own job of a task, kills the leader of the partition while the first end
// listener waits, and shows that the new leader goes on from the scope: the
// task's own job is not created again, the end listeners see what it was
// completed with, and what they are completed with reaches the instance.
func TestEndListenersGoOnWhereTheyStoodAfterPartitionLeaderFailover(t *testing.T) {
	tc, leader := clusterWithTaskListeners(t)
	defer tc.Teardown(t)
	instanceKey := CreateInstanceOnNode(t, leader, GetFirstDefinitionKey(t, leader), map[string]any{"given": "g"})
	completeListenerTaskJob(t, leader, instanceKey, "before-first", map[string]any{"fromBefore": "b1"})
	completeListenerTaskJob(t, leader, instanceKey, "before-second", nil)
	completeListenerTaskJob(t, leader, instanceKey, "listened-work", map[string]any{"result": "r"})
	waitingBeforeTheFailover := jobOfType(t, leader, instanceKey, "after-first")

	tc.KillNode(t, partitionLeaderID(t, tc))
	survivor := tc.RunningNodes()[0]

	waitingAfterTheFailover := jobOfType(t, survivor, instanceKey, "after-first")
	assert.Equal(t, waitingBeforeTheFailover.Key, waitingAfterTheFailover.Key, "the listener which waited is not created again")
	assert.Equal(t, "r", waitingAfterTheFailover.InputVariables["result"], "an end listener sees what the task's job was completed with")
	completeListenerTaskJob(t, survivor, instanceKey, "after-first", map[string]any{"audited": true})
	afterSecond := jobOfType(t, survivor, instanceKey, "after-second")
	assert.Equal(t, "b1", afterSecond.InputVariables["fromBefore"], "what a start listener was completed with before the failover reaches the last listener")
	assert.Equal(t, true, afterSecond.InputVariables["audited"], "a later end listener sees what an earlier one was completed with")
	completeListenerTaskJob(t, survivor, instanceKey, "after-second", nil)

	jobOfType(t, survivor, instanceKey, "following-work")
	assertEveryJobOfTheListenedTaskRanOnce(t, survivor, instanceKey)
	variables := variablesOfInstance(t, survivor, instanceKey)
	assert.Equal(t, "r", variables["taskResult"])
	assert.Equal(t, true, variables["audited"], "what an end listener is completed with is written to the instance")
}

// clusterWithTaskListeners starts three nodes with one partition and deploys
// a task with two start listeners and two end listeners around its own job,
// followed by a second task.
func clusterWithTaskListeners(t *testing.T) (*TestCluster, *TestNode) {
	t.Helper()
	tc := NewTestCluster(t, 3)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)
	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "execution_listeners/task-listeners.bpmn")
	return tc, leader
}

// jobsOfInstance reads the jobs of the instance on the node. After a failover
// a read may still be routed to the killed node, so it is repeated until one
// is answered.
func jobsOfInstance(t *testing.T, n *TestNode, instanceKey int64) []zenclient.Job {
	t.Helper()
	var jobs []zenclient.Job
	require.Eventually(t, func() bool {
		response, err := n.RestClient.GetProcessInstanceJobsWithResponse(context.Background(), instanceKey, &zenclient.GetProcessInstanceJobsParams{})
		if err != nil || response.JSON200 == nil {
			return false
		}
		jobs = response.JSON200.Items
		return true
	}, 60*time.Second, 500*time.Millisecond, "the jobs of instance %d must be readable on node %s", instanceKey, n.ID)
	return jobs
}

// jobOfType waits until the instance has an active job of the type, and
// answers it.
func jobOfType(t *testing.T, n *TestNode, instanceKey int64, jobType string) zenclient.Job {
	t.Helper()
	var found zenclient.Job
	require.Eventually(t, func() bool {
		response, err := n.RestClient.GetProcessInstanceJobsWithResponse(context.Background(), instanceKey, &zenclient.GetProcessInstanceJobsParams{})
		if err != nil || response.JSON200 == nil {
			return false
		}
		for _, job := range response.JSON200.Items {
			if job.Type == jobType && job.State == zenclient.JobStateActive {
				found = job
				return true
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "instance %d must wait for its job of type %s", instanceKey, jobType)
	return found
}

// completeListenerTaskJob completes the active job of the type. The
// completion is repeated until the node answers it: one whose answer was lost
// to a failover committed all the same, and is answered as done when repeated.
func completeListenerTaskJob(t *testing.T, n *TestNode, instanceKey int64, jobType string, variables map[string]any) {
	t.Helper()
	job := jobOfType(t, n, instanceKey, jobType)
	body := zenclient.CompleteJobJSONRequestBody{}
	if variables != nil {
		body.Variables = &variables
	}
	require.Eventually(t, func() bool {
		response, err := n.RestClient.CompleteJobWithResponse(context.Background(), job.Key, body)
		return err == nil && response.StatusCode() == http.StatusCreated
	}, 60*time.Second, 500*time.Millisecond, "node %s must complete the job of type %s", n.ID, jobType)
}

func variablesOfInstance(t *testing.T, n *TestNode, instanceKey int64) map[string]any {
	t.Helper()
	var variables map[string]any
	require.Eventually(t, func() bool {
		response, err := n.RestClient.GetProcessInstanceWithResponse(context.Background(), instanceKey)
		if err != nil || response.JSON200 == nil {
			return false
		}
		variables = response.JSON200.Variables
		return true
	}, 60*time.Second, 500*time.Millisecond, "instance %d must be readable on node %s", instanceKey, n.ID)
	return variables
}

// assertEveryJobOfTheListenedTaskRanOnce shows that the task of
// task-listeners.bpmn got one completed job for each of its listeners and for
// its own work, in the order of the lifecycle, and no other.
func assertEveryJobOfTheListenedTaskRanOnce(t *testing.T, n *TestNode, instanceKey int64) {
	t.Helper()
	var ran []string
	for _, job := range jobsOfInstance(t, n, instanceKey) {
		if job.ElementId != "listened-task" {
			continue
		}
		assert.Equal(t, zenclient.JobStateCompleted, job.State, "the job of type %s", job.Type)
		ran = append(ran, job.Type)
	}
	assert.Equal(t, []string{"before-first", "before-second", "listened-work", "after-first", "after-second"}, ran)
}
