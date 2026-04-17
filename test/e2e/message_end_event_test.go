package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestMessageEndEvent(t *testing.T) {
	var instance zenclient.ProcessInstance
	var jobToComplete zenclient.Job
	definition, err := deployGetDefinition(t, "message_end_event.bpmn", "message_end_event")

	t.Run("create process instance with message end event", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{
			"instVar": "instVarValue",
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	t.Run("the process instance state should be ACTIVE and token state should be WAITING", func(t *testing.T) {
		fetchedProcessInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedProcessInstance.State)
		assert.Equal(t, 1, len(fetchedProcessInstance.ActiveElementInstances))
		assert.Equal(t, "TokenStateWaiting", fetchedProcessInstance.ActiveElementInstances[0].State)
		assert.Equal(t, map[string]any{"instVar": "instVarValue"}, fetchedProcessInstance.Variables)
	})

	t.Run("there should exist corresponding waiting job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "message-end-event-task-1")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, instance.Key, jobToComplete.ProcessInstanceKey)
	})

	t.Run("complete the waiting job", func(t *testing.T) {
		err := completeJob(t, jobToComplete.Key, jobToComplete.Variables)
		assert.NoError(t, err)
	})

	t.Run("the state of corresponding job should be COMPLETED", func(t *testing.T) {
		jobsPartitionPage, err := getJobs(t, zenclient.GetJobsParams{JobType: ptr.To("message-end-event-task-1")})
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		job := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateCompleted, job.State)
	})

	t.Run("the process instance state should be COMPLETED and no ActiveElementInstances should exist as the token is COMPLETED", func(t *testing.T) {
		fetchedProcessInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedProcessInstance.State)
		assert.Equal(t, 0, len(fetchedProcessInstance.ActiveElementInstances))
		assert.Equal(t, map[string]any{"instVar": "instVarValue", "newCity": "beijing"}, fetchedProcessInstance.Variables)
	})

}
