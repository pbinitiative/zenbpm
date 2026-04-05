package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestTimerStartEvent(t *testing.T) {
	// process definition:
	// (Timer start event with DateTime in past) -> (service task with job type "input-task-for-timer-start-event-test") -> (end event)
	var definition zenclient.ProcessDefinitionSimple
	var processInstances *zenclient.GetProcessInstancesResponse
	var err error

	t.Run("create timer-start-event-process.bpmn process definition", func(t *testing.T) {
		definition, err = deployGetDefinition(t, "timer-start-event-process.bpmn", "timer-start-event-process-1")
		assert.NoError(t, err)

		// No process instance creation is needed. Timer start event should create the instance itself after definition deployment and timer activation duration
		time.Sleep(2 * time.Second)

		// verify that process instance exists and is in Active state
		processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, processInstances.JSON200.Partitions[0].Items[0].State)
	})

	// find the input-task-for-timer-start-event-test job and verify it is Active
	var jobToComplete zenclient.Job
	t.Run("read waiting jobs", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "input-task-for-timer-start-event-test")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, zenclient.JobStateActive, jobToComplete.State)
	})

	t.Run("complete job and verify instance is completed", func(t *testing.T) {
		err := completeJob(t, jobToComplete, map[string]any{})
		assert.NoError(t, err)

		processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
		})
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, processInstances.JSON200.Partitions[0].Items[0].State)
	})

}
