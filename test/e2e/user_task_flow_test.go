package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

// TestUserTaskFlow drives a User Task with a configured non-default job
// type ("test") end to end: it asserts the type shows up on the active
// job, that the public REST job filter returns the job for the configured
// type, and that an unrelated type filter returns no jobs.
func TestUserTaskFlow(t *testing.T) {
	t.Run("Process waits on the user task and records history up to the active task", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")
		require.Equal(t, "test", job.Type)
		require.Equal(t, "USER_TASK", job.ElementType)

		jobType := "test"
		page, err := getJobs(t, zenclient.GetJobsParams{
			ProcessInstanceKey: new(processInstance.Key),
			JobType:            &jobType,
		})
		require.NoError(t, err)
		var filteredJobs []zenclient.Job
		for _, partition := range page.Partitions {
			filteredJobs = append(filteredJobs, partition.Items...)
		}
		require.Len(t, filteredJobs, 1)
		require.Equal(t, job.Key, filteredJobs[0].Key)
		require.Equal(t, "test", filteredJobs[0].Type)
		require.Equal(t, "USER_TASK", filteredJobs[0].ElementType)

		nonMatchingType := "not-a-user-task-type"
		nonMatchingPage, err := getJobs(t, zenclient.GetJobsParams{
			ProcessInstanceKey: new(processInstance.Key),
			JobType:            &nonMatchingType,
		})
		require.NoError(t, err)
		for _, partition := range nonMatchingPage.Partitions {
			require.Empty(t, partition.Items)
		}

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"user_task"}, []string{"end_event"})
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"user_task",
		})
	})
}
