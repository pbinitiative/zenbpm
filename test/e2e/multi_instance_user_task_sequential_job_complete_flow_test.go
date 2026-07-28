package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestSequentialMultiInstanceUserTaskJobCompleteFlow(t *testing.T) {
	t.Run("Completing each iteration in sequence completes the parent and child with exact history", func(t *testing.T) {
		approvers := []string{"alice", "bob"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		firstJob := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "user_task")

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateActive,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateActive,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_to_user_task",
			"user_task",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"user_task",
		})

		require.NoError(t, completeJob(t, firstJob.Key, nil))
		secondJob := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "user_task")
		require.NotEqual(t, firstJob.Key, secondJob.Key)

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateActive,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateActive,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_to_user_task",
			"user_task",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"user_task",
			"user_task",
		})

		iterationHistory := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "user_task")
		require.Len(t, iterationHistory, len(approvers))
		require.NotNil(t, iterationHistory[0].CompletedAt, "the first iteration must be completed")
		require.Nil(t, iterationHistory[1].CompletedAt, "the second iteration must remain active")

		require.NoError(t, completeJob(t, secondJob.Key, nil))

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateCompleted,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateCompleted,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "user_task", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_to_user_task",
			"user_task",
			"Flow_to_end",
			"end_event",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"user_task",
			"user_task",
		})

		iterationHistory = getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "user_task")
		require.Len(t, iterationHistory, len(approvers))
		for _, iteration := range iterationHistory {
			require.NotNil(t, iteration.CompletedAt, "every multi-instance iteration must be completed")
		}

		completedJobs, err := getProcessInstanceJobs(t, multiInstanceProcess.Key)
		require.NoError(t, err)
		require.Len(t, completedJobs, len(approvers))
		for _, job := range completedJobs {
			require.Equal(t, public.JobStateCompleted, job.State)
		}
	})
}
