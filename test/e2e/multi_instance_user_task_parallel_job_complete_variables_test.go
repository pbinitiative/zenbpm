package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallelMultiInstanceUserTaskJobCompleteVariables(t *testing.T) {

	t.Run("output element reads variables produced by activity output mapping", func(t *testing.T) {
		approvers := []string{"alice", "bob", "carol"}
		approvals := map[string]bool{"alice": true, "bob": false, "carol": true}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/multi_instance/multi_instance_user_task_parallel_with_output_mapping.bpmn", map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", len(approvers))
		// Complete in reverse order to verify each iteration owns its own output mapping result
		// independently of completion order.
		for i := len(jobs) - 1; i >= 0; i-- {
			approver := jobs[i].InputVariables["approver"].(string)
			require.NoError(t, completeJob(t, jobs[i].Key, map[string]any{"approved": approvals[approver]}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		results, ok := instance.Variables["results"].([]interface{})
		require.True(t, ok, "results must be a list, got %T", instance.Variables["results"])
		require.ElementsMatch(t, []interface{}{
			map[string]interface{}{"kdo": "alice", "vysledek": true},
			map[string]interface{}{"kdo": "bob", "vysledek": false},
			map[string]interface{}{"kdo": "carol", "vysledek": true},
		}, results, "each parallel iteration must compose its own output object from its own output mapping; ordering is not guaranteed for parallel multi-instance")
	})
}
