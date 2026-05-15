package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSequentialMultiInstanceUserTaskJobCompleteVariables(t *testing.T) {

	t.Run("output element reads variables produced by activity output mapping", func(t *testing.T) {
		approvers := []string{"alice", "bob"}
		approvals := []bool{true, false}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/multi_instance/multi_instance_user_task_sequential_with_output_mapping.bpmn", map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		for i := range approvers {
			job := waitForProcessInstanceJobByElementId(t, firstChild.Key, "user_task")
			require.NoError(t, completeJob(t, job.Key, map[string]any{"approved": approvals[i]}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, []interface{}{
			map[string]interface{}{"kdo": "alice", "vysledek": true},
			map[string]interface{}{"kdo": "bob", "vysledek": false},
		}, instance.Variables["results"], "outputElement must compose objects from activity output mappings in source order")

		require.NotContains(t, instance.Variables, "kdo", "activity output mapping target must remain in iteration scope")
		require.NotContains(t, instance.Variables, "vysledek", "activity output mapping target must remain in iteration scope")
	})
}
