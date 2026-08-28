package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

// TestUserTaskJobCompleteFlow verifies that a User Task with a configured
// non-default job type ("test") can be completed end to end through the
// runtime Job API.
func TestUserTaskJobCompleteFlow(t *testing.T) {

	t.Run("Completing the user task completes the process and records the full history", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")
		require.Equal(t, "test", job.Type)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"user_task",
		})

		completeJobForElementId(t, processInstance.Key, "user_task", nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateCompleted)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, []string{"user_task"})
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"user_task",
			"Flow_12bso6z",
			"end_event",
		})
	})
}
