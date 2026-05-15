package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestUserTaskJobFailFlow(t *testing.T) {

	t.Run("Failing the user task holds the process and records the full history", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"user_task",
		})

		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		failJobForElementId(t, processInstance.Key, "user_task", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "user_task", incidents[0].ElementId)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"user_task"}, []string{"end_event"})
		assertProcessInstanceTokenState(t, processInstance.Key, "user_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"user_task",
		})
	})

	t.Run("Failed user task can be resolved and completed, and the full history is recorded", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")

		failJobForElementId(t, processInstance.Key, "user_task", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)

		resolveIncident(t, incidents[0].Key)

		completeJobForElementId(t, processInstance.Key, "user_task", nil)

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
