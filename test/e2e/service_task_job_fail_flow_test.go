package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskJobFailFlow(t *testing.T) {

	t.Run("Failing the service task holds the process and records the full history", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"service_task",
		})

		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		failJobForElementId(t, processInstance.Key, "service_task", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "service_task", incidents[0].ElementId)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"service_task"}, []string{"end_event"})
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"service_task",
		})
	})

	t.Run("Failed service task can be resolved and completed, and the full history is recorded", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")

		failJobForElementId(t, processInstance.Key, "service_task", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)

		resolveIncident(t, incidents[0].Key)

		completeJobForElementId(t, processInstance.Key, "service_task", nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, []string{"service_task"})
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"service_task",
			"Flow_12bso6z",
			"end_event",
		})
	})
}
