package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskJobFailIncident(t *testing.T) {

	t.Run("Failing job creates incident when error is uncaught", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		failJobForElementId(t, processInstance.Key, "service_task", nil, nil)

		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "service_task", incidents[0].ElementId)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("Failing job creates incident when error is uncaught and then resolved", func(t *testing.T) {

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

		incidentsAfterResolve, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NotNil(t, incidentsAfterResolve[0].ResolvedAt)
	})
}
