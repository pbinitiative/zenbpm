package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

func TestServiceTaskFlow(t *testing.T) {
	t.Run("Process waits on the service task and records history up to the active task", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"service_task"}, []string{"end_event"})
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_0dgvzs1",
			"service_task",
		})
	})
}
