package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceTaskJobCompleteVariables(t *testing.T) {

	t.Run("Job completion variables are local without output mapping", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_from_create_instance_1": "var1",
			"variable_from_create_instance_2": "var2",
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_complete_minimal.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		serviceTaskJob := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task")
		completeJobVariables := map[string]any{
			"variable_from_create_instance_1": "overwritten",
		}
		err := completeJob(t, serviceTaskJob.Key, completeJobVariables)
		require.NoError(t, err)

		expectedProcessVariables := map[string]any{
			"variable_from_create_instance_1": "var1",
			"variable_from_create_instance_2": "var2",
		}
		assertProcessInstanceVariables(t, processInstance.Key, expectedProcessVariables)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("Job completion variables with non-string types are local without output mapping", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_from_create_instance_1": "var1",
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_complete_minimal.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		serviceTaskJob := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task")
		completeJobVariables := map[string]any{
			"number_value":  float64(42),
			"boolean_value": true,
			"null_value":    nil,
			"list_value":    []any{"a", "b", "c"},
			"object_value": map[string]any{
				"nested_key": "nested_value",
			},
		}
		err := completeJob(t, serviceTaskJob.Key, completeJobVariables)
		require.NoError(t, err)

		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})
}
