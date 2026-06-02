package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestUserTaskJobCompleteVariables(t *testing.T) {

	t.Run("Job completion variables are local without output mapping", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobVariables := map[string]any{"variable_without_output_mapping": "value"}
		completeJobForElementId(t, processInstance.Key, "user_task", completeJobVariables)

		assertFlowElementInputVariables(t, processInstance.Key, "user_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "user_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]interface{}{})
	})

	t.Run("Job completion variables with non-string types are local without output mapping", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobVariables := map[string]any{
			"number_value":  float64(42),
			"boolean_value": true,
			"null_value":    nil,
			"list_value":    []any{"a", "b", "c"},
			"object_value": map[string]any{
				"nested_key": "nested_value",
			},
		}
		completeJobForElementId(t, processInstance.Key, "user_task", completeJobVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "user_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "user_task", map[string]interface{}{})

		assertProcessInstanceVariables(t, processInstance.Key, map[string]interface{}{})
	})

	t.Run("Job completion variables should not be propagated to job on job complete", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobVariables := map[string]any{"variable_without_output_mapping": "value"}
		completeJobForElementId(t, processInstance.Key, "user_task", completeJobVariables)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateCompleted)
		require.Equal(t, completeJobVariables, *job.OutputVariables, "completion variables should be stored as output variables on the job")
	})

	t.Run("Only job completion variables referenced by output mappings are propagated", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_with_output_mapping.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		completeJobVariables := map[string]any{
			"complete_job_variable": "mapped_value",
			"ignored_variable":      "ignored_value",
		}
		completeJobForElementId(t, processInstance.Key, "user_task", completeJobVariables)

		expectedVariables := map[string]any{"output_variable_from_user_task": "mapped_value"}
		assertFlowElementInputVariables(t, processInstance.Key, "user_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "user_task", expectedVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
	})

	t.Run("Job completion variables override local variables with the same name in output mapping scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_job_replace_local_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobForElementId(t, processInstance.Key, "user_task", map[string]any{"local_variable": "complete_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "user_task", map[string]any{"output_variable": "complete_value"})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"process_variable": "input_value",
			"output_variable":  "complete_value",
		})
	})

	t.Run("Job completion variables override process variables with the same name in output mapping scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/user_task/user_task_job_replace_process_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobForElementId(t, processInstance.Key, "user_task", map[string]any{"local_variable": "complete_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "user_task", map[string]any{"process_variable": "complete_value"})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"process_variable": "complete_value",
		})
	})
}
