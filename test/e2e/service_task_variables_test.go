package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceTaskVariables(t *testing.T) {

	t.Run("If there are no mappings defined, the process variables remain unchanged", func(t *testing.T) {

		createInstanceVariables := map[string]any{"variable_from_create_instance_1": "var1"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		completeJobForElementId(t, processInstance.Key, "service_task", map[string]any{"variable_from_request_2": "request_var2"})

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
	})

	t.Run("Input mapping non-existing source variable assigns nil to target", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_from_create_instance_1": "var1",
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_input_missing_variable.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		expectedJobInputVariables := mergeMaps(createInstanceVariables, map[string]any{"missing_input": interface{}(nil)})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		require.Equal(t, expectedJobInputVariables, job.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)

		require.NoError(t, completeJob(t, job.Key, nil))

		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("Input mapping without a source definition assigns an empty string to the target", func(t *testing.T) {

		createInstanceVariables := map[string]any{"variable_from_create_instance_1": "var1"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_input_mapping_without_source.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		createInstanceVariablesWithInputVariable := mergeMaps(createInstanceVariables, map[string]any{"input_without_source": ""})

		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		require.Equal(t, createInstanceVariablesWithInputVariable, job.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		require.NoError(t, completeJob(t, job.Key, nil))

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
	})

	t.Run("Input-mapped variable is local and does not leak to process scope", func(t *testing.T) {

		createInstanceVariables := map[string]any{"variable_from_create_instance_1": "var1"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_input_mapping.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		createInstanceVariablesWithInputVariable := mergeMaps(createInstanceVariables, map[string]any{"amount": "var1"})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		require.Equal(t, createInstanceVariablesWithInputVariable, job.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		require.NoError(t, completeJob(t, job.Key, nil))

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
	})

	t.Run("Input mapping with static value 'name:John' assigns literal to local variable", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_input_mapping_value_in_source.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		inputVariable := map[string]any{"name": "John"}
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		require.Equal(t, inputVariable, job.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		require.NoError(t, completeJob(t, job.Key, nil))

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]interface{}{})
	})

	t.Run("Output mapping with static value 'name:John' writes literal to process scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_mapping_value_in_source.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		completeJobForElementId(t, processInstance.Key, "service_task", nil)

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})

		outputVariables := map[string]any{"name": "John"}
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", outputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, outputVariables)
	})

	t.Run("Output mapping with static value overwrites process variable", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_from_create_instance_to_be_changed": "var1",
			"variable_from_create_instance_2":             "var2",
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_mapping_replace_process_variable.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		createInstanceVariablesWithInputVariable := mergeMaps(createInstanceVariables, nil)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariablesWithInputVariable)
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", nil)

		completeJobForElementId(t, processInstance.Key, "service_task", nil)

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariablesWithInputVariable)

		expectedOutputVariables := map[string]any{"variable_from_create_instance_to_be_changed": "new_value"}
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", expectedOutputVariables)

		createInstanceVariablesAndOutputVariables := mergeMaps(createInstanceVariables, expectedOutputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariablesAndOutputVariables)
	})

	t.Run("Output mapping reads input-mapped local variable and propagates value to the process instance", func(t *testing.T) {
		createInstanceVariables := map[string]any{"variable_from_create_instance_1": "var1"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_uses_input_local.bpmn", createInstanceVariables)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		createInstanceVariablesWithInputVariable := mergeMaps(createInstanceVariables, map[string]any{
			"local_variable_from_input": "var1",
		})
		job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task")
		require.Equal(t, createInstanceVariablesWithInputVariable, job.InputVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", createInstanceVariables)

		require.NoError(t, completeJob(t, job.Key, nil))

		outputVariables := map[string]any{"output_variable": "var1"}
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", outputVariables)

		expectedProcessVariables := mergeMaps(createInstanceVariables, outputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedProcessVariables)
	})
}
