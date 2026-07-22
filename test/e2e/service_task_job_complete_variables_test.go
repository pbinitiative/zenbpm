package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestSequentialServiceTaskJobAndFlowElementVariables(t *testing.T) {
	processVariables := map[string]any{"seed": "initial"}
	processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_two_jobs_variables.bpmn", processVariables)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	firstJob := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "first_service_task")
	require.Equal(t, map[string]any{
		"seed":          "initial",
		"firstJobInput": "initial",
	}, firstJob.InputVariables)
	firstJobOutput := map[string]any{"firstJobOutput": "first-value"}
	require.NoError(t, completeJob(t, firstJob.Key, firstJobOutput))

	secondJob := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "second_service_task")
	require.Equal(t, map[string]any{
		"seed":           "initial",
		"firstResult":    "first-value",
		"secondJobInput": "first-value",
	}, secondJob.InputVariables)
	secondJobOutput := map[string]any{"secondJobOutput": "second-value"}
	require.NoError(t, completeJob(t, secondJob.Key, secondJobOutput))
	assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

	firstJob = waitForProcessInstanceJobByElementId(t, processInstance.Key, "first_service_task", public.JobStateCompleted)
	require.Equal(t, firstJobOutput, *firstJob.OutputVariables)
	secondJob = waitForProcessInstanceJobByElementId(t, processInstance.Key, "second_service_task", public.JobStateCompleted)
	require.Equal(t, secondJobOutput, *secondJob.OutputVariables)

	assertFlowElementInputVariables(t, processInstance.Key, "first_service_task", processVariables)
	assertFlowElementOutputVariables(t, processInstance.Key, "first_service_task", map[string]any{"firstResult": "first-value"})
	assertFlowElementInputVariables(t, processInstance.Key, "second_service_task", map[string]any{
		"seed":        "initial",
		"firstResult": "first-value",
	})
	assertFlowElementOutputVariables(t, processInstance.Key, "second_service_task", map[string]any{"secondResult": "second-value"})
}

func TestServiceTaskJobCompleteVariables(t *testing.T) {

	t.Run("Job completion variables are local without output mapping", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobVariables := map[string]any{"variable_without_output_mapping": "value"}
		completeJobForElementId(t, processInstance.Key, "service_task", completeJobVariables)

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]interface{}{})
	})

	t.Run("Job completion variables with non-string types are local without output mapping", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

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
		completeJobForElementId(t, processInstance.Key, "service_task", completeJobVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})

		assertProcessInstanceVariables(t, processInstance.Key, map[string]interface{}{})
	})

	t.Run("Job completion variables should not be propagated to job on job complete", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobVariables := map[string]any{"variable_without_output_mapping": "value"}
		completeJobForElementId(t, processInstance.Key, "service_task", completeJobVariables)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateCompleted)
		require.Equal(t, completeJobVariables, *job.OutputVariables, "completion variables should be stored as output variables on the job")
	})

	t.Run("Only job completion variables referenced by output mappings are propagated", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_mapping.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		completeJobVariables := map[string]any{
			"complete_job_variable": "mapped_value",
			"ignored_variable":      "ignored_value",
		}
		completeJobForElementId(t, processInstance.Key, "service_task", completeJobVariables)

		// FlowElementInstance (history) stores only mapped output variables
		expectedVariables := map[string]any{"output_variable_from_service_task": "mapped_value"}
		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", expectedVariables)
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)

		// Job stores all output variables before mapping, including unmapped ones
		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateCompleted)
		require.Equal(t, completeJobVariables, *job.OutputVariables, "job must store all output variables before mapping")
	})

	t.Run("Job output variables include unmapped variables; history includes only mapped", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_mapping.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		completeJobVariables := map[string]any{
			"complete_job_variable": "mapped_value",
			"extra_unmapped":        "extra_value",
		}
		completeJobForElementId(t, processInstance.Key, "service_task", completeJobVariables)

		// Job stores ALL output variables (before output mapping filters them)
		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateCompleted)
		require.Equal(t, completeJobVariables, *job.OutputVariables, "job must store all output variables")

		// FlowElementInstance (history) stores only mapped output variables
		mappedOutput := map[string]any{"output_variable_from_service_task": "mapped_value"}
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", mappedOutput)

		// Verify the history does NOT contain the unmapped variable
		flowElement := getFlowElementByElementId(t, processInstance.Key, "service_task")
		require.NotContains(t, flowElement.OutputVariables, "extra_unmapped", "flow element history must not contain unmapped output variables")
		require.Contains(t, *job.OutputVariables, "extra_unmapped", "job must contain unmapped output variable")
	})

	t.Run("Job completion variables override local variables with the same name in output mapping scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_replace_local_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobForElementId(t, processInstance.Key, "service_task", map[string]any{"local_variable": "complete_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]any{"output_variable": "complete_value"})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"process_variable": "input_value",
			"output_variable":  "complete_value",
		})
	})

	t.Run("Job completion variables override process variables with the same name in output mapping scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_replace_process_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		completeJobForElementId(t, processInstance.Key, "service_task", map[string]any{"local_variable": "complete_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]any{"process_variable": "complete_value"})
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"process_variable": "complete_value",
		})
	})
}
