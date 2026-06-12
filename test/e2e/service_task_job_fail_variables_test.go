package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestServiceTaskJobFailVariables(t *testing.T) {

	t.Run("Job fail variables are retained on the failed job", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_minimal.bpmn", nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		jobVariables := map[string]any{"variable_without_output_mapping": "value"}
		failJobForElementId(t, processInstance.Key, "service_task", nil, jobVariables)

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)
		require.Equal(t, jobVariables, *job.OutputVariables)
	})

	t.Run("Job fail variables do not propagate through service task output mappings", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_output_mapping.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		jobVariables := map[string]any{
			"complete_job_variable": "mapped_value",
			"ignored_variable":      "ignored_value",
		}
		failJobForElementId(t, processInstance.Key, "service_task", nil, jobVariables)

		assertFlowElementInputVariables(t, processInstance.Key, "service_task", map[string]interface{}{})
		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]interface{}(nil))

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)
		require.Equal(t, jobVariables, *job.OutputVariables)

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{})
	})

	t.Run("Job fail variables override same-named variables in the local scope", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_replace_local_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		failJobForElementId(t, processInstance.Key, "service_task", nil, map[string]any{"local_variable": "fail_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]any(nil))

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)
		require.Equal(t, "fail_value", (*job.OutputVariables)["local_variable"])

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"process_variable": "input_value"})
	})

	t.Run("Job fail variables do not override process variables through output mappings", func(t *testing.T) {

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_job_replace_process_variable.bpmn", map[string]any{"process_variable": "input_value"})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		failJobForElementId(t, processInstance.Key, "service_task", nil, map[string]any{"local_variable": "complete_value"})

		assertFlowElementOutputVariables(t, processInstance.Key, "service_task", map[string]any(nil))

		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task", public.JobStateFailed)
		require.Equal(t, "complete_value", (*job.OutputVariables)["local_variable"])

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"process_variable": "input_value"})
	})
}
