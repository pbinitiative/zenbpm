package e2e

import (
	"testing"
)

func TestServiceTaskErrorBoundaryVariables(t *testing.T) {

	t.Run("Matching error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{"variable_name": "boundary_value"}

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_error_boundary_event.bpmn", createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		failJobForElementId(t, processInstance.Key, "service-task-error-boundary", new("42"), jobVariables)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertFlowElementOutputVariables(t, processInstance.Key, "boundary-error-main-task", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})

	t.Run("Matching error boundary output mapping propagates only mapped fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_error_boundary_event_and_output_mapping.bpmn", createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		jobVariables := map[string]any{
			"variable_name": "boundary_value",
			"ignored":       "ignored_value",
		}
		failJobForElementId(t, processInstance.Key, "service-task-error-boundary", new("42"), jobVariables)

		expectedBoundaryVariables := map[string]any{"variable_name": "boundary_value"}
		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertFlowElementOutputVariables(t, processInstance.Key, "boundary-error-main-task", expectedBoundaryVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, expectedBoundaryVariables))
	})

	t.Run("Catch-all error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/service_task/service_task_with_catch_all_error_boundary_event.bpmn", createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		jobVariables := map[string]any{
			"error_detail": "catch_all_value",
		}
		failJobForElementId(t, processInstance.Key, "service_task", new("any-error"), jobVariables)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertFlowElementOutputVariables(t, processInstance.Key, "boundary-error-main-task", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})
}
