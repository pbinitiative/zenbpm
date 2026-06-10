package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

func TestSubProcessErrorBoundaryVariables(t *testing.T) {
	t.Run("Matching error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{"variable_name": "boundary_value"}

		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/sub_process/subprocess_with_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, innerProcess.Key, "service_task", new("31"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, innerProcess.Key, "boundary_error_exact_match", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})

	t.Run("Matching error boundary output mapping propagates only mapped fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{
			"variable_name": "boundary_value",
			"ignored":       "ignored_value",
		}

		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/sub_process/subprocess_with_error_boundary_event_and_output_mapping.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, innerProcess.Key, "service_task", new("31"), jobVariables)

		expectedBoundaryVariables := map[string]any{"variable_name": "boundary_value"}
		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, innerProcess.Key, "boundary_error_exact_match", expectedBoundaryVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, expectedBoundaryVariables))
	})

	t.Run("Catch-all error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{"error_detail": "catch_all_value"}

		definitionKey := deployTestDataProcessDefinitionKey(t, "testdata/sub_process/subprocess_with_error_boundary_event_catch_all.bpmn")
		processInstance := createProcessInstanceWithVariables(t, definitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, innerProcess.Key, "service_task", new("any-error"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, innerProcess.Key, "boundary_error_catch_all", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})
}
