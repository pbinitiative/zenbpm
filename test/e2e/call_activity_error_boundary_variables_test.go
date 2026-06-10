package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

func TestCallActivityErrorBoundaryVariables(t *testing.T) {
	t.Run("Matching error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{"variable_name": "boundary_value"}

		parentDefinitionKey := deployCallActivityErrorBoundaryTestDataProcessDefinitions(t, "testdata/call_activity/call_activity_with_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, parentDefinitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, childInstance.Key, "id", new("42"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, childInstance.Key, "boundary-error-main-task", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})

	t.Run("Matching error boundary output mapping propagates only mapped fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{
			"variable_from_request": "boundary_value",
			"ignored":               "ignored_value",
		}

		parentDefinitionKey := deployCallActivityErrorBoundaryTestDataProcessDefinitions(t, "testdata/call_activity/call_activity_with_error_boundary_event_and_output_mapping.bpmn")
		processInstance := createProcessInstanceWithVariables(t, parentDefinitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, childInstance.Key, "id", new("42"), jobVariables)

		expectedBoundaryVariables := map[string]any{"boundary_variable": "boundary_value"}
		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, childInstance.Key, "boundary-error-main-task", expectedBoundaryVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, expectedBoundaryVariables))
	})

	t.Run("Catch-all error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":     "create_value",
			"unchanged_process": "process_value",
		}
		jobVariables := map[string]any{"error_detail": "catch_all_value"}

		parentDefinitionKey := deployCallActivityErrorBoundaryTestDataProcessDefinitions(t, "testdata/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn")
		processInstance := createProcessInstanceWithVariables(t, parentDefinitionKey, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)
		failJobForElementId(t, childInstance.Key, "id", new("any-error"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, childInstance.Key, "boundary-error-main-task", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(createInstanceVariables, jobVariables))
	})
}
