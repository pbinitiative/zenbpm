package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestParallelMultiInstanceErrorBoundaryVariables(t *testing.T) {
	t.Run("Matching error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":       "create_value",
			"unchanged_process":   "process_value",
			"testInputCollection": []string{"test1", "test2", "test3"},
		}
		jobVariables := map[string]any{"variable_name": "boundary_value"}

		definitionKey := deployParallelMultiInstanceErrorBoundaryVariablesDefinition(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_error_boundary_event.bpmn", "parallel-multi-instance-error-boundary-variables")
		processInstance, multiInstanceProcess := createMultiInstanceErrorBoundaryVariablesProcessInstance(t, definitionKey, createInstanceVariables)

		failJobForElementId(t, multiInstanceProcess.Key, "service_task", new("44"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, multiInstanceProcess.Key, "Event_15f0ox7", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(multiInstanceErrorBoundaryExpectedBaseVariables(), jobVariables))
	})

	t.Run("Matching error boundary output mapping propagates only mapped fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":       "create_value",
			"unchanged_process":   "process_value",
			"testInputCollection": []string{"test1", "test2", "test3"},
		}
		jobVariables := map[string]any{
			"variable_name": "boundary_value",
			"ignored":       "ignored_value",
		}

		definitionKey := deployParallelMultiInstanceErrorBoundaryVariablesDefinition(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_error_boundary_event_and_output_mapping.bpmn", "parallel-multi-instance-error-boundary-variables-output-mapping")
		processInstance, multiInstanceProcess := createMultiInstanceErrorBoundaryVariablesProcessInstance(t, definitionKey, createInstanceVariables)

		failJobForElementId(t, multiInstanceProcess.Key, "service_task", new("44"), jobVariables)

		expectedBoundaryVariables := map[string]any{"variable_name": "boundary_value"}
		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, multiInstanceProcess.Key, "Event_15f0ox7", expectedBoundaryVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(multiInstanceErrorBoundaryExpectedBaseVariables(), expectedBoundaryVariables))
	})

	t.Run("Catch-all error boundary without output mapping propagates all fail variables", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"variable_name":       "create_value",
			"unchanged_process":   "process_value",
			"testInputCollection": []string{"test1", "test2", "test3"},
		}
		jobVariables := map[string]any{"error_detail": "catch_all_value"}

		definitionKey := deployParallelMultiInstanceErrorBoundaryVariablesDefinition(t, "testdata/multi_instance/parallel_multi_instance_service_task_with_catch_all_error_boundary_event.bpmn", "parallel-multi-instance-error-boundary-variables-catch-all")
		processInstance, multiInstanceProcess := createMultiInstanceErrorBoundaryVariablesProcessInstance(t, definitionKey, createInstanceVariables)

		failJobForElementId(t, multiInstanceProcess.Key, "service_task", new("any-error"), jobVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, multiInstanceProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertFlowElementOutputVariables(t, multiInstanceProcess.Key, "Event_15f0ox7", jobVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(multiInstanceErrorBoundaryExpectedBaseVariables(), jobVariables))
	})
}

func deployParallelMultiInstanceErrorBoundaryVariablesDefinition(t *testing.T, filename string, processId string) int64 {
	t.Helper()

	uniqueSuffix := time.Now().UnixNano()
	jobType := fmt.Sprintf("%s-job-%d", processId, uniqueSuffix)
	definition, err := deployTestDataDefinitionWithJobType(t, filename, fmt.Sprintf("%s-%d", processId, uniqueSuffix), map[string]string{
		"TestType": jobType,
	})
	require.NoError(t, err)
	require.NotZero(t, definition.ProcessDefinitionKey)
	return definition.ProcessDefinitionKey
}
