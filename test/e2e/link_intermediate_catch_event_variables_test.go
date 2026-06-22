package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

const (
	linkEventNonStringOutputVariablesPath = "testdata/link_event/link-event-output-variables-non-string.bpmn"
	linkEventCatchUsesThrowOutputPath     = "testdata/link_event/link-event-catch-uses-throw-output.bpmn"
	linkEventReplaceProcessVariablePath   = "testdata/link_event/link-event-replace-process-variable.bpmn"
	linkEventOutputVariablesPath          = "testdata/link_event/link-event-output-variables.bpmn"
)

func TestLinkIntermediateEventVariables(t *testing.T) {
	t.Run("Link events without output mappings keep all process variables in scope", func(t *testing.T) {
		createInstanceVariables := map[string]any{
			"string_value":  "value",
			"number_value":  float64(42),
			"boolean_value": true,
			"list_value":    []any{"a", "b", "c"},
			"object_value": map[string]any{
				"nested_key": "nested_value",
			},
		}
		processInstance := deployAndCreateUniqueProcessDefinition(t, linkEventsPath, createInstanceVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-A")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-B")
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)

		assertProcessInstanceVariables(t, processInstance.Key, createInstanceVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "Task-A", createInstanceVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "Task-B", createInstanceVariables)
	})

	t.Run("Throw and catch link output mappings are written to the process scope", func(t *testing.T) {
		processInstance := createLinkOutputVariablesProcessInstance(t, linkEventOutputVariablesPath, nil)

		expectedVariables := map[string]any{"throw": "throw", "catch": "catch"}
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)

		assertFlowElementInputVariables(t, processInstance.Key, "Task", expectedVariables)
	})

	t.Run("Throw and catch link output mappings preserve non-string values", func(t *testing.T) {
		processInstance := createLinkOutputVariablesProcessInstance(t, linkEventNonStringOutputVariablesPath, nil)

		expectedVariables := map[string]any{
			"number_value":  float64(42),
			"boolean_value": true,
			"null_value":    nil,
			"list_value":    []any{"a", "b", "c"},
			"object_value": map[string]any{
				"nested_key": "nested_value",
			},
		}
		assertProcessInstanceVariables(t, processInstance.Key, expectedVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "Task", expectedVariables)
	})

	t.Run("Catch link output mappings can read values written by throw link output mappings", func(t *testing.T) {
		processInstance := createLinkOutputVariablesProcessInstance(t, linkEventCatchUsesThrowOutputPath, map[string]any{
			"local_variable":   "input_value",
			"process_variable": "input_value",
		})

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"local_variable":   "throw_value",
			"process_variable": "input_value",
			"output_variable":  "throw_value",
		})
	})

	t.Run("Catch link output mappings override process variables with the same target", func(t *testing.T) {
		processInstance := createLinkOutputVariablesProcessInstance(t, linkEventReplaceProcessVariablePath, map[string]any{
			"local_variable":   "input_value",
			"process_variable": "input_value",
		})

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"local_variable":   "throw_value",
			"process_variable": "throw_value",
		})
	})
}

func createLinkOutputVariablesProcessInstance(t *testing.T, filePath string, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	processInstance := deployAndCreateUniqueProcessDefinition(t, filePath, variables)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task")
	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)

	return processInstance
}
