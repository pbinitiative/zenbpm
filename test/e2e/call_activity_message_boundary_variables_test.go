package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestCallActivityMessageBoundaryVariables(t *testing.T) {
	t.Run("Message boundary without output mapping propagates all message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_without_output_mapping.bpmn",
			"call-activity-message-boundary-event-without-output-mapping",
		)
		processVariables := map[string]any{
			"existing":    "process-value",
			"overwritten": "initial-value",
		}
		instance := createMessageBoundaryVariablesInstance(t, definitionKey, processVariables)
		childInstance := waitForChildProcessInstance(t, instance.Key, 0)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertMessageSubscriptionState(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive)

		messageVariables := map[string]any{
			"overwritten": "message-value",
			"unmapped":    "unmapped-value",
			"numeric":     float64(42),
		}
		err := publishMessage(t, messageName, correlationKey, &messageVariables)
		require.NoError(t, err)

		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertFlowElementOutputVariables(t, instance.Key, "boundary_message_event", messageVariables)
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(processVariables, messageVariables))

		completeJobForElementId(t, childInstance.Key, "id", map[string]any{"variable_name": "child-value"})
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
	})

	t.Run("Message boundary with output mapping propagates only mapped message variables", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_with_output_mapping.bpmn",
			"call-activity-message-boundary-event-with-output-mapping",
		)
		processVariables := map[string]any{"existing": "process-value"}
		instance := createMessageBoundaryVariablesInstance(t, definitionKey, processVariables)
		childInstance := waitForChildProcessInstance(t, instance.Key, 0)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertMessageSubscriptionState(t, instance.Key, "call_activity", zenclient.EventSubscriptionStateActive)

		messageVariables := map[string]any{
			"payload":  "mapped-value",
			"unmapped": "ignored-value",
			"numeric":  float64(42),
		}
		err := publishMessage(t, messageName, correlationKey, &messageVariables)
		require.NoError(t, err)

		expectedMappedVariables := map[string]any{"payload": "mapped-value"}
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertFlowElementOutputVariables(t, instance.Key, "boundary_message_event", expectedMappedVariables)
		assertProcessInstanceVariables(t, instance.Key, mergeMaps(processVariables, expectedMappedVariables))

		completeJobForElementId(t, childInstance.Key, "id", map[string]any{"variable_name": "child-value"})
		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
	})
}

func deployCallActivityMessageBoundaryDefinitions(t testing.TB, parentFilename string, baseProcessID string) (int64, string, string) {
	return deployCallActivityMessageBoundaryDefinitionsWithInterrupting(t, parentFilename, baseProcessID, false)
}

func deployInterruptingCallActivityMessageBoundaryDefinitions(t testing.TB, parentFilename string, baseProcessID string) (int64, string, string) {
	return deployCallActivityMessageBoundaryDefinitionsWithInterrupting(t, parentFilename, baseProcessID, true)
}

func deployCallActivityMessageBoundaryDefinitionsWithInterrupting(t testing.TB, parentFilename string, baseProcessID string, interrupting bool) (int64, string, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	childProcessID := fmt.Sprintf("%s-%d", callActivityErrorBoundaryChildProcessId, suffix)
	childJobType := fmt.Sprintf("call-activity-message-boundary-child-%d", suffix)
	_, err := deployTestDataDefinitionWithJobType(
		t,
		"testdata/call_activity/call_activity_with_error_boundary_event_child_process.bpmn",
		childProcessID,
		map[string]string{"TestType": childJobType},
	)
	require.NoError(t, err)

	processID := fmt.Sprintf("%s-%d", baseProcessID, suffix)
	messageName := fmt.Sprintf("%s-ref-%d", baseProcessID, suffix)
	correlationKey := fmt.Sprintf("%s-key-%d", baseProcessID, suffix)
	content := string(readBPMNTestCaseFile(t, parentFilename))
	if interrupting {
		content = strings.Replace(content, `cancelActivity="false"`, `cancelActivity="true"`, 1)
	}
	content = strings.NewReplacer(
		fmt.Sprintf(`bpmn:process id="%s"`, baseProcessID), fmt.Sprintf(`bpmn:process id="%s"`, processID),
		fmt.Sprintf(`calledElement processId="%s"`, callActivityErrorBoundaryChildProcessId), fmt.Sprintf(`calledElement processId="%s"`, childProcessID),
		`name="message"`, fmt.Sprintf(`name="%s"`, messageName),
		`correlationKey="=correlationKey"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKey),
	).Replace(content)

	return deployBPMNTestCaseContent(t, parentFilename, []byte(content)), messageName, correlationKey
}
