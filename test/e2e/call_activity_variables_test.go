package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallActivityVariables(t *testing.T) {
	t.Run("Input mapping starts the child with a snapshot of parent variables plus mapped variables", func(t *testing.T) {
		parentVariables := map[string]any{
			"order_id":      "order-1",
			"variable_name": "parent_value",
		}

		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_input_output_mapping.bpmn", parentVariables)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertProcessInstanceVariables(t, childInstance.Key, mergeMaps(parentVariables, map[string]any{"child_order_id": "order-1"}))
		// The call activity's history records the scope as it was before the input mappings ran.
		assertFlowElementInputVariables(t, processInstance.Key, "call_activity", parentVariables)
		assertProcessInstanceVariables(t, processInstance.Key, parentVariables)
	})

	t.Run("Input mapping overrides an inherited parent variable with the same name in the child only", func(t *testing.T) {
		parentVariables := map[string]any{
			"order_id":       "order-1",
			"child_order_id": "inherited_value",
		}

		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_input_output_mapping.bpmn", parentVariables)

		waitForProcessInstanceActiveJobByElementId(t, childInstance.Key, "id")
		assertProcessInstanceVariables(t, childInstance.Key, mergeMaps(parentVariables, map[string]any{"child_order_id": "order-1"}))
		assertFlowElementInputVariables(t, processInstance.Key, "call_activity", parentVariables)
		assertProcessInstanceVariables(t, processInstance.Key, parentVariables)
	})

	t.Run("Output mapping propagates only mapped child variables to the parent", func(t *testing.T) {
		parentVariables := map[string]any{
			"order_id":      "order-1",
			"variable_name": "parent_value",
		}

		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_input_output_mapping.bpmn", parentVariables)
		completeJobForElementId(t, childInstance.Key, "id", map[string]any{"variable_name": "child_value"})

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, childInstance.Key, map[string]any{
			"order_id":       "order-1",
			"variable_name":  "child_value",
			"child_order_id": "order-1",
		})
		assertFlowElementOutputVariables(t, processInstance.Key, "call_activity", map[string]any{"call_result": "child_value"})
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(parentVariables, map[string]any{"call_result": "child_value"}))
	})

	t.Run("Mapped variable round-trips through a called process that completes immediately", func(t *testing.T) {
		parentVariables := map[string]any{
			"variable_name":     "parent_value",
			"unchanged_process": "process_value",
		}

		definitionKey := deployCallActivityWithSimpleSubProcessDefinitions(t)
		processInstance := createProcessInstanceWithVariables(t, definitionKey, parentVariables)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, childInstance.Key, parentVariables)
		assertFlowElementInputVariables(t, processInstance.Key, "callActivity", parentVariables)
		assertFlowElementOutputVariables(t, processInstance.Key, "callActivity", map[string]any{"variable_name": "parent_value"})
		assertProcessInstanceVariables(t, processInstance.Key, parentVariables)
		assertMessageSubscriptionStateCount(t, processInstance.Key, "callActivity", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactCompletedProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_0y8zi5j",
			"callActivity",
			"Flow_1htsftl",
			"Event_0rrd3fx",
		})
	})

	t.Run("Without output mapping no child variables propagate to the parent", func(t *testing.T) {
		// The fixture is named after its boundary event's missing output mapping; its call activity has no ioMapping either.
		definitionKey, _, _ := deployCallActivityMessageBoundaryDefinitions(
			t,
			"testdata/call_activity/call_activity_with_message_boundary_event_without_output_mapping.bpmn",
			"call-activity-message-boundary-event-without-output-mapping",
		)
		parentVariables := map[string]any{"variable_name": "parent_value"}
		processInstance := createMessageBoundaryVariablesInstance(t, definitionKey, parentVariables)
		childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)

		completeJobForElementId(t, childInstance.Key, "id", map[string]any{"variable_name": "child_value"})

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, childInstance.Key, map[string]any{"variable_name": "child_value"})
		assert.Empty(t, getFlowElementByElementId(t, processInstance.Key, "call_activity").OutputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, parentVariables)
	})

	t.Run("Error boundary skips the call activity output mapping and propagates the fail variables", func(t *testing.T) {
		parentVariables := map[string]any{
			"order_id":      "order-1",
			"variable_name": "parent_value",
		}
		failVariables := map[string]any{
			"variable_name": "failed_value",
			"error_detail":  "detail",
		}

		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_input_output_mapping.bpmn", parentVariables)
		failJobForElementId(t, childInstance.Key, "id", new("42"), failVariables)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenCount(t, processInstance.Key, "completed-end", 0)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", runtime.TokenStateCanceled)
		assertFlowElementOutputVariables(t, processInstance.Key, "error_boundary", failVariables)
		require.Empty(t, getFlowElementByElementId(t, processInstance.Key, "call_activity").OutputVariables)
		assertProcessInstanceVariables(t, processInstance.Key, mergeMaps(parentVariables, failVariables))
	})
}

// deployCallActivityWithSimpleSubProcessDefinitions deploys the shared call-activity-with-simple-subprocess
// fixture and its called process under unique ids, message name and correlation key, so that concurrently
// running tests deploying the same fixtures cannot be resolved as the called process or correlate its boundary.
func deployCallActivityWithSimpleSubProcessDefinitions(t testing.TB) int64 {
	t.Helper()

	const (
		parentFilename = "../../pkg/bpmn/test-cases/call-activity/call-activity-with-simple-subprocess.bpmn"
		childFilename  = "../../pkg/bpmn/test-cases/simple-simple-sub-process.bpmn"
	)
	suffix := messageEventTestSuffix()
	childProcessID := fmt.Sprintf("empty-sub-process-%d", suffix)

	child := string(readBPMNTestCaseFile(t, childFilename))
	child = replaceInBPMNFixture(t, childFilename, child, `bpmn:process id="empty-sub-process"`, fmt.Sprintf(`bpmn:process id="%s"`, childProcessID))
	deployBPMNTestCaseContent(t, []byte(child))

	parent := string(readBPMNTestCaseFile(t, parentFilename))
	parent = replaceInBPMNFixture(t, parentFilename, parent,
		`bpmn:process id="Simple_CallActivity_Process"`, fmt.Sprintf(`bpmn:process id="call-activity-with-simple-subprocess-%d"`, suffix))
	parent = replaceInBPMNFixture(t, parentFilename, parent,
		`calledElement processId="empty-sub-process"`, fmt.Sprintf(`calledElement processId="%s"`, childProcessID))
	parent = replaceInBPMNFixture(t, parentFilename, parent,
		`name="Message_2ffbhei"`, fmt.Sprintf(`name="call-activity-with-simple-subprocess-message-%d"`, suffix))
	parent = replaceInBPMNFixture(t, parentFilename, parent,
		`correlationKey="=&#34;testMessage&#34;"`, fmt.Sprintf(`correlationKey="=&#34;call-activity-with-simple-subprocess-key-%d&#34;"`, suffix))
	return deployBPMNTestCaseContent(t, []byte(parent))
}
