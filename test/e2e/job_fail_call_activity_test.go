package e2e

import (
	"fmt"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrpcJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch all", ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity catch by error code", ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailActiveJobViaGrpc(t, job, "grpc call activity incident", ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateFailed)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "boundary-error-call-activity", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})
}

func TestRestJobFailOnCallActivity(t *testing.T) {

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_catch_all_error_boundary_event.bpmn", "call-activity-with-catch-all-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")
		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_with_error_boundary_event.bpmn", "call-activity-with-error-boundary-event")

		deployCallActivityChildProcessDefinition(t)
		parentInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)

		childInstance := waitForChildProcessInstance(t, parentInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, childInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, childInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForTwoProcessInstanceStates(t, parentInstance.Key, zenclient.ProcessInstanceStateActive, childInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", bpmnruntime.TokenStateFailed)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, childInstance.Key)

		assertProcessInstanceTokenState(t, parentInstance.Key, "boundary-error-call-activity", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, parentInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, parentInstance.Key, nil, []string{"handled-end", "should-not-happen-end"})
	})

	t.Run("nested_matched_error_is_caught", func(t *testing.T) {
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_leaf.bpmn", "nested_call_activity_error_leaf")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_1.bpmn", "nested_call_activity_error_parent_1")
		deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_2.bpmn", "nested_call_activity_error_parent_2")
		rootDefinitionKey := deployProcessDefinitionKey(t, "error_events/call_activity/call_activity_nested_with_error_boundary_parent_root.bpmn", "nested_call_activity_error_root")

		rootProcessInstance := createErrorBoundaryProcessInstanceWithDefaultVariables(t, rootDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, rootProcessInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, rootProcessInstance.Key, 0, 0)

		parentTwoInstance := waitForChildProcessInstance(t, rootProcessInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 0, 0)

		parentOneInstance := waitForChildProcessInstance(t, parentTwoInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, parentOneInstance.Key, 0, 0)

		leafInstance := waitForChildProcessInstance(t, parentOneInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, leafInstance.Key, 0, 0)

		job := waitForProcessInstanceJobByElementId(t, leafInstance.Key, "id")
		callFailJobViaRest(t, job.Key, ptr.To("42"))

		waitForProcessInstanceState(t, leafInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, leafInstance.Key, "id", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, leafInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, leafInstance.Key, 0, 0)

		waitForProcessInstanceState(t, parentOneInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, parentOneInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentOneInstance.Key, 0, 0)

		waitForProcessInstanceState(t, parentTwoInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, parentTwoInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, parentTwoInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentTwoInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentTwoInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})

		waitForProcessInstanceState(t, rootProcessInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, rootProcessInstance.Key, "End", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceIncidentsLength(t, rootProcessInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, rootProcessInstance.Key, 0, 0)
		assertProcessInstanceTokenElements(t, rootProcessInstance.Key, []string{"End"}, nil)
	})
}

func deployCallActivityChildProcessDefinition(t testing.TB) {
	t.Helper()

	_, err := deployDefinitionWithJobType(t, "error_events/call_activity/call_activity_with_error_boundary_event_child_process.bpmn", "simple_task_for_call_activity_error_boundary_event", map[string]string{
		"TestType": fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
}

func waitForTwoProcessInstanceStates(t testing.TB, firstKey int64, firstExpected zenclient.ProcessInstanceState, secondKey int64, secondExpected zenclient.ProcessInstanceState) {
	t.Helper()

	assert.Eventually(t, func() bool {
		first, err := getProcessInstance(t, firstKey)
		if err != nil {
			return false
		}
		second, err := getProcessInstance(t, secondKey)
		if err != nil {
			return false
		}
		return first.State == firstExpected && second.State == secondExpected
	}, 10*time.Second, 100*time.Millisecond, "process instances %d and %d should reach states %s and %s", firstKey, secondKey, firstExpected, secondExpected)
}

func waitForChildProcessInstance(t testing.TB, parentProcessInstanceKey int64) zenclient.ProcessInstance {
	t.Helper()

	var child zenclient.ProcessInstance
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil {
			return false
		}
		if len(page.Partitions) == 0 || len(page.Partitions[0].Items) == 0 {
			return false
		}
		child = page.Partitions[0].Items[0]
		return true
	}, 1*time.Second, 100*time.Millisecond, "process instance %d should create a child process instance", parentProcessInstanceKey)
	return child
}
