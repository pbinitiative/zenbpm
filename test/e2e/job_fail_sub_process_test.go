package e2e

import (
	"testing"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestGrpcJobFailOnSubProcess(t *testing.T) {
	t.Run("catch_all_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_catch_all.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process catch all", ptr.To("any-error"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_and_output_mapping.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process matched error with output mapping", ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process matched error", ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailActiveJobViaGrpc(t, job, "grpc sub process unmatched error", ptr.To("99"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "subprocess_1", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"subprocess_1"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1"})
	})
}

func TestRestJobFailOnSubProcess(t *testing.T) {
	t.Run("catch_all_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_catch_all.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("any-error"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event_and_output_mapping.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"handled-end"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1", "Flow_01g5l10", "handled-end"})
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		definitionKey := deployUniqueSubProcessDefinitionKey(t, "error_events/sub_process/subprocess_with_error_boundery_event.bpmn")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)

		innerProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, innerProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("99"))

		waitForProcessInstanceState(t, innerProcess.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, innerProcess.Key, "service_task", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, innerProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, innerProcess.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, innerProcess.Key)
		assertProcessInstanceTokenElements(t, innerProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, innerProcess.Key, []string{"Flow_0921sm2", "Event_0r3npi7", "service_task"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "subprocess_1", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"subprocess_1"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_08y636o", "Event_18sxgar", "subprocess_1"})
	})

	t.Run("nested_matched_error_is_caught_and_propagates_all_variables_to_catching_scope", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn", "nested_subprocess_with_error_boundery_event")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChildProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		secondChildProcess := waitForChildProcessInstance(t, firstChildProcess.Key, 0)
		serviceTaskProcess := waitForChildProcessInstance(t, secondChildProcess.Key, 0)

		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 1, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)

		job := waitForProcessInstanceActiveJobByElementId(t, serviceTaskProcess.Key, "service_task")
		callFailJobViaRest(t, job.Key, ptr.To("54"))

		waitForProcessInstanceState(t, serviceTaskProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, serviceTaskProcess.Key, "service_task", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceVariables(t, serviceTaskProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, serviceTaskProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, serviceTaskProcess.Key)
		assertProcessInstanceTokenElements(t, serviceTaskProcess.Key, []string{"service_task"}, []string{"handled-end", "should-not-happen-end"})
		assertProcessInstanceHistory(t, serviceTaskProcess.Key, []string{"Flow_0lr9l93", "Event_0vhkfv0", "service_task", "Event_0pbbln3"})

		waitForProcessInstanceState(t, secondChildProcess.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceVariables(t, secondChildProcess.Key, map[string]interface{}{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, secondChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, secondChildProcess.Key)
		assertProcessInstanceHistory(t, secondChildProcess.Key, []string{"Flow_0921sm2", "start_subprocess_0", "subprocess_0"})

		waitForProcessInstanceState(t, firstChildProcess.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, firstChildProcess.Key, map[string]interface{}{"variable_from_request": "request_variable", "variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, firstChildProcess.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, firstChildProcess.Key, 0, 1)
		assertProcessInstanceHistory(t, firstChildProcess.Key, []string{"Flow_1ijsfip", "start_subprocess_1", "subprocess_1", "Flow_1nd1fpn", "Event_09haznn"})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "complete", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"complete"}, []string{})
		assertProcessInstanceHistory(t, processInstance.Key, []string{"Flow_07q4pdb", "start_subprocess_2", "subprocess_2", "Flow_17ropai", "complete"})
	})
}

func deployUniqueSubProcessDefinitionKey(t *testing.T, filename string) int64 {
	t.Helper()

	definition, err := deployGetUniqueDefinition(t, filename)
	require.NoError(t, err)
	require.NotZero(t, definition.Key)
	return definition.Key
}
