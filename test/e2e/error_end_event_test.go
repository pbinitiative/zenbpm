package e2e

import (
	"fmt"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorEndEventInSubprocess(t *testing.T) {
	t.Run("catch_all", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_catch_all_error_boundary.bpmn", "Error_end_event_catch_all")

		instance, err := createProcessInstance(t, &definitionKey, map[string]any{
			"variable_name": "test-value",
		})
		require.NoError(t, err)
		require.NotEmpty(t, instance.Key)

		childInstance := waitForChildProcessInstance(t, instance.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)

		processInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, processInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, processInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("default"), processInstance.ProcessType)
		assert.Nil(t, processInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, processInstance.ActiveElementInstances, nil)

		childProcessInstance, err := getProcessInstance(t, childInstance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, childProcessInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateTerminated, childProcessInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("subprocess"), childProcessInstance.ProcessType)
		require.NotNil(t, childProcessInstance.ParentProcessInstanceKey)
		assert.Equal(t, instance.Key, *childProcessInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, childProcessInstance.ActiveElementInstances, nil)

		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceTokenElements(t, instance.Key, []string{"handled_end"}, []string{"Event_should_not_happen"})
		assertProcessInstanceTokenElements(t, childInstance.Key, []string{"error_end_event"}, nil)
		assertProcessTokensExact(t, instance.Key, []processTokenExpectation{
			{elementID: "handled_end", state: bpmnruntime.TokenStateCompleted},
		})
		assertProcessTokensExact(t, childInstance.Key, []processTokenExpectation{
			{elementID: "error_end_event", state: bpmnruntime.TokenStateCompleted},
		})
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_matching_error_boundary.bpmn", "Error_end_event_catch_by_code")

		instance, err := createProcessInstance(t, &definitionKey, map[string]any{
			"variable_name": "test-value",
		})
		require.NoError(t, err)
		require.NotEmpty(t, instance.Key)

		childInstance := waitForChildProcessInstance(t, instance.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)

		processInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, processInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, processInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("default"), processInstance.ProcessType)
		assert.Nil(t, processInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, processInstance.ActiveElementInstances, nil)

		childProcessInstance, err := getProcessInstance(t, childInstance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, childProcessInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateTerminated, childProcessInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("subprocess"), childProcessInstance.ProcessType)
		require.NotNil(t, childProcessInstance.ParentProcessInstanceKey)
		assert.Equal(t, instance.Key, *childProcessInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, childProcessInstance.ActiveElementInstances, nil)

		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceTokenElements(t, instance.Key, []string{"handled_end"}, []string{"Event_should_not_happen"})
		assertProcessInstanceTokenElements(t, childInstance.Key, []string{"error_end_event"}, nil)
		assertProcessTokensExact(t, instance.Key, []processTokenExpectation{
			{elementID: "handled_end", state: bpmnruntime.TokenStateCompleted},
		})
		assertProcessTokensExact(t, childInstance.Key, []processTokenExpectation{
			{elementID: "error_end_event", state: bpmnruntime.TokenStateCompleted},
		})
	})

	t.Run("incident_on_nonmatching_error_code", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_end_event/sub_process/simple_sub_process_task_with_error_end_event_and_nonmatching_error_boundary.bpmn", "Error_end_event_incident")

		instance, err := createProcessInstance(t, &definitionKey, map[string]any{
			"variable_name": "test-value",
		})
		require.NoError(t, err)
		require.NotEmpty(t, instance.Key)

		childInstance := waitForChildProcessInstance(t, instance.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		waitForTwoProcessInstanceStates(t, instance.Key, zenclient.ProcessInstanceStateFailed, childInstance.Key, zenclient.ProcessInstanceStateTerminated)

		processInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, processInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateFailed, processInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("default"), processInstance.ProcessType)
		assert.Nil(t, processInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, processInstance.ActiveElementInstances, []string{"Subprocess:TokenStateFailed"})

		childProcessInstance, err := getProcessInstance(t, childInstance.Key)
		require.NoError(t, err)
		require.NotEmpty(t, childProcessInstance.Key)
		assert.Equal(t, zenclient.ProcessInstanceStateTerminated, childProcessInstance.State)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("subprocess"), childProcessInstance.ProcessType)
		require.NotNil(t, childProcessInstance.ParentProcessInstanceKey)
		assert.Equal(t, instance.Key, *childProcessInstance.ParentProcessInstanceKey)
		assertProcessActiveElementInstancesExact(t, childProcessInstance.ActiveElementInstances, nil)

		assertProcessInstanceIncidentsLength(t, instance.Key, 1)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)
		assertProcessInstanceTokenElements(t, instance.Key, nil, []string{"handled_end", "Event_should_not_happen"})
		assertProcessInstanceTokenElements(t, childInstance.Key, []string{"error_end_event"}, nil)
		assertProcessTokensExact(t, instance.Key, []processTokenExpectation{
			{elementID: "Subprocess", state: bpmnruntime.TokenStateFailed},
		})
		assertProcessTokensExact(t, childInstance.Key, []processTokenExpectation{
			{elementID: "error_end_event", state: bpmnruntime.TokenStateCompleted},
		})

		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, instance.Key, incidents[0].ProcessInstanceKey)
		assert.Equal(t, "Subprocess", incidents[0].ElementId)
		assert.Equal(t, processInstance.ActiveElementInstances[0].ElementId, incidents[0].ElementId)
		assert.Equal(t, processInstance.ActiveElementInstances[0].ElementInstanceKey, incidents[0].ElementInstanceKey)
	})
}

func TestErrorEndEventInNestedCallActivity(t *testing.T) {

	_, err := deployDefinitionWithJobType(t, "error_end_event/call_activity/call_activity_nested_with_error_end_event_leaf.bpmn", "nested_call_activity_error_end_event_leaf", map[string]string{
		"TestType": fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)

	_, err = deployDefinitionWithJobType(t, "error_end_event/call_activity/call_activity_nested_with_error_end_event_middle.bpmn", "nested_call_activity_error_end_event_middle", map[string]string{
		"TestType": fmt.Sprintf("call-activity-child-%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)

	t.Run("catch_all", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_end_event/call_activity/call_activity_nested_with_error_end_event_root_catch_all.bpmn", "nested_call_activity_error_end_event_root_catch_all")

		parentInstanceWithBoundaryEvent := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		})

		waitForProcessInstanceState(t, parentInstanceWithBoundaryEvent.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, parentInstanceWithBoundaryEvent.Key, "root-handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstanceWithBoundaryEvent.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstanceWithBoundaryEvent.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstanceWithBoundaryEvent.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstanceWithBoundaryEvent.Key, []string{"root-handled-end"}, []string{"should-not-happen-end"})

		middleInstance := waitForChildProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		waitForProcessInstanceState(t, middleInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, middleInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, middleInstance.Key)

		leafInstanceWithErrorEndEvent := waitForChildProcessInstance(t, middleInstance.Key)
		waitForProcessInstanceState(t, leafInstanceWithErrorEndEvent.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, leafInstanceWithErrorEndEvent.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstanceWithErrorEndEvent.Key)
	})

	t.Run("matching_error_code_is_caught", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_end_event/call_activity/call_activity_nested_with_error_end_event_root_matching.bpmn", "nested_call_activity_error_end_event_root_matching")

		parentInstanceWithBoundaryEvent := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		})

		waitForProcessInstanceState(t, parentInstanceWithBoundaryEvent.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceTokenState(t, parentInstanceWithBoundaryEvent.Key, "root-handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceVariables(t, parentInstanceWithBoundaryEvent.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstanceWithBoundaryEvent.Key, 0)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstanceWithBoundaryEvent.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstanceWithBoundaryEvent.Key, []string{"root-handled-end"}, []string{"should-not-happen-end"})

		middleInstance := waitForChildProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		waitForProcessInstanceState(t, middleInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, middleInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, middleInstance.Key)

		leafInstanceWithErrorEndEvent := waitForChildProcessInstance(t, middleInstance.Key)
		waitForProcessInstanceState(t, leafInstanceWithErrorEndEvent.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, leafInstanceWithErrorEndEvent.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstanceWithErrorEndEvent.Key)
	})

	t.Run("unmatched_error_creates_incident", func(t *testing.T) {
		parentDefinitionKey := deployProcessDefinitionKey(t, "error_end_event/call_activity/call_activity_nested_with_error_end_event_root_nonmatching.bpmn", "nested_call_activity_error_end_event_root_nonmatching")

		parentInstanceWithBoundaryEvent := createProcessInstanceWithDefaultVariables(t, parentDefinitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		})

		waitForProcessInstanceState(t, parentInstanceWithBoundaryEvent.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, parentInstanceWithBoundaryEvent.Key, "call_activity", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceVariables(t, parentInstanceWithBoundaryEvent.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, parentInstanceWithBoundaryEvent.Key, 1)
		assertProcessInstanceErrorSubscriptionCount(t, parentInstanceWithBoundaryEvent.Key, 0, 1)
		assertProcessInstanceTokenElements(t, parentInstanceWithBoundaryEvent.Key, []string{"call_activity"}, nil)

		middleInstance := waitForChildProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		waitForProcessInstanceState(t, middleInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, middleInstance.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, middleInstance.Key)

		leafInstanceWithErrorEndEvent := waitForChildProcessInstance(t, parentInstanceWithBoundaryEvent.Key)
		waitForProcessInstanceState(t, leafInstanceWithErrorEndEvent.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIncidentsLength(t, leafInstanceWithErrorEndEvent.Key, 0)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, leafInstanceWithErrorEndEvent.Key)

		incidents, err := getProcessInstanceIncidents(t, parentInstanceWithBoundaryEvent.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, parentInstanceWithBoundaryEvent.Key, incidents[0].ProcessInstanceKey)
		assert.Equal(t, "call_activity", incidents[0].ElementId)
	})
}

func TestErrorEndEventWithUserTask(t *testing.T) {
	t.Run("without_error_boundary", func(t *testing.T) {
		definitionKey := deployProcessDefinitionKey(t, "error_end_event/user_task/user_task_with_error_end_event_and_without_error_boundary.bpmn", "simple_user_task_with_error_end_event_and_without_boundary_event")

		processInstance := createProcessInstanceWithDefaultVariables(t, definitionKey)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		job := waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task")
		err := completeJob(t, job.Key, nil)
		assert.NoError(t, err)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", bpmnruntime.TokenStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"variable_name": "test-value"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceErrorSubscriptionsCountIsZero(t, processInstance.Key)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
	})
}

type processTokenExpectation struct {
	elementID string
	state     bpmnruntime.TokenState
}

func getAllProcessInstanceTokens(t testing.TB, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	t.Helper()

	partitionStore, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get partition store for process instance %d: %w", processInstanceKey, err)
	}

	tokens, err := partitionStore.GetAllTokensForProcessInstance(t.Context(), processInstanceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get tokens for process instance %d: %w", processInstanceKey, err)
	}

	return tokens, nil
}

func assertProcessTokensExact(t testing.TB, processInstanceKey int64, expected []processTokenExpectation) {
	t.Helper()

	tokens, err := getAllProcessInstanceTokens(t, processInstanceKey)
	require.NoError(t, err)

	actualSnapshot := make([]string, 0, len(tokens))
	for _, token := range tokens {
		actualSnapshot = append(actualSnapshot, fmt.Sprintf("%s:%s", token.ElementId, token.State.String()))
	}

	expectedSnapshot := make([]string, 0, len(expected))
	for _, expectedToken := range expected {
		expectedSnapshot = append(expectedSnapshot, fmt.Sprintf("%s:%s", expectedToken.elementID, expectedToken.state.String()))
	}

	assert.ElementsMatch(t, expectedSnapshot, actualSnapshot)
}

func assertProcessActiveElementInstancesExact(t testing.TB, actual []zenclient.ElementInstance, expected []string) {
	t.Helper()

	if expected == nil {
		expected = []string{}
	}

	for _, elementInstance := range actual {
		assert.NotZero(t, elementInstance.ElementInstanceKey)
		assert.False(t, elementInstance.CreatedAt.IsZero())
	}

	actualSnapshot := make([]string, 0, len(actual))
	for _, elementInstance := range actual {
		actualSnapshot = append(actualSnapshot, fmt.Sprintf("%s:%s", elementInstance.ElementId, elementInstance.State))
	}

	assert.ElementsMatch(t, expected, actualSnapshot)
}
