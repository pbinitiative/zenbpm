package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const inclusiveGatewaySplitJoinPath = "testdata/gateway_inclusive/inclusive_gateway_split_join.bpmn"
const inclusiveGatewaySplitToEndEventsPath = "testdata/gateway_inclusive/inclusive_gateway_split_to_end_events.bpmn"
const inclusiveGatewayReentryPath = "testdata/gateway_inclusive/inclusive_gateway_reentry.bpmn"

func TestInclusiveGatewayFlow(t *testing.T) {
	t.Run("One active branch completes only the selected split path", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewaySplitToEndEventsPath, map[string]any{
			"routeA": true,
			"routeB": false,
			"routeC": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "inclusive_gateway_split", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"inclusive_gateway_split", "end_event_a"},
			[]string{"end_event_b", "end_event_c", "end_event_default"},
		)
	})

	t.Run("Multiple active branches complete all selected split paths", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewaySplitToEndEventsPath, map[string]any{
			"routeA": true,
			"routeB": true,
			"routeC": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "inclusive_gateway_split", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_b", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"inclusive_gateway_split", "end_event_a", "end_event_b"},
			[]string{"end_event_c", "end_event_default"},
		)
	})

	t.Run("Default branch is used when no condition matches", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewaySplitToEndEventsPath, map[string]any{
			"routeA": false,
			"routeB": false,
			"routeC": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "inclusive_gateway_split", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_default", runtime.TokenStateCompleted)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"inclusive_gateway_split", "end_event_default"},
			[]string{"end_event_a", "end_event_b", "end_event_c"},
		)
	})

	t.Run("Join continues after the only activated branch completes", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewaySplitJoinPath, map[string]any{
			"routeA": true,
			"routeB": false,
			"routeC": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_b")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_c")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_default")

		completeJobForElementId(t, processInstance.Key, "service_task_a", nil)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		completeJobForElementId(t, processInstance.Key, "service_task_after_join", nil)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
	})

	t.Run("Join waits until all selected branches complete", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewaySplitJoinPath, map[string]any{
			"routeA": true,
			"routeB": true,
			"routeC": false,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_c")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_default")

		completeJobForElementId(t, processInstance.Key, "service_task_a", nil)
		assertProcessInstanceTokenCount(t, processInstance.Key, "inclusive_gateway_join", 1)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		completeJobForElementId(t, processInstance.Key, "service_task_b", nil)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		completeJobForElementId(t, processInstance.Key, "service_task_after_join", nil)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
	})

	t.Run("Re-entering the split gateway completes after the loop condition changes", func(t *testing.T) {
		processInstance := deployAndCreateInclusiveGatewayInstance(t, inclusiveGatewayReentryPath, map[string]any{
			"loopCount": 0,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_loop")

		completeJobForElementId(t, processInstance.Key, "service_task_loop", map[string]any{
			"loopCount": 2,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_loop")
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_completed", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"Flow_start_to_gateway",
			"inclusive_gateway_loop",
			"Flow_gateway_to_loop_task",
			"service_task_loop",
			"Flow_loop_task_to_gateway",
			"inclusive_gateway_loop",
			"Flow_gateway_to_end",
			"end_event_completed",
		})
	})
}

func deployAndCreateInclusiveGatewayInstance(t testing.TB, filePath string, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	definition, err := deployTestDataDefinition(
		t,
		filePath,
		fmt.Sprintf("inclusive_gateway-%d", time.Now().UnixNano()),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.NotZero(t, definition.ProcessDefinitionKey)

	processInstance, err := createProcessInstance(t, &definition.ProcessDefinitionKey, variables)
	require.NoError(t, err)
	return processInstance
}
