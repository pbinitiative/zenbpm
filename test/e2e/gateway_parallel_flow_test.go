package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

const (
	parallelGatewayJoinPath                 = "testdata/gateway_parallel/parallel_gateway_split_and_join.bpmn"
	parallelGatewaySplitToEndPath           = "testdata/gateway_parallel/parallel_gateway_split_to_end_events.bpmn"
	parallelGatewayNestedPath               = "testdata/gateway_parallel/parallel_gateway_nested.bpmn"
	parallelGatewaySplitJoinToEndEventsPath = "testdata/gateway_parallel/parallel_gateways_split_join_to_end_events.bpmn"
)

func TestParallelGatewayFlow(t *testing.T) {
	t.Run("Split creates all outgoing paths", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayJoinPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_c")

		assertProcessInstanceTokenState(t, processInstance.Key, "parallel_gateway_split", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task_a", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task_b", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task_c", runtime.TokenStateWaiting)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")
	})

	t.Run("Join waits for all incoming paths", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayJoinPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_c")

		completeJobForElementId(t, processInstance.Key, "service_task_a", nil)

		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_join", 1)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")
	})

	t.Run("Split branches can end independently without a join", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewaySplitToEndPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")

		completeJobForElementId(t, processInstance.Key, "service_task_a", nil)

		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task_b", runtime.TokenStateWaiting)

		completeJobForElementId(t, processInstance.Key, "service_task_b", nil)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_a", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_b", runtime.TokenStateCompleted)
	})

	t.Run("Split can feed independent joins that end separately", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewaySplitJoinToEndEventsPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_top_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_top_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_bottom_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_bottom_b")

		completeJobForElementId(t, processInstance.Key, "service_task_top_a", nil)
		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_top_join", 1)
		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_bottom_join", 0)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"parallel_gateway_top_join", "service_task_top_b", "service_task_bottom_a", "service_task_bottom_b"},
			[]string{"end_event_top_join", "end_event_bottom_join"})

		completeJobForElementId(t, processInstance.Key, "service_task_bottom_a", nil)
		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_top_join", 1)
		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_bottom_join", 1)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"parallel_gateway_top_join", "parallel_gateway_bottom_join"},
			[]string{"end_event_top_join", "end_event_bottom_join"})

		completeJobForElementId(t, processInstance.Key, "service_task_top_b", nil)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_top_join", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task_bottom_b", runtime.TokenStateWaiting)
		assertProcessInstanceTokenElements(t, processInstance.Key,
			[]string{"end_event_top_join", "parallel_gateway_bottom_join", "service_task_bottom_b"},
			[]string{"end_event_bottom_join"})

		completeJobForElementId(t, processInstance.Key, "service_task_bottom_b", nil)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_top_join", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event_bottom_join", runtime.TokenStateCompleted)
	})

	t.Run("Completion order does not matter", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayJoinPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_c")

		completeJobsForElementIds(t, processInstance.Key, "service_task_c", "service_task_a", "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		completeJobForElementId(t, processInstance.Key, "service_task_after_join", nil)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
	})

	t.Run("Process continues only after the last branch finishes", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayJoinPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_c")

		completeJobsForElementIds(t, processInstance.Key, "service_task_a", "service_task_b")
		assertProcessInstanceTokenCount(t, processInstance.Key, "parallel_gateway_join", 2)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		completeJobForElementId(t, processInstance.Key, "service_task_c", nil)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_after_join")
	})

	t.Run("One failed branch prevents continuation", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayJoinPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_c")

		completeJobsForElementIds(t, processInstance.Key, "service_task_b", "service_task_c")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")

		failJobForElementId(t, processInstance.Key, "service_task_a", nil, nil)
		waitForProcessInstanceJobByElementId(t, processInstance.Key, "service_task_a", public.JobStateFailed)

		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"service_task_a", "parallel_gateway_join"}, []string{"service_task_after_join", "end_event"})
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_join")
	})

	t.Run("Nested parallel gateways work correctly", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayNestedPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_outer")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_inner_a")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_inner_b")

		completeJobForElementId(t, processInstance.Key, "service_task_inner_a", nil)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_inner_after")

		completeJobForElementId(t, processInstance.Key, "service_task_outer", nil)
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_outer_join")

		completeJobForElementId(t, processInstance.Key, "service_task_inner_b", nil)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_inner_after")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "service_task_after_outer_join")

		completeJobForElementId(t, processInstance.Key, "service_task_inner_after", nil)
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "service_task_after_outer_join")

		completeJobForElementId(t, processInstance.Key, "service_task_after_outer_join", nil)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
	})
}
