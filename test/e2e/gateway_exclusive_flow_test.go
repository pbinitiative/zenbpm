package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

func TestExclusiveGatewayFlow(t *testing.T) {
	t.Run("Low amount completes through the low amount path and records history", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn",
			map[string]any{"amount": 99},
			"Flow_less_than_100",
			"end_event_less_than_100",
			[]string{"end_event_between_100_1000", "end_event_between_150_200"},
		)
	})

	t.Run("Boundary lower amount completes through the medium amount path and records history", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn",
			map[string]any{"amount": 100},
			"Flow_between_100_1000",
			"end_event_between_100_1000",
			[]string{"end_event_less_than_100", "end_event_between_150_200"},
		)
	})

	t.Run("Boundary upper amount completes through the medium amount path and records history", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn",
			map[string]any{"amount": 999},
			"Flow_between_100_1000",
			"end_event_between_100_1000",
			[]string{"end_event_less_than_100", "end_event_between_150_200"},
		)
	})

	t.Run("Multiple matching conditions complete through the first matching path only", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_amount_based_routing.bpmn",
			map[string]any{"amount": 175},
			"Flow_between_100_1000",
			"end_event_between_100_1000",
			[]string{"end_event_less_than_100", "end_event_between_150_200"},
		)
	})

	t.Run("Matching condition completes through the success path instead of the default path", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_with_default_flow_simple.bpmn",
			map[string]any{"isSuccess": true},
			"Flow_yes",
			"end_event_success",
			[]string{"end_event_failure"},
		)
	})

	t.Run("False condition completes through the default path and records history", func(t *testing.T) {

		assertExclusiveGatewayCompletedPath(t,
			"testdata/gateway_exclusive/exclusive_gateway_with_default_flow_simple.bpmn",
			map[string]any{"isSuccess": false},
			"Flow_no",
			"end_event_failure",
			[]string{"end_event_success"},
		)
	})

	t.Run("Selected branch merges through the converging exclusive gateway", func(t *testing.T) {

		assertExclusiveGatewayMergedPath(t,
			map[string]any{"approved": true},
			"Flow_approved",
		)
	})

	t.Run("Default branch merges through the converging exclusive gateway", func(t *testing.T) {

		assertExclusiveGatewayMergedPath(t,
			map[string]any{"approved": false},
			"Flow_rejected",
		)
	})
}

func assertExclusiveGatewayCompletedPath(t *testing.T, filePath string, variables map[string]any, expectedFlow string, expectedEndEvent string, notExpectedEndEvents []string) {
	t.Helper()

	processInstance := deployAndCreateUniqueProcessDefinition(t, filePath, variables)

	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{expectedEndEvent}, notExpectedEndEvents)
	assertProcessInstanceTokenState(t, processInstance.Key, expectedEndEvent, runtime.TokenStateCompleted)
	assertExactProcessInstanceHistory(t, processInstance.Key, []string{
		"StartEvent_1",
		"Flow_035xjey",
		"exclusive_gateway",
		expectedFlow,
		expectedEndEvent,
	})
}

func assertExclusiveGatewayMergedPath(t *testing.T, variables map[string]any, expectedFlow string) {
	t.Helper()

	processInstance := deployAndCreateUniqueProcessDefinition(t,
		"testdata/gateway_exclusive/exclusive_gateway_merge.bpmn",
		variables,
	)

	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
	assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
	assertExactProcessInstanceHistory(t, processInstance.Key, []string{
		"StartEvent_1",
		"Flow_start_to_decision",
		"decision_gateway",
		expectedFlow,
		"merge_gateway",
		"Flow_to_end",
		"end_event",
	})
}
