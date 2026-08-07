package e2e

import (
	"net/http"
	"sort"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/require"
)

type flowElementHistoryState string

const (
	flowElementHistoryStateActive    flowElementHistoryState = "active"
	flowElementHistoryStateCompleted flowElementHistoryState = "completed"
)

type flowElementHistoryExpectation struct {
	elementID string
	state     flowElementHistoryState
}

func getFlowElementByElementId(t testing.TB, processInstanceKey int64, elementId string) runtime.FlowElementInstance {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
	require.NoError(t, err)

	for _, flowElement := range flowElements {
		if flowElement.ElementId == elementId {
			return flowElement
		}
	}

	t.Fatalf("Flow element %s not found in process instance %d", elementId, processInstanceKey)
	return runtime.FlowElementInstance{}
}

func assertFlowElementInputVariables(t testing.TB, processInstanceKey int64, elementId string, expectedVariables map[string]any) {
	t.Helper()

	flowElement := getFlowElementByElementId(t, processInstanceKey, elementId)

	require.Equal(t, expectedVariables, flowElement.InputVariables)
}

func assertFlowElementOutputVariables(t testing.TB, processInstanceKey int64, elementId string, expectedVariables map[string]any) {
	t.Helper()

	flowElement := getFlowElementByElementId(t, processInstanceKey, elementId)

	require.Equal(t, expectedVariables, flowElement.OutputVariables)
}

func assertExactCompletedProcessInstanceHistory(t testing.TB, processInstanceKey int64, expectedElementIDs []string) {
	t.Helper()

	expectedHistory := make([]flowElementHistoryExpectation, 0, len(expectedElementIDs))
	for _, elementID := range expectedElementIDs {
		expectedHistory = append(expectedHistory, completedFlowElementHistory(elementID))
	}

	assertExactProcessInstanceHistoryStates(t, processInstanceKey, expectedHistory...)
}

func assertExactProcessInstanceHistoryStates(t testing.TB, processInstanceKey int64, expectedHistory ...flowElementHistoryExpectation) {
	t.Helper()

	const pageSize int32 = 100
	history := make([]zenclient.FlowElementHistory, 0, len(expectedHistory))
	for page := int32(1); ; page++ {
		response, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{
			Page: new(page),
			Size: new(pageSize),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())
		require.NotNil(t, response.JSON200)
		require.NotNil(t, response.JSON200.Items)
		require.Equal(t, len(expectedHistory), response.JSON200.TotalCount, "history should contain exactly the expected flow elements")

		history = append(history, *response.JSON200.Items...)
		require.LessOrEqual(t, len(history), response.JSON200.TotalCount, "history pages should not contain more than the reported total count")
		if len(history) == response.JSON200.TotalCount {
			break
		}
		require.NotEmpty(t, *response.JSON200.Items, "history page %d returned no items before the total count was reached", page)
	}

	require.Len(t, history, len(expectedHistory))
	sort.Slice(history, func(i, j int) bool {
		if history[i].CreatedAt.Equal(history[j].CreatedAt) {
			return history[i].Key < history[j].Key
		}
		return history[i].CreatedAt.Before(history[j].CreatedAt)
	})

	for index, expected := range expectedHistory {
		actual := history[index]
		require.Equalf(t, expected.elementID, actual.ElementId, "unexpected flow element at history index %d", index)

		switch expected.state {
		case flowElementHistoryStateActive:
			require.Nilf(t, actual.CompletedAt, "flow element %s at history index %d should be active", actual.ElementId, index)
		case flowElementHistoryStateCompleted:
			require.NotNilf(t, actual.CompletedAt, "flow element %s at history index %d should be completed", actual.ElementId, index)
			require.Falsef(t, actual.CompletedAt.Before(actual.CreatedAt), "flow element %s completed before it was created", actual.ElementId)
		default:
			require.Failf(t, "unsupported flow element history state", "state %q for element %s is not supported", expected.state, expected.elementID)
		}
	}
}

func activeFlowElementHistory(elementID string) flowElementHistoryExpectation {
	return flowElementHistoryExpectation{elementID: elementID, state: flowElementHistoryStateActive}
}

func completedFlowElementHistory(elementID string) flowElementHistoryExpectation {
	return flowElementHistoryExpectation{elementID: elementID, state: flowElementHistoryStateCompleted}
}
