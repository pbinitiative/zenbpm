package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/require"
)

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
