package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFlowElementInstanceHistory(t *testing.T) {
	cleanProcessInstances(t)

	multiInstanceDefinition, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	callActivityDefinition, err := deployGetUniqueDefinition(t, "call-activity-simple.bpmn")
	assert.NoError(t, err)
	_, err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition, err := deployGetUniqueDefinition(t, "simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	var instance1Key int64
	var instance2Key int64
	var instance3Key int64
	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, &multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
		instance1Key = instance1.Key

		instance2, err := createProcessInstance(t, &callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
		instance2Key = instance2.Key

		instance3, err := createProcessInstance(t, &subprocessDefinition.Key, map[string]any{
			"variable_name": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance3.Key)
		instance3Key = instance3.Key
	})

	time.Sleep(500 * time.Millisecond)

	t.Run("get history multiInstance", func(t *testing.T) {
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance1Key, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
	})

	t.Run("get child history multiInstance", func(t *testing.T) {
		children, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance1Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, children.JSON200.TotalCount)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("multiInstance"), children.JSON200.Partitions[0].Items[0].ProcessType)
		childKey := children.JSON200.Partitions[0].Items[0].Key
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), childKey, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, history.JSON200.TotalCount)
	})

	t.Run("get history callActivity", func(t *testing.T) {
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance2Key, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
		require.NotNil(t, history.JSON200.Items)
		for _, item := range *history.JSON200.Items {
			if item.ElementId == "callActivity" {
				assert.Equal(t, "CALL_ACTIVITY", item.ElementType)
				return
			}
		}
		assert.Fail(t, "call activity should be present in history")
	})

	t.Run("get child history callActivity", func(t *testing.T) {
		children, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance2Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, children.JSON200.TotalCount)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("callActivity"), children.JSON200.Partitions[0].Items[0].ProcessType)
		childKey := children.JSON200.Partitions[0].Items[0].Key
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), childKey, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
	})

	t.Run("get history subprocess", func(t *testing.T) {
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance3Key, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
		require.NotNil(t, history.JSON200.Items)
		for _, item := range *history.JSON200.Items {
			if item.ElementId == "Activity_1f5yxes" {
				assert.Equal(t, "SUB_PROCESS", item.ElementType)
				return
			}
		}
		assert.Fail(t, "subprocess should be present in history")
	})

	t.Run("get child history subprocess", func(t *testing.T) {
		children, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance3Key, &zenclient.GetChildProcessInstancesParams{})
		assert.NoError(t, err)
		assert.Equal(t, 1, children.JSON200.TotalCount)
		assert.Equal(t, zenclient.ProcessInstanceProcessType("subprocess"), children.JSON200.Partitions[0].Items[0].ProcessType)
		childKey := children.JSON200.Partitions[0].Items[0].Key
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), childKey, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
	})

	assert.NoError(t, err)
}

// TestGetFlowElementHistoryCompletedAtAndVariables verifies that the /history REST endpoint
// surfaces the new `completedAt`, `inputVariables` and `outputVariables` fields after a
// process executes, including synchronous elements that complete immediately.
func TestGetFlowElementHistoryCompletedAtAndVariables(t *testing.T) {
	cleanProcessInstances(t)

	definition, err := deployGetUniqueDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, nil)
	assert.NoError(t, err)

	// complete the service task job with an output variable that is mapped via the
	// BPMN output mapping (source =variable_name -> target variable_name)
	completeJobForElementId(t, instance.Key, "id", map[string]any{"variable_name": "done"})

	history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance.Key, &zenclient.GetHistoryParams{})
	assert.NoError(t, err)
	require.NotNil(t, history.JSON200)

	items := []zenclient.FlowElementHistory{}
	if history.JSON200.Items != nil {
		items = *history.JSON200.Items
	}
	assert.NotEmpty(t, items)

	byElementID := make(map[string]zenclient.FlowElementHistory, len(items))
	for _, item := range items {
		byElementID[item.ElementId] = item
	}

	// service task completes via job completion -> CompletedAt set, output variables written
	task, ok := byElementID["id"]
	assert.True(t, ok, "service task 'id' should be present in history")
	assert.NotNil(t, task.CompletedAt, "service task should have CompletedAt set after job completion")
	assert.NotNil(t, task.OutputVariables, "service task should have OutputVariables set after completion")
	if task.OutputVariables != nil {
		assert.Equal(t, map[string]any{"variable_name": "done"}, *task.OutputVariables,
			"service task output variables should reflect the mapped job output")
	}
	assert.NotNil(t, task.InputVariables, "service task should carry InputVariables")

	for _, elementID := range []string{"StartEvent_1", "Flow_0xt1d7q", "Flow_1vz4oo2"} {
		fe, found := byElementID[elementID]
		assert.True(t, found, "element %s should be present in history", elementID)
		assert.NotNil(t, fe.CompletedAt, "element %s should have CompletedAt after execution passes it", elementID)
		assert.Nil(t, fe.OutputVariables, "element %s should have nil OutputVariables", elementID)
		assert.NotNil(t, fe.InputVariables, "element %s should always carry InputVariables", elementID)
	}

	endEvent, ok := byElementID["Event_1j4mcqg"]
	assert.True(t, ok, "end event 'Event_1j4mcqg' should be present in history")
	assert.NotNil(t, endEvent.CompletedAt, "plain end event should have CompletedAt set when the process completes")
}

// TestGetFlowElementHistoryElementType verifies that the /history REST endpoint
// surfaces the new `elementType` field for every recorded flow element and that
// it matches the underlying BPMN element type.
func TestGetFlowElementHistoryElementType(t *testing.T) {
	cleanProcessInstances(t)

	definition, err := deployGetUniqueDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, nil)
	assert.NoError(t, err)

	// complete the service task job so the process ends and all flow elements
	// (including the trailing sequence flow and end event) are recorded
	completeJobForElementId(t, instance.Key, "id", map[string]any{"variable_name": "done"})

	history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance.Key, &zenclient.GetHistoryParams{})
	assert.NoError(t, err)
	require.NotNil(t, history.JSON200)

	items := []zenclient.FlowElementHistory{}
	if history.JSON200.Items != nil {
		items = *history.JSON200.Items
	}
	assert.NotEmpty(t, items)

	byElementID := make(map[string]zenclient.FlowElementHistory, len(items))
	for _, item := range items {
		byElementID[item.ElementId] = item
	}

	expectedElementTypes := map[string]string{
		"StartEvent_1":  "START_EVENT",
		"Flow_0xt1d7q":  "SEQUENCE_FLOW",
		"id":            "SERVICE_TASK",
		"Flow_1vz4oo2":  "SEQUENCE_FLOW",
		"Event_1j4mcqg": "END_EVENT",
	}

	for elementID, expectedType := range expectedElementTypes {
		fe, found := byElementID[elementID]
		assert.True(t, found, "element %s should be present in history", elementID)
		require.NotNil(t, fe.ElementType, "element %s should have ElementType set", elementID)
		assert.Equal(t, expectedType, fe.ElementType,
			"element %s should be reported as %s", elementID, expectedType)
	}
}
