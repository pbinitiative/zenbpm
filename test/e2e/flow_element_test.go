package e2e

import (
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetFlowElementInstanceHistory(t *testing.T) {
	cleanProcessInstances(t)

	multiInstanceDefinition, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	callActivityDefinition, err := deployGetUniqueDefinition(t, "call-activity-simple.bpmn")
	assert.NoError(t, err)
	err = deployDefinition(t, "simple_task.bpmn")
	assert.NoError(t, err)

	subprocessDefinition, err := deployGetUniqueDefinition(t, "simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	var instance1Key int64
	var instance2Key int64
	var instance3Key int64
	t.Run("create process instances", func(t *testing.T) {
		instance1, err := createProcessInstance(t, multiInstanceDefinition.Key, map[string]any{
			"testInputCollection": []string{"test1", "test2", "test3"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance1.Key)
		instance1Key = instance1.Key

		instance2, err := createProcessInstance(t, callActivityDefinition.Key, map[string]any{
			"testVar": 123,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance2.Key)
		instance2Key = instance2.Key

		instance3, err := createProcessInstance(t, subprocessDefinition.Key, map[string]any{
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
		assert.Equal(t, 4, history.JSON200.TotalCount)
	})

	t.Run("get history callActivity", func(t *testing.T) {
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance2Key, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 3, history.JSON200.TotalCount)
	})

	t.Run("get history subprocess", func(t *testing.T) {
		history, err := app.restClient.GetHistoryWithResponse(t.Context(), instance3Key, &zenclient.GetHistoryParams{})
		assert.NoError(t, err)
		assert.Equal(t, 6, history.JSON200.TotalCount)
	})

	assert.NoError(t, err)
}
