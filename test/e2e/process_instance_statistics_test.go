package e2e

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetProcessInstanceElementStatistics(t *testing.T) {
	definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
	require.NoError(t, err)

	t.Run("active token exists while blocked at service task at element `service-task-1`", func(t *testing.T) {
		instance, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		totalActive, totalIncidents := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 1, totalActive, "blocked instance should have exactly one active token")
		assert.Equal(t, 0, totalIncidents)

		assert.Equal(t, 1, resp.JSON200.Partitions[0].Items["service-task-1"].ActiveCount)
	})

	t.Run("active tokens are zero for a completed process instance", func(t *testing.T) {
		_, err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
		require.NoError(t, err)

		def, err := deployGetUniqueDefinition(t, "simple-business-rule-task-local.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, ptr.To(def.Key), map[string]any{})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		totalActive, totalIncidents := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 0, totalActive, "completed instance should have no active tokens")
		assert.Equal(t, 0, totalIncidents)
	})

	t.Run("statistics are scoped to the given process instance", func(t *testing.T) {
		instance1, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance1.Key)

		instance2, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{"testVar": 2})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance2.Key)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance1.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		totalActive, _ := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 1, totalActive, "only instance1 token should appear")
	})

	t.Run("active count is zero after instance is cancelled", func(t *testing.T) {
		instance, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		totalActive, _ := sumElementStatistics(resp.JSON200)
		assert.Equal(t, 1, totalActive, "one active token before cancel")

		_, err = app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)

		resp, err = app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		totalActive, _ = sumElementStatistics(resp.JSON200)
		assert.Equal(t, 0, totalActive, "no active tokens after cancel")
	})

	t.Run("incident count appears for process instance with incident", func(t *testing.T) {
		incidentDef, err := deployGetUniqueDefinition(t, "exclusive-gateway-with-condition.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, ptr.To(incidentDef.Key), map[string]any{"price": 0})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		_, totalIncidents := sumElementStatistics(resp.JSON200)
		assert.Equal(t, totalIncidents, 1, "should have exact one incident")
	})
}

func TestGetProcessInstanceElementStatisticsCompletedAndTerminated(t *testing.T) {
	t.Run("completedCount equals number of finished parallel multi-instance iterations", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "multi_instance_parallel_service_task.bpmn")
		require.NoError(t, err)

		testInputCollection := []string{"a", "b", "c"}
		instance, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{
			"testInputCollection": testInputCollection,
		})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key) //nolint:errcheck

		require.Eventually(t, func() bool {
			resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
			return err == nil && resp.JSON200 != nil &&
				resp.JSON200.Partitions[0].Items["Activity_0rae016"].ActiveCount == len(testInputCollection)
		}, 5*time.Second, 50*time.Millisecond)

		var jobs zenclient.JobPartitionPage
		require.Eventually(t, func() bool {
			var e error
			jobs, e = readWaitingJobs(t, "TestType")
			return e == nil && len(jobs.Partitions) > 0 && jobs.Partitions[0].Items != nil && len(jobs.Partitions[0].Items) == len(testInputCollection)
		}, 5*time.Second, 50*time.Millisecond)

		for _, job := range jobs.Partitions[0].Items {
			err = completeJob(t, job, map[string]any{})
			require.NoError(t, err)
		}

		var stats *zenclient.ElementStatisticsPartitions
		require.Eventually(t, func() bool {
			resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
			if err != nil || resp.JSON200 == nil {
				return false
			}
			stats = resp.JSON200
			return ptr.Deref(stats.Partitions[0].Items["Activity_0rae016"].CompletedCount, 0) == len(testInputCollection)
		}, 5*time.Second, 50*time.Millisecond,
			"Activity_0rae016 completedCount should equal the number of iterations")

		assert.Equal(t, 0, stats.Partitions[0].Items["Activity_0rae016"].ActiveCount)
		assert.Equal(t, 0, ptr.Deref(stats.Partitions[0].Items["Activity_0rae016"].TerminatedCount, 0))
	})

	t.Run("terminatedCount increases after instance is cancelled", func(t *testing.T) {
		definition, err := deployGetUniqueDefinition(t, "service-task-input-output.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{"testVar": 1})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 1, resp.JSON200.Partitions[0].Items["service-task-1"].ActiveCount)
		assert.Equal(t, 0, ptr.Deref(resp.JSON200.Partitions[0].Items["service-task-1"].TerminatedCount, 0))

		_, err = app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)

		resp, err = app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 0, resp.JSON200.Partitions[0].Items["service-task-1"].ActiveCount)
		assert.Equal(t, 1, ptr.Deref(resp.JSON200.Partitions[0].Items["service-task-1"].TerminatedCount, 0))
		assert.Equal(t, 0, ptr.Deref(resp.JSON200.Partitions[0].Items["service-task-1"].CompletedCount, 0))
	})

	t.Run("terminatedCount appears for token cancelled by terminate end event", func(t *testing.T) {
		termDef, err := deployGetDefinition(t, "parallel_flow_with_terminate_end_task.bpmn", "parallel_flow_with_terminate_end_task")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, ptr.To(termDef.Key), map[string]any{})
		require.NoError(t, err)

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)

		assert.Equal(t, 0, resp.JSON200.Partitions[0].Items["task-id"].ActiveCount)
		assert.Equal(t, 1, ptr.Deref(resp.JSON200.Partitions[0].Items["task-id"].TerminatedCount, 0),
			"task-id token should be counted as terminated because the terminate end event cancelled it")
	})
}

func TestGetProcessInstanceElementStatisticsMultiInstance(t *testing.T) {
	definition, err := deployGetUniqueDefinition(t, "multi_instance_parallel_service_task.bpmn")
	require.NoError(t, err)

	t.Run("parallel multi-instance shows body tokens not scope token", func(t *testing.T) {
		testInputCollection := []string{"a", "b", "c"}
		testInputCollectionLen := len(testInputCollection)
		instance, err := createProcessInstance(t, ptr.To(definition.Key), map[string]any{
			"testInputCollection": testInputCollection,
		})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key) //nolint:errcheck
		var lastError error
		var activeByElement map[string]int

		ok := assert.Eventually(t, func() bool {
			lastError = nil
			resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
			if err != nil {
				lastError = err
				return false
			}
			if resp.JSON200 == nil {
				lastError = errors.New("JSON200 is nil")
				return false
			}

			result := collectActiveByElement(resp.JSON200)
			activeByElement = result
			return result["Activity_0rae016"] == testInputCollectionLen
		}, 5*time.Second, 100*time.Millisecond, "should count child body tokens, not the parent scope token")
		if !ok {
			t.Logf("lastError=%v, activeTokenCount=%d, expectedTokenCount=%d",
				lastError,
				activeByElement["Activity_0rae016"],
				testInputCollectionLen)
		}
	})

	t.Run("sequential multi-instance shows body token not scope token", func(t *testing.T) {
		seqDef, err := deployGetUniqueDefinition(t, "multi_instance_service_task.bpmn")
		require.NoError(t, err)

		instance, err := createProcessInstance(t, ptr.To(seqDef.Key), map[string]any{
			"testInputCollection": []string{"a", "b", "c"},
		})
		require.NoError(t, err)
		defer app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key) //nolint:errcheck

		resp, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(t.Context(), instance.Key)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		activeByElement := collectActiveByElement(resp.JSON200)
		assert.Equal(t, 1, activeByElement["Activity_0rae016"],
			"sequential multi-instance should show 1 active body token")
	})
}
