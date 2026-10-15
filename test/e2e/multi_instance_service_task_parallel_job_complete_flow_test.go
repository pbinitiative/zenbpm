package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const multiInstanceServiceTaskParallelWithOutputMappingBpmn = "testdata/multi_instance/multi_instance_service_task_parallel_with_output_mapping.bpmn"

func TestParallelMultiInstanceServiceTaskJobCompleteFlow(t *testing.T) {
	t.Run("Completing every concurrent iteration completes the parent and child with exact history", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskParallelWithOutputMappingBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeMultiInstance, multiInstanceProcess.ProcessType)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", len(items))

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateActive,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateActive,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
		assertProcessInstanceTokenStates(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting, len(items))
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"service_task",
			"service_task",
			"service_task",
		})

		for i, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, map[string]any{"outcome": i}))
			if remaining := len(jobs) - 1 - i; remaining > 0 {
				// once the completion is processed, the parent must still wait for the remaining iterations
				waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", remaining)
				assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
			}
		}

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateCompleted,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateCompleted,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"flow_to_service_task",
			"service_task",
			"flow_to_end",
			"end_event",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"service_task",
			"service_task",
			"service_task",
		})
		for _, iteration := range getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "service_task") {
			require.NotNil(t, iteration.CompletedAt, "every multi-instance iteration must be completed")
		}
		parentHistory := getFlowElementInstancesByElementId(t, processInstance.Key, "service_task")
		require.Len(t, parentHistory, 1, "the parent records the multi-instance activity once")
		require.NotNil(t, parentHistory[0].CompletedAt, "the multi-instance activity must be completed in the parent")

		completedJobs, err := getProcessInstanceJobs(t, multiInstanceProcess.Key)
		require.NoError(t, err)
		require.Len(t, completedJobs, len(items))
		for _, job := range completedJobs {
			require.Equal(t, public.JobStateCompleted, job.State)
		}
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceIncidentsLength(t, multiInstanceProcess.Key, 0)
	})
}
