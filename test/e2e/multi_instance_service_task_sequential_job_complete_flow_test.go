package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const multiInstanceServiceTaskSequentialWithOutputMappingBpmn = "testdata/multi_instance/multi_instance_service_task_sequential_with_output_mapping.bpmn"

func TestSequentialMultiInstanceServiceTaskJobCompleteFlow(t *testing.T) {
	t.Run("Completing each iteration in collection order completes the parent and child with exact history", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskSequentialWithOutputMappingBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeMultiInstance, multiInstanceProcess.ProcessType)

		completedJobKeys := make([]int64, 0, len(items))
		for i, item := range items {
			job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
			require.NotContains(t, completedJobKeys, job.Key, "every iteration must create a new job")
			require.Equal(t, item, job.InputVariables["item"], "sequential iterations must follow the input collection order")
			waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 1)

			assertProcessInstanceTokenState(t, processInstance.Key, "service_task", runtime.TokenStateWaiting)
			assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateWaiting)
			iterationHistory := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "service_task")
			require.Len(t, iterationHistory, i+1, "a sequential iteration must not start before the previous one completed")
			for _, completed := range iterationHistory[:i] {
				require.NotNil(t, completed.CompletedAt, "earlier iterations must be completed")
			}
			require.Nil(t, iterationHistory[i].CompletedAt, "the current iteration must remain active")

			require.NoError(t, completeJob(t, job.Key, map[string]any{"outcome": i}))
			completedJobKeys = append(completedJobKeys, job.Key)
		}

		waitForTwoProcessInstanceStates(
			t,
			processInstance.Key,
			zenclient.ProcessInstanceStateCompleted,
			multiInstanceProcess.Key,
			zenclient.ProcessInstanceStateCompleted,
		)
		assertProcessInstanceTokenState(t, processInstance.Key, "end_event", runtime.TokenStateCompleted)
		assertProcessInstanceTokenState(t, multiInstanceProcess.Key, "service_task", runtime.TokenStateCompleted)
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
