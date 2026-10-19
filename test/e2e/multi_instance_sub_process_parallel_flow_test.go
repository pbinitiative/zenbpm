package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const multiInstanceSubProcessParallelBpmn = "testdata/multi_instance/multi_instance_sub_process_parallel.bpmn"

func TestParallelMultiInstanceSubProcessFlow(t *testing.T) {
	t.Run("Completing every concurrent sub process iteration completes the parent with exact history", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessParallelBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeMultiInstance, multiInstanceProcess.ProcessType)
		iterations := waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, len(items))

		jobs := make([]int64, 0, len(iterations))
		for _, iteration := range iterations {
			jobs = append(jobs, waitForProcessInstanceActiveJobByElementId(t, iteration.Key, "review_task").Key)
			assertProcessInstanceTokenState(t, iteration.Key, "review_task", runtime.TokenStateWaiting)
			assertExactProcessInstanceHistory(t, iteration.Key, []string{
				"sub_start_event",
				"flow_to_review_task",
				"review_task",
			})
		}
		assertProcessInstanceTokenState(t, processInstance.Key, "sub_process", runtime.TokenStateWaiting)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"start_event",
			"flow_to_sub_process",
			"sub_process",
		})
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"sub_process",
			"sub_process",
			"sub_process",
		})

		for _, jobKey := range jobs {
			require.NoError(t, completeJob(t, jobKey, map[string]any{"decision": "approved"}))
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
			"flow_to_sub_process",
			"sub_process",
			"flow_to_end",
			"end_event",
		})
		for _, iteration := range getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "sub_process") {
			require.NotNil(t, iteration.CompletedAt, "every sub process iteration must be completed")
		}
		for _, iteration := range iterations {
			waitForProcessInstanceState(t, iteration.Key, zenclient.ProcessInstanceStateCompleted)
			assertExactProcessInstanceHistory(t, iteration.Key, []string{
				"sub_start_event",
				"flow_to_review_task",
				"review_task",
				"flow_to_sub_end",
				"sub_end_event",
			})
		}
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})
}
