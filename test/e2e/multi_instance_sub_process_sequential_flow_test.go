package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const multiInstanceSubProcessSequentialBpmn = "testdata/multi_instance/multi_instance_sub_process_sequential.bpmn"

func TestSequentialMultiInstanceSubProcessFlow(t *testing.T) {
	t.Run("Completing each sub process iteration in collection order completes the parent with exact history", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessSequentialBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		require.Equal(t, zenclient.ProcessInstanceProcessTypeMultiInstance, multiInstanceProcess.ProcessType)

		var iterations []zenclient.ProcessInstancesSimple
		for i, item := range items {
			iterations = waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, i+1)
			current := iterations[i]
			for _, completed := range iterations[:i] {
				waitForProcessInstanceState(t, completed.Key, zenclient.ProcessInstanceStateCompleted)
			}
			require.Equal(t, zenclient.ProcessInstanceStateActive, current.State, "the current sub process iteration must be active")

			job := waitForProcessInstanceActiveJobByElementId(t, current.Key, "review_task")
			require.Equal(t, item, job.InputVariables["item"], "sequential iterations must follow the input collection order")
			assertProcessInstanceTokenState(t, processInstance.Key, "sub_process", runtime.TokenStateWaiting)
			iterationHistory := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "sub_process")
			require.Len(t, iterationHistory, i+1, "a sequential iteration must not start before the previous one completed")
			require.Nil(t, iterationHistory[i].CompletedAt, "the current iteration must remain active")

			require.NoError(t, completeJob(t, job.Key, map[string]any{"decision": "approved"}))
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
		assertExactProcessInstanceHistory(t, multiInstanceProcess.Key, []string{
			"sub_process",
			"sub_process",
			"sub_process",
		})
		for _, iteration := range getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "sub_process") {
			require.NotNil(t, iteration.CompletedAt, "every sub process iteration must be completed")
		}
		require.Len(t, iterations, len(items))
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
