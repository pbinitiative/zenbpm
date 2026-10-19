package e2e

import (
	"maps"
	"slices"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestSequentialMultiInstanceSubProcessVariables(t *testing.T) {
	t.Run("a later sub process iteration does not see variables of an earlier one", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessSequentialBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for i, item := range items {
			current := waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, i+1)[i]
			job := waitForProcessInstanceActiveJobByElementId(t, current.Key, "review_task")
			require.Equal(t, map[string]any{"item": item}, job.InputVariables,
				"iteration %q must expose only its own input element, not the output of earlier iterations", item)
			instance, err := getProcessInstance(t, current.Key)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"item": item}, instance.Variables,
				"sub process iteration %q must start with only its own input element", item)

			require.NoError(t, completeJob(t, job.Key, map[string]any{"decision": "decided-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("output collection holds every sub process output in collection order", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessSequentialBpmn, map[string]any{
			"items":              items,
			"unrelated_variable": "carry_through",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for i, item := range items {
			current := waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, i+1)[i]
			job := waitForProcessInstanceActiveJobByElementId(t, current.Key, "review_task")
			require.NoError(t, completeJob(t, job.Key, map[string]any{"decision": "decided-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		iterations := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "sub_process")
		require.Len(t, iterations, len(items))
		for i, iteration := range iterations {
			require.Equal(t, map[string]any{"reviewOutcome": "decided-" + items[i]}, iteration.OutputVariables,
				"iteration %d must record only its own sub process output mapping", i)
		}
		expectedResults := []interface{}{
			map[string]interface{}{"item": "first", "outcome": "decided-first"},
			map[string]interface{}{"item": "second", "outcome": "decided-second"},
			map[string]interface{}{"item": "third", "outcome": "decided-third"},
		}
		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, expectedResults, instance.Variables["results"])
		require.Equal(t, []string{"items", "results", "unrelated_variable"}, slices.Sorted(maps.Keys(instance.Variables)),
			"only the output collection may be added to the parent scope")

		history := getFlowElementInstancesByElementId(t, processInstance.Key, "sub_process")
		require.Len(t, history, 1)
		require.Equal(t, map[string]any{"results": expectedResults}, history[0].OutputVariables)
	})
}
