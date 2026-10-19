package e2e

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSequentialMultiInstanceServiceTaskJobCompleteVariables(t *testing.T) {
	t.Run("a later iteration job does not see variables of an earlier iteration", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskSequentialWithOutputMappingBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for _, item := range items {
			job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
			require.Equal(t, map[string]any{"item": item, "itemId": item}, job.InputVariables,
				"iteration %q must expose only its own input element and input mapping, not the output of earlier iterations", item)
			require.NoError(t, completeJob(t, job.Key, map[string]any{"outcome": "done-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("output collection holds each iteration's own output element in collection order", func(t *testing.T) {
		items := []string{"first", "second", "third"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskSequentialWithOutputMappingBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for _, item := range items {
			job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
			require.NoError(t, completeJob(t, job.Key, map[string]any{"outcome": "done-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		iterations := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "service_task")
		require.Len(t, iterations, len(items))
		for i, iteration := range iterations {
			require.Equal(t, items[i], iteration.InputVariables["item"], "iteration %d must record its own input element", i)
			require.Equal(t, map[string]any{"iterationOutcome": "done-" + items[i]}, iteration.OutputVariables,
				"iteration %d must record only its own mapped output", i)
		}
		expectedResults := []interface{}{
			map[string]interface{}{"item": "first", "outcome": "done-first"},
			map[string]interface{}{"item": "second", "outcome": "done-second"},
			map[string]interface{}{"item": "third", "outcome": "done-third"},
		}
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"items":   []interface{}{"first", "second", "third"},
			"results": expectedResults,
		})

		history := getFlowElementInstancesByElementId(t, processInstance.Key, "service_task")
		require.Len(t, history, 1)
		require.Equal(t, map[string]any{"results": expectedResults}, history[0].OutputVariables,
			"the multi-instance activity history must record the output collection as its only output")
	})

	t.Run("iteration variables do not reach the parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskSequentialWithOutputMappingBpmn, map[string]any{
			"items":              []string{"first", "second"},
			"unrelated_variable": "carry_through",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for range 2 {
			job := waitForProcessInstanceActiveJobByElementId(t, multiInstanceProcess.Key, "service_task")
			item := stringVariable(t, job.InputVariables, "item")
			require.NoError(t, completeJob(t, job.Key, map[string]any{"outcome": "done-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, []string{"items", "results", "unrelated_variable"}, slices.Sorted(maps.Keys(instance.Variables)),
			"only the output collection may be added to the parent scope: input element, input and output mapping targets and raw job variables stay in the iterations")
		require.Equal(t, "carry_through", instance.Variables["unrelated_variable"])
	})
}
