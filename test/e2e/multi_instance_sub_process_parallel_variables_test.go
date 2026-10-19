package e2e

import (
	"maps"
	"slices"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestParallelMultiInstanceSubProcessVariables(t *testing.T) {
	t.Run("each sub process iteration scope holds only its own input element", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessParallelBpmn, map[string]any{
			"items": []string{"first", "second", "third"},
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		iterations := waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, 3)

		scopes := make([]any, 0, len(iterations))
		for _, iteration := range iterations {
			job := waitForProcessInstanceActiveJobByElementId(t, iteration.Key, "review_task")
			require.Equal(t, map[string]any{"item": job.InputVariables["item"]}, job.InputVariables,
				"the inner task must see only the input element of its own iteration")
			instance, err := getProcessInstance(t, iteration.Key)
			require.NoError(t, err)
			scopes = append(scopes, instance.Variables)
		}
		require.ElementsMatch(t, []any{
			map[string]any{"item": "first"},
			map[string]any{"item": "second"},
			map[string]any{"item": "third"},
		}, scopes, "every concurrent sub process iteration must hold a distinct collection entry")
	})

	t.Run("output collection pairs every iteration with its own sub process output", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceSubProcessParallelBpmn, map[string]any{
			"items":              []string{"first", "second", "third"},
			"unrelated_variable": "carry_through",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		iterations := waitForChildProcessInstancesOfType(t, multiInstanceProcess.Key, zenclient.ProcessInstanceProcessTypeSubprocess, 3)
		// Complete in reverse creation order; every job returns the same variable names.
		for i := len(iterations) - 1; i >= 0; i-- {
			job := waitForProcessInstanceActiveJobByElementId(t, iterations[i].Key, "review_task")
			item := stringVariable(t, job.InputVariables, "item")
			require.NoError(t, completeJob(t, job.Key, map[string]any{"decision": "decided-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		for _, iteration := range iterations {
			instance, err := getProcessInstance(t, iteration.Key)
			require.NoError(t, err)
			item := stringVariable(t, instance.Variables, "item")
			require.Equal(t, map[string]any{"item": item, "reviewDecision": "decided-" + item}, instance.Variables,
				"sub process iteration %q must hold only its own input element and inner output mapping", item)
		}
		for _, iteration := range getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "sub_process") {
			item := stringVariable(t, iteration.InputVariables, "item")
			require.Equal(t, map[string]any{"reviewOutcome": "decided-" + item}, iteration.OutputVariables,
				"iteration %q must record only its own sub process output mapping", item)
		}

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		results, ok := instance.Variables["results"].([]interface{})
		require.True(t, ok, "results must be a list, got %T", instance.Variables["results"])
		require.ElementsMatch(t, []interface{}{
			map[string]interface{}{"item": "first", "outcome": "decided-first"},
			map[string]interface{}{"item": "second", "outcome": "decided-second"},
			map[string]interface{}{"item": "third", "outcome": "decided-third"},
		}, results, "ordering is not guaranteed for parallel multi-instance")
		require.Equal(t, []string{"items", "results", "unrelated_variable"}, slices.Sorted(maps.Keys(instance.Variables)),
			"only the output collection may be added to the parent scope")
	})
}
