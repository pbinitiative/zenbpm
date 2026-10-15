package e2e

import (
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallelMultiInstanceServiceTaskJobCompleteVariables(t *testing.T) {
	t.Run("each iteration job receives only its own input element and input mapping", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskParallelWithOutputMappingBpmn, map[string]any{
			"items":              []string{"first", "second", "third"},
			"unrelated_variable": "parent-only",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 3)

		jobInputs := make([]any, 0, len(jobs))
		for _, job := range jobs {
			jobInputs = append(jobInputs, job.InputVariables)
		}
		require.ElementsMatch(t, []any{
			map[string]any{"item": "first", "itemId": "first"},
			map[string]any{"item": "second", "itemId": "second"},
			map[string]any{"item": "third", "itemId": "third"},
		}, jobInputs, "a concurrent iteration job must expose its own input element and input mapping and nothing of the sibling iterations")
	})

	t.Run("output collection holds each iteration's own output element regardless of completion order", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskParallelWithOutputMappingBpmn, map[string]any{
			"items": []string{"first", "second", "third"},
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 3)
		// Every job returns the same variable names, so a value leaking between the
		// concurrent iterations would show up in another iteration's output.
		for i := len(jobs) - 1; i >= 0; i-- {
			item := stringVariable(t, jobs[i].InputVariables, "item")
			require.NoError(t, completeJob(t, jobs[i].Key, map[string]any{"outcome": "done-" + item, "workerNote": "raw-" + item}))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		iterations := getFlowElementInstancesByElementId(t, multiInstanceProcess.Key, "service_task")
		require.Len(t, iterations, 3)
		for _, iteration := range iterations {
			item := stringVariable(t, iteration.InputVariables, "item")
			require.Equal(t, map[string]any{"iterationOutcome": "done-" + item}, iteration.OutputVariables,
				"iteration %q must record only its own mapped output", item)
		}

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		results, ok := instance.Variables["results"].([]interface{})
		require.True(t, ok, "results must be a list, got %T", instance.Variables["results"])
		require.ElementsMatch(t, []interface{}{
			map[string]interface{}{"item": "first", "outcome": "done-first"},
			map[string]interface{}{"item": "second", "outcome": "done-second"},
			map[string]interface{}{"item": "third", "outcome": "done-third"},
		}, results, "each output element must pair an iteration's input with that iteration's own output")

		history := getFlowElementInstancesByElementId(t, processInstance.Key, "service_task")
		require.Len(t, history, 1)
		require.Equal(t, map[string]any{"results": instance.Variables["results"]}, history[0].OutputVariables,
			"the multi-instance activity history must record the output collection as its only output")
	})

	t.Run("output collection holds every iteration's output element when the jobs are completed concurrently", func(t *testing.T) {
		items := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskParallelWithOutputMappingBpmn, map[string]any{
			"items": items,
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", len(items))
		// the completions are released together so that they overlap, and an
		// iteration result lost between concurrent completions is missing below
		start := make(chan struct{})
		errs := make([]error, len(jobs))
		var completions sync.WaitGroup
		for i, job := range jobs {
			item := stringVariable(t, job.InputVariables, "item")
			completions.Go(func() {
				<-start
				errs[i] = completeJob(t, job.Key, map[string]any{"outcome": "done-" + item})
			})
		}
		close(start)
		completions.Wait()
		for i, err := range errs {
			require.NoError(t, err, "completing job %d", jobs[i].Key)
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		expectedResults := make([]interface{}, 0, len(items))
		for _, item := range items {
			expectedResults = append(expectedResults, map[string]interface{}{"item": item, "outcome": "done-" + item})
		}
		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		results, ok := instance.Variables["results"].([]interface{})
		require.True(t, ok, "results must be a list, got %T", instance.Variables["results"])
		require.ElementsMatch(t, expectedResults, results, "every concurrently completed iteration must contribute its own output element exactly once")
	})

	t.Run("iteration variables do not reach the parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceServiceTaskParallelWithOutputMappingBpmn, map[string]any{
			"items":              []string{"first", "second"},
			"unrelated_variable": "carry_through",
		})
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		multiInstanceProcess := waitForChildProcessInstance(t, processInstance.Key, 0)
		for _, job := range waitForProcessInstanceActiveJobsByElementId(t, multiInstanceProcess.Key, "service_task", 2) {
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
