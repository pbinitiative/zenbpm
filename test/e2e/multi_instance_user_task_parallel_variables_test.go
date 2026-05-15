package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const (
	multiInstanceUserTaskParallelMinimalBpmn = "testdata/multi_instance/multi_instance_user_task_parallel_minimal.bpmn"
)

func TestParallelMultiInstanceUserTaskVariables(t *testing.T) {

	t.Run("nil input collection initializes empty output collection in parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"results": []interface{}{},
		})
	})

	t.Run("empty input collection initializes empty output collection in parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"approvers": []interface{}{},
			"results":   []interface{}{},
		})
	})

	t.Run("input element propagates to each concurrent iteration job", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob", "carol"},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 3)

		approvers := make([]string, 0, len(jobs))
		for _, job := range jobs {
			approver, ok := job.Variables["approver"].(string)
			require.True(t, ok, "iteration job must expose input element as string, got %T", job.Variables["approver"])
			approvers = append(approvers, approver)
		}
		require.ElementsMatch(t, []string{"alice", "bob", "carol"}, approvers, "each parallel iteration must bind input element to a distinct collection entry")
	})

	t.Run("flow element capture input element per iteration", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob"},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 2)
		for _, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		flowElements := getFlowElementInstancesByElementId(t, firstChild.Key, "user_task")
		approversInFlowElements := make([]string, 0, 2)
		for _, flowElement := range flowElements {
			approver, ok := flowElement.InputVariables["approver"].(string)
			if !ok {
				continue
			}
			approversInFlowElements = append(approversInFlowElements, approver)
		}
		require.ElementsMatch(t, []string{"alice", "bob"}, approversInFlowElements, "flow element snapshots must record input element of each iteration")
	})

	t.Run("output element value accumulates into output collection in parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob", "carol"},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 3)
		for _, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		results, ok := instance.Variables["results"].([]interface{})
		require.True(t, ok, "results must be a list, got %T", instance.Variables["results"])
		require.ElementsMatch(t, []interface{}{"alice", "bob", "carol"}, results, "output collection must contain one entry per iteration; ordering is not guaranteed for parallel multi-instance")
	})

	t.Run("input element does not leak to parent scope after completion", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob"},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 2)
		for _, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.NotContains(t, instance.Variables, "approver", "input element must remain local to iteration scope")
	})

	t.Run("create instance variables persist alongside output collection after completion", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers":          []string{"alice", "bob"},
			"unrelated_variable": "carry_through",
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 2)
		for _, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, "carry_through", instance.Variables["unrelated_variable"], "unrelated parent-scope variables must survive multi-instance loop")
		require.Contains(t, instance.Variables, "results")
		require.Contains(t, instance.Variables, "approvers")
	})

	t.Run("non-string input collection elements bind to iteration jobs", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/multi_instance/multi_instance_user_task_parallel_minimal.bpmn", map[string]any{
			"approvers": []int{10, 20, 30},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 3)

		expectedValues := make([]any, 0, len(jobs))
		for _, job := range jobs {
			expectedValues = append(expectedValues, job.Variables["approver"])
			require.NoError(t, completeJob(t, job.Key, nil))
		}
		require.ElementsMatch(t, []any{float64(10), float64(20), float64(30)}, expectedValues, "each parallel iteration must bind a distinct numeric collection entry")

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("parent scope variable with same name as input element is shadowed", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskParallelMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob"},
			"approver":  "parent_value",
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)
		jobs := waitForProcessInstanceActiveJobsByElementId(t, firstChild.Key, "user_task", 2)

		approvers := make([]string, 0, len(jobs))
		for _, job := range jobs {
			approver, ok := job.Variables["approver"].(string)
			require.True(t, ok, "iteration job must shadow parent variable with iteration entry, got %T", job.Variables["approver"])
			approvers = append(approvers, approver)
			require.NoError(t, completeJob(t, job.Key, nil))
		}
		require.ElementsMatch(t, []string{"alice", "bob"}, approvers, "iteration scope must shadow same-named parent variable")

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, "parent_value", instance.Variables["approver"], "parent-scope variable must remain unchanged after the loop")
	})
}
