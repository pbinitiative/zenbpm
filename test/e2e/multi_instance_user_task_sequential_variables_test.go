package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

const (
	multiInstanceUserTaskSequentialMinimalBpmn = "testdata/multi_instance/multi_instance_user_task_sequential_minimal.bpmn"
)

func TestSequentialMultiInstanceUserTaskVariables(t *testing.T) {

	t.Run("nil input collection initializes empty output collection in parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, nil)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"results": []interface{}{},
		})
	})

	t.Run("empty input collection initializes empty output collection in parent scope", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
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

	t.Run("input element propagates to iteration job per iteration", func(t *testing.T) {

		approvers := []string{"alice", "bob"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		firstJob := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
		require.Equal(t, "alice", firstJob.Variables["approver"], "first iteration job should bind input element to first collection entry")
		err := completeJob(t, firstJob.Key, nil)
		require.NoError(t, err)

		secondJob := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
		require.Equal(t, "bob", secondJob.Variables["approver"], "second iteration job should bind input element to second collection entry")
		err = completeJob(t, secondJob.Key, nil)
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")
	})

	t.Run("flow element capture input element per iteration", func(t *testing.T) {
		approvers := []string{"alice", "bob"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		for i := 0; i < len(approvers); i++ {
			job := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		assertFlowElementInputVariablesAt(t, firstChild.Key, "user_task", 0, map[string]any{"approver": "alice"})
		assertFlowElementInputVariablesAt(t, firstChild.Key, "user_task", 1, map[string]any{"approver": "bob"})
	})

	t.Run("output element value accumulates into output collection in parent scope", func(t *testing.T) {

		approvers := []string{"alice", "bob", "carol"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		for i := 0; i < len(approvers); i++ {
			job := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)

		require.Equal(t, []interface{}{"alice", "bob", "carol"}, instance.Variables["results"], "output element must produce one entry per iteration in source order")
	})

	t.Run("input element does not leak to parent scope after completion", func(t *testing.T) {

		approvers := []string{"alice", "bob"}
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": approvers,
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		for i := 0; i < len(approvers); i++ {
			job := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.NotContains(t, instance.Variables, "approver", "input element must remain local to iteration scope")
	})

	t.Run("create instance variables persist alongside output collection after completion", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers":          []string{"alice"},
			"unrelated_variable": "carry_through",
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)
		job := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
		require.NoError(t, completeJob(t, job.Key, nil))

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, "carry_through", instance.Variables["unrelated_variable"], "unrelated parent-scope variables must survive multi-instance loop")
		require.Equal(t, []interface{}{"alice"}, instance.Variables["results"])
		require.Contains(t, instance.Variables, "approvers")
	})

	t.Run("non-string input collection elements bind to iteration job", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": []int{1, 2, 3},
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)
		seen := make([]any, 0, 3)
		for i := 0; i < 3; i++ {
			job := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
			seen = append(seen, job.Variables["approver"])
			require.NoError(t, completeJob(t, job.Key, nil))
		}

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		require.Equal(t, []any{float64(1), float64(2), float64(3)}, seen, "iteration jobs must expose numeric input element entries in source order")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, []interface{}{float64(1), float64(2), float64(3)}, instance.Variables["results"])
	})

	t.Run("parent scope variable with same name as input element is shadowed", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, multiInstanceUserTaskSequentialMinimalBpmn, map[string]any{
			"approvers": []string{"alice", "bob"},
			"approver":  "parent_value",
		})

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		firstChild := waitForChildProcessInstance(t, processInstance.Key, 0)

		firstJob := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
		require.Equal(t, "alice", firstJob.Variables["approver"], "iteration scope must shadow same-named parent variable")
		require.NoError(t, completeJob(t, firstJob.Key, nil))

		secondJob := waitForProcessInstanceActiveJobByElementId(t, firstChild.Key, "user_task")
		require.Equal(t, "bob", secondJob.Variables["approver"], "iteration scope must shadow same-named parent variable across iterations")
		require.NoError(t, completeJob(t, secondJob.Key, nil))

		assertProcessInstanceIsCompleted(t, processInstance.Key, "end_event")

		instance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, "parent_value", instance.Variables["approver"], "parent-scope variable must remain unchanged after the loop")
	})
}
