package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageStartEventFlow(t *testing.T) {
	t.Run("Published message creates one process instance on the message start branch", func(t *testing.T) {
		definition := deployUniqueMessageStartEventDefinition(t, "flow")
		require.Empty(t, waitForProcessInstancesByBPMNProcessID(t, definition.processID, 0))

		publishMessageStartEvent(t, definition.messageName, map[string]any{
			"messageId": "d2d04b17-ebd6-4a64-a0c4-27d39aa7d724",
		})

		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		instance := instances[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, instance.Key)
		})

		require.Equal(t, definition.key, instance.ProcessDefinitionKey)
		require.Equal(t, zenclient.ProcessInstanceStateActive, instance.State)
		assertProcessInstanceTokenState(t, instance.Key, messageStartEventTaskID, runtime.TokenStateWaiting)
		assertProcessInstanceTokenElements(t, instance.Key,
			[]string{messageStartEventTaskID},
			[]string{messageStartEventElementID, "plainStartEvent_0mtr7my"},
		)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			messageStartEventElementID,
			"Flow_1xx1ap4",
			messageStartEventTaskID,
		})

		taskJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, messageStartEventTaskID)
		require.Equal(t, definition.jobType, taskJob.Type)
		require.NoError(t, completeJob(t, taskJob.Key, nil))

		assertProcessInstanceIsCompleted(t, instance.Key, messageStartEventEndID)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			messageStartEventElementID,
			"Flow_1xx1ap4",
			messageStartEventTaskID,
			"Flow_0rvswpk",
			messageStartEventEndID,
		})
		assertProcessInstanceHasNoActiveJobs(t, instance.Key)
	})

	t.Run("Renewed subscription starts a separate process instance for the next message", func(t *testing.T) {
		definition := deployUniqueMessageStartEventDefinition(t, "renewal")

		publishMessageStartEvent(t, definition.messageName, map[string]any{
			"delivery": "first",
		})
		firstBatch := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 1)
		first := firstBatch[0]
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, first.Key)
		})

		publishMessageStartEvent(t, definition.messageName, map[string]any{
			"delivery": "second",
		})
		instances := waitForProcessInstancesByBPMNProcessID(t, definition.processID, 2)

		var second zenclient.ProcessInstancesSimple
		for _, instance := range instances {
			if instance.Key != first.Key {
				second = instance
				break
			}
		}
		require.NotZero(t, second.Key)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, second.Key)
		})

		require.NotEqual(t, first.Key, second.Key)
		require.Equal(t, definition.key, first.ProcessDefinitionKey)
		require.Equal(t, definition.key, second.ProcessDefinitionKey)
		waitForTwoProcessInstanceStates(
			t,
			first.Key, zenclient.ProcessInstanceStateActive,
			second.Key, zenclient.ProcessInstanceStateActive,
		)

		firstJob := waitForProcessInstanceActiveJobByElementId(t, first.Key, messageStartEventTaskID)
		secondJob := waitForProcessInstanceActiveJobByElementId(t, second.Key, messageStartEventTaskID)
		require.NotEqual(t, firstJob.Key, secondJob.Key)

		require.NoError(t, completeJob(t, firstJob.Key, nil))
		require.NoError(t, completeJob(t, secondJob.Key, nil))
		waitForTwoProcessInstanceStates(
			t,
			first.Key, zenclient.ProcessInstanceStateCompleted,
			second.Key, zenclient.ProcessInstanceStateCompleted,
		)
	})
}
