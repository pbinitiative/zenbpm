package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartEventFlow(t *testing.T) {
	t.Run("Manual process creation moves the token to the first flow node and leaves the instance active", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/start_event/start_event.bpmn", nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "first_flow_node")

		fetchedInstance, err := getProcessInstance(t, processInstance.Key)
		require.NoError(t, err)
		require.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)
		require.Len(t, fetchedInstance.ActiveElementInstances, 1)
		require.Equal(t, "first_flow_node", fetchedInstance.ActiveElementInstances[0].ElementId)
		require.Equal(t, runtime.TokenStateWaiting.String(), fetchedInstance.ActiveElementInstances[0].State)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstance.Key))
		require.NoError(t, err)
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			tokens, getTokensErr := store.GetAllTokensForProcessInstance(t.Context(), processInstance.Key)
			if !assert.NoError(collect, getTokensErr) || !assert.Len(collect, tokens, 1) {
				return
			}
			assert.Equal(collect, "first_flow_node", tokens[0].ElementId)
			assert.Equal(collect, runtime.TokenStateWaiting, tokens[0].State)
		}, 5*time.Second, 100*time.Millisecond, "the start event token should transition to the first flow node")

		assertExactProcessInstanceHistoryStates(t, processInstance.Key,
			completedFlowElementHistory("start_event"),
			completedFlowElementHistory("flow_to_first_node"),
			activeFlowElementHistory("first_flow_node"),
		)
	})
}
