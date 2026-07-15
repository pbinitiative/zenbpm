package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestMessageCatchEventFlow(t *testing.T) {
	t.Run("Process waits on the intermediate message catch event and continues after message receipt", func(t *testing.T) {
		definition, err := deployGetDefinition(t, "simple-intermediate-message-catch-event.bpmn", "simple-intermediate-message-catch-event")
		require.NoError(t, err)

		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "msg", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, processInstance.Key, "msg", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_0h5js9r",
			"msg",
		})

		err = publishMessage(t, "msg", "key", &map[string]any{
			"foo": "mapped-value",
		})
		require.NoError(t, err)

		assertProcessInstanceIsCompleted(t, processInstance.Key, "EndEvent_1")
		assertProcessInstanceTokenCount(t, processInstance.Key, "msg", 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, "msg", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, processInstance.Key, "msg", zenclient.EventSubscriptionStateCompleted, 1)
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_0h5js9r",
			"msg",
			"Flow_03f7s7e",
			"EndEvent_1",
		})
	})
}
