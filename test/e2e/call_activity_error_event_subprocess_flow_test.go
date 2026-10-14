package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestCallActivityErrorEventSubProcessFlow(t *testing.T) {
	t.Run("Parent error event sub process catches a job error thrown in the called child", func(t *testing.T) {
		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_error_event_subprocess.bpmn", nil)

		assertProcessInstanceTokenState(t, processInstance.Key, "call_activity", runtime.TokenStateWaiting)
		failJobForElementId(t, childInstance.Key, "id", new("42"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceTokenState(t, childInstance.Key, "id", runtime.TokenStateCanceled)
		assertProcessInstanceIncidentsLength(t, childInstance.Key, 0)

		eventSubProcessInstance := waitForChildProcessInstanceByType(t, processInstance.Key, zenclient.ProcessInstanceProcessTypeSubprocess)
		waitForProcessInstanceState(t, eventSubProcessInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIsCompleted(t, eventSubProcessInstance.Key, "event-subprocess-end")

		assertProcessInstanceTokenState(t, processInstance.Key, "call_activity", runtime.TokenStateCanceled)
		assertProcessInstanceTokenCount(t, processInstance.Key, "completed-end", 0)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("Call activity error boundary takes precedence over the parent error event sub process", func(t *testing.T) {
		processInstance, childInstance := createCallActivityInstance(t, "testdata/call_activity/call_activity_with_error_boundary_and_error_event_subprocess.bpmn", nil)

		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 1, 0)
		failJobForElementId(t, childInstance.Key, "id", new("42"), nil)

		waitForTwoProcessInstanceStates(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted, childInstance.Key, zenclient.ProcessInstanceStateTerminated)
		assertProcessInstanceIsCompleted(t, processInstance.Key, "handled-end")
		assertProcessInstanceTokenCount(t, processInstance.Key, "completed-end", 0)
		assertProcessInstanceErrorSubscriptionCount(t, processInstance.Key, 0, 1)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		children, err := getChildInstances(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, children.Partitions, 1)
		require.Len(t, children.Partitions[0].Items, 1, "only the called child may exist; the error event sub process must not start")
		require.Equal(t, childInstance.Key, children.Partitions[0].Items[0].Key)
	})
}
