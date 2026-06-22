package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	linkEventsPath      = "testdata/link_event/link-events.bpmn"
	linkEventBrokenPath = "testdata/link_event/link-event-broken.bpmn"
)

func TestLinkIntermediateEventFlow(t *testing.T) {
	t.Run("Token from the matching throw link is transferred to the catch link and the flow continues", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, linkEventsPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-A")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-B")

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, processInstance.Key, "Task-A", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, processInstance.Key, "Task-B", runtime.TokenStateWaiting)

		assertProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_1",
			"Flow_0ctyw1t",
			"Gateway_0iadd89",
			"Flow_0htwu8n",
			"Flow_05mg0kq",
			"Link-A-Throw",
			"Link-B-Throw",
			"Link-A-Catch",
			"Link-B-Catch",
			"Flow_09vbthh",
			"Flow_0qpum2z",
			"Task-A",
			"Task-B",
		})
	})

	t.Run("Catch link event is not a wait state and exposes no job or subscription", func(t *testing.T) {
		processInstance := deployAndCreateUniqueProcessDefinition(t, linkEventsPath, nil)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-A")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "Task-B")

		assertProcessInstanceTokenCount(t, processInstance.Key, "Link-A-Catch", 0)
		assertProcessInstanceTokenCount(t, processInstance.Key, "Link-B-Catch", 0)

		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "Link-A-Catch")
		assertProcessInstanceHasNoActiveJobByElementId(t, processInstance.Key, "Link-B-Catch")
		assertProcessInstanceMessageSubscriptionCount(t, processInstance.Key, 0)
	})

	t.Run("Broken link deployment fails the instance through the public API", func(t *testing.T) {
		definition := deployAndGetUniqueProcessDefinition(t, linkEventBrokenPath)
		require.NotZero(t, definition.Key)

		processInstance, err := createProcessInstance(t, &definition.Key, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		assert.Equal(t, zenclient.ProcessInstanceStateFailed, processInstance.State)
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)

		assertProcessInstanceTokenState(t, processInstance.Key, "Link-A-Throw", runtime.TokenStateFailed)
		assertProcessInstanceTokenElements(t, processInstance.Key, nil, []string{"Link-B-Catch", "Task"})

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, "Link-A-Throw", incidents[0].ElementId)
		assert.Contains(t, incidents[0].Message, "failed to find link")
	})
}

func assertProcessInstanceMessageSubscriptionCount(t testing.TB, processInstanceKey int64, expectedCount int) {
	t.Helper()

	resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(t.Context(), processInstanceKey, &zenclient.GetProcessInstanceMessageSubscriptionsParams{
		Size: new(int32(100)),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	require.Equal(t, expectedCount, resp.JSON200.TotalCount, "process instance %d should expose %d message subscriptions", processInstanceKey, expectedCount)
}
