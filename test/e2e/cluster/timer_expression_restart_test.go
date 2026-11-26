//go:build cluster_e2e

package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimeCycleExpressionGoesOnAfterARestart starts a boundary timer whose
// cycle is a FEEL expression, lets the variable the expression reads hold
// nothing a cycle could be, and restarts the node. The timers after the
// restart take the cycle from the stored timer before them: the cycle goes on
// as it began, without evaluating the expression again and without an
// incident.
func TestTimeCycleExpressionGoesOnAfterARestart(t *testing.T) {
	skipIfShort(t)

	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	node := tc.Leader()
	require.NotNil(t, node)
	DeployDefinitionOnNode(t, node, "timer_expression/timer-expression-boundary-cycle.bpmn")
	instanceKey := CreateInstanceOnNode(t, node, GetFirstDefinitionKey(t, node), map[string]interface{}{"cycle": "R30/PT2S"})

	firedBeforeRestart := 0
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		firedBeforeRestart, _ = boundaryCycleTimersOf(c, node, instanceKey)
		assert.GreaterOrEqual(c, firedBeforeRestart, 1, "the cycle fires before the restart")
	}, 30*time.Second, 200*time.Millisecond)

	updated, err := node.RestClient.UpdateProcessInstanceVariablesWithResponse(context.Background(), instanceKey,
		zenclient.UpdateProcessInstanceVariablesJSONRequestBody{Variables: map[string]any{"cycle": "not a cycle"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, updated.StatusCode(), "body: %s", string(updated.Body))

	tc.KillNode(t, node.ID)
	tc.RestartNode(t, node.ID)
	WaitForHealthy(t, tc, 150*time.Second)

	// Two more timers fire: the second of them was created after the restart,
	// from what the timer before it carries.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		fired, _ := boundaryCycleTimersOf(c, node, instanceKey)
		assert.GreaterOrEqual(c, fired, firedBeforeRestart+2, "the cycle goes on after the restart")
	}, 90*time.Second, 500*time.Millisecond)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, waiting := boundaryCycleTimersOf(c, node, instanceKey)
		assert.Equal(c, 1, waiting, "and its next timer waits")
	}, 30*time.Second, 500*time.Millisecond)

	incidents, err := node.RestClient.GetIncidentsWithResponse(context.Background(), instanceKey, &zenclient.GetIncidentsParams{})
	require.NoError(t, err)
	require.NotNil(t, incidents.JSON200, "body: %s", string(incidents.Body))
	assert.Empty(t, incidents.JSON200.Items, "the expression is not evaluated again")
}

// boundaryCycleTimersOf counts the timers of the boundary event of the
// instance which have fired and which wait to fire.
func boundaryCycleTimersOf(c *assert.CollectT, node *TestNode, instanceKey int64) (fired int, waiting int) {
	response, err := node.RestClient.GetProcessInstanceTimerSubscriptionsWithResponse(context.Background(), instanceKey,
		&zenclient.GetProcessInstanceTimerSubscriptionsParams{Size: new(int32(100))})
	if !assert.NoError(c, err) || !assert.NotNil(c, response.JSON200, "body: %s", string(response.Body)) {
		return 0, 0
	}
	for _, subscription := range response.JSON200.Items {
		if subscription.ElementId != "timer-boundary-event" {
			continue
		}
		switch subscription.State {
		case zenclient.EventSubscriptionStateCompleted:
			fired++
		case zenclient.EventSubscriptionStateActive:
			waiting++
		}
	}
	return fired, waiting
}
