package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertMessageSubscriptionState(t testing.TB, processInstanceKey int64, elementId string, expectedState zenclient.EventSubscriptionState) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subscription, found, err := findMessageSubscription(t, processInstanceKey, elementId)
		if !assert.NoError(collect, err) || !assert.Truef(collect, found, "message subscription %s should exist", elementId) {
			return
		}
		assert.Equal(collect, expectedState, subscription.State)
	}, 5*time.Second, 100*time.Millisecond, "message subscription %s should reach state %s", elementId, expectedState)
}

func assertMessageSubscriptionStateCount(t testing.TB, processInstanceKey int64, elementId string, expectedState zenclient.EventSubscriptionState, expectedCount int) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subscriptions, err := getProcessInstanceMessageSubscriptions(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		count := 0
		for _, subscription := range subscriptions {
			if subscription.ElementId == elementId && subscription.State == expectedState {
				count++
			}
		}

		assert.Equal(collect, expectedCount, count)
	}, 5*time.Second, 100*time.Millisecond, "message subscription %s should expose %d subscriptions in state %s", elementId, expectedCount, expectedState)
}

func findMessageSubscription(t testing.TB, processInstanceKey int64, elementId string) (zenclient.MessageSubscription, bool, error) {
	t.Helper()

	subscriptions, err := getProcessInstanceMessageSubscriptions(t, processInstanceKey)
	if err != nil {
		return zenclient.MessageSubscription{}, false, err
	}
	for _, subscription := range subscriptions {
		if subscription.ElementId == elementId {
			return subscription, true, nil
		}
	}
	return zenclient.MessageSubscription{}, false, nil
}

func getProcessInstanceMessageSubscriptions(t testing.TB, processInstanceKey int64) ([]zenclient.MessageSubscription, error) {
	t.Helper()

	response, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
		t.Context(),
		processInstanceKey,
		&zenclient.GetProcessInstanceMessageSubscriptionsParams{Size: new(int32(100))},
	)
	if err != nil {
		return nil, err
	}
	if response.JSON200 == nil {
		return nil, fmt.Errorf("message subscription response is nil: %s", response.Status())
	}
	return response.JSON200.Items, nil
}
