package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertTimerSubscriptionState(t testing.TB, processInstanceKey int64, elementID string, expectedState zenclient.EventSubscriptionState) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subscription, found, err := findTimerSubscription(t, processInstanceKey, elementID)
		if !assert.NoError(collect, err) || !assert.Truef(collect, found, "timer subscription %s should exist", elementID) {
			return
		}
		assert.Equal(collect, expectedState, subscription.State)
	}, 5*time.Second, 100*time.Millisecond, "timer subscription %s should reach state %s", elementID, expectedState)
}

func assertTimerSubscriptionStateCount(t testing.TB, processInstanceKey int64, elementID string, expectedState zenclient.EventSubscriptionState, expectedCount int) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subscriptions, err := getProcessInstanceTimerSubscriptions(t, processInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}

		count := 0
		for _, subscription := range subscriptions {
			if subscription.ElementId == elementID && subscription.State == expectedState {
				count++
			}
		}

		assert.Equal(collect, expectedCount, count)
	}, 5*time.Second, 100*time.Millisecond, "timer subscription %s should expose %d subscriptions in state %s", elementID, expectedCount, expectedState)
}

func findTimerSubscription(t testing.TB, processInstanceKey int64, elementID string) (zenclient.TimerSubscription, bool, error) {
	t.Helper()

	subscriptions, err := getProcessInstanceTimerSubscriptions(t, processInstanceKey)
	if err != nil {
		return zenclient.TimerSubscription{}, false, err
	}
	for _, subscription := range subscriptions {
		if subscription.ElementId == elementID {
			return subscription, true, nil
		}
	}
	return zenclient.TimerSubscription{}, false, nil
}

func getProcessInstanceTimerSubscriptions(t testing.TB, processInstanceKey int64) ([]zenclient.TimerSubscription, error) {
	t.Helper()

	response, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
		t.Context(),
		processInstanceKey,
		&zenclient.GetProcessInstanceTimerSubscriptionsParams{Size: new(int32(100))},
	)
	if err != nil {
		return nil, err
	}
	if response.JSON200 == nil {
		return nil, fmt.Errorf("timer subscription response is nil: %s", response.Status())
	}
	return response.JSON200.Items, nil
}

func assertProcessInstanceHasNoTimerSubscriptions(t testing.TB, processInstanceKey int64) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		response, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
			t.Context(),
			processInstanceKey,
			&zenclient.GetProcessInstanceTimerSubscriptionsParams{},
		)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, response.JSON200) {
			return
		}
		assert.Empty(collect, response.JSON200.Items)
		assert.Equal(collect, 0, response.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond, "process instance %d should not expose timer subscriptions", processInstanceKey)
}
