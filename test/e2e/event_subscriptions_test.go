package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetProcessInstanceMessageSubscriptions(t *testing.T) {
	t.Parallel()

	definition, err := deployGetUniqueDefinition(t, "simple-intermediate-message-catch-event.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotZero(t, instance.Key)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceMessageSubscriptionsParams{
			Size: ptr.To(int32(100)),
		})
		require.NoError(collect, err)
		require.NotNil(collect, resp.JSON200)
		assert.Equal(collect, 1, resp.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond)

	resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceMessageSubscriptionsParams{})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)

	assert.Equal(t, 1, resp.JSON200.TotalCount)
	require.Len(t, resp.JSON200.Items, 1)
	item := resp.JSON200.Items[0]
	assert.Equal(t, "msg", item.MessageName)
	assert.Equal(t, zenclient.EventSubscriptionStateActive, item.State)
	assert.Equal(t, instance.Key, item.ProcessInstanceKey)
}

func TestMessageSubscriptionsPagination(t *testing.T) {
	t.Parallel()

	var instanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "message-subscriptions",
		Setup: func(t *testing.T) (cleanup func()) {
			definition, err := deployGetUniqueDefinition(t, "event-sub-pagination-test.bpmn")
			require.NoError(t, err)
			uniqKey := fmt.Sprintf("pagination-%d", time.Now().UnixNano())
			instance, err := createProcessInstance(t, &definition.Key, map[string]any{
				"correlationKey": uniqKey,
			})
			require.NoError(t, err)
			require.NotZero(t, instance.Key)
			instanceKey = instance.Key

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
					t.Context(), instanceKey,
					&zenclient.GetProcessInstanceMessageSubscriptionsParams{Size: ptr.To(int32(100))})
				require.NoError(collect, err)
				require.NotNil(collect, resp.JSON200)
				assert.Equal(collect, 3, resp.JSON200.TotalCount)
			}, 5*time.Second, 100*time.Millisecond, "all 3 message subscriptions should appear")

			return func() { cleanupOwnedProcessInstance(t, instanceKey) }
		},
		FetchPage: func(t *testing.T, page, size int) (returnedCount int, totalCount int, receivedPage int, receivedSize int) {
			resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
				t.Context(), instanceKey,
				&zenclient.GetProcessInstanceMessageSubscriptionsParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.NotNil(t, resp.JSON200)
			return len(resp.JSON200.Items), resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 2,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 2},
					{Page: 2, ExpectedCount: 1},
				},
				TotalCount:     3,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func TestMessageSubscriptionsStateFilter(t *testing.T) {
	t.Parallel()

	definition, err := deployGetUniqueDefinition(t, "message-multiple-intermediate-catch-events-parallel.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceMessageSubscriptionsParams{Size: ptr.To(int32(100))})
		require.NoError(collect, err)
		require.NotNil(collect, resp.JSON200)
		assert.Equal(collect, 3, resp.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond, "3 message subscriptions should appear")

	t.Run("active filter returns all subscriptions", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceMessageSubscriptionsParams{
				State: ptr.To(zenclient.EventSubscriptionStateActive),
				Size:  ptr.To(int32(100)),
			})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 3, resp.JSON200.TotalCount)
	})

	t.Run("completed filter returns empty", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceMessageSubscriptionsParams{
				State: ptr.To(zenclient.EventSubscriptionStateCompleted),
			})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 0, resp.JSON200.TotalCount)
	})
}

func TestMessageSubscriptionsInvalidState(t *testing.T) {
	t.Parallel()

	resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
		t.Context(), 1,
		&zenclient.GetProcessInstanceMessageSubscriptionsParams{
			State: ptr.To(zenclient.EventSubscriptionStateCompensated),
		})
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode())
}

func TestGetProcessInstanceTimerSubscriptions(t *testing.T) {
	t.Parallel()

	definition, err := deployGetUniqueDefinition(t, "event-subscriptions-timer-test.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceTimerSubscriptionsParams{Size: ptr.To(int32(100))})
		require.NoError(collect, err)
		require.NotNil(collect, resp.JSON200)
		assert.Equal(collect, 1, resp.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond)

	resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceTimerSubscriptionsParams{})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)

	assert.Equal(t, 1, resp.JSON200.TotalCount)
	require.Len(t, resp.JSON200.Items, 1)
	item := resp.JSON200.Items[0]
	assert.Equal(t, zenclient.EventSubscriptionStateActive, item.State)
	assert.Equal(t, instance.Key, item.ProcessInstanceKey)
	assert.False(t, item.DueDate.IsZero())
}

func TestTimerSubscriptionsPagination(t *testing.T) {
	t.Parallel()

	var instanceKey int64

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "timer-subscriptions",
		Setup: func(t *testing.T) (cleanup func()) {
			definition, err := deployGetUniqueDefinition(t, "event-sub-timer-pagination-test.bpmn")
			require.NoError(t, err)
			instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
			require.NoError(t, err)
			require.NotZero(t, instance.Key)
			instanceKey = instance.Key

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
					t.Context(), instanceKey,
					&zenclient.GetProcessInstanceTimerSubscriptionsParams{Size: ptr.To(int32(100))})
				require.NoError(collect, err)
				require.NotNil(collect, resp.JSON200)
				assert.Equal(collect, 3, resp.JSON200.TotalCount)
			}, 5*time.Second, 100*time.Millisecond, "all 3 timer subscriptions should appear")

			return func() { cleanupOwnedProcessInstance(t, instanceKey) }
		},
		FetchPage: func(t *testing.T, page, size int) (returnedCount int, totalCount int, receivedPage int, receivedSize int) {
			resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
				t.Context(), instanceKey,
				&zenclient.GetProcessInstanceTimerSubscriptionsParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.NotNil(t, resp.JSON200)
			return len(resp.JSON200.Items), resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 2,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 2},
					{Page: 2, ExpectedCount: 1},
				},
				TotalCount:     3,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func TestTimerSubscriptionsInvalidState(t *testing.T) {
	t.Parallel()

	resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
		t.Context(), 1,
		&zenclient.GetProcessInstanceTimerSubscriptionsParams{
			State: ptr.To(zenclient.EventSubscriptionStateCompensated),
		})
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode())
}

func TestGetProcessInstanceErrorSubscriptions(t *testing.T) {
	t.Parallel()

	definition, err := deployGetUniqueDefinition(t, "error_events/user_task/user_task_with_error_boundary_event.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceErrorSubscriptionsParams{Size: ptr.To(int32(100))})
		require.NoError(collect, err)
		require.NotNil(collect, resp.JSON200)
		assert.Equal(collect, 1, resp.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond)

	resp, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(t.Context(), instance.Key, &zenclient.GetProcessInstanceErrorSubscriptionsParams{})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)

	assert.Equal(t, 1, resp.JSON200.TotalCount)
	require.Len(t, resp.JSON200.Items, 1)
	item := resp.JSON200.Items[0]
	assert.Equal(t, zenclient.EventSubscriptionStateActive, item.State)
	assert.Equal(t, instance.Key, item.ProcessInstanceKey)
}

func TestErrorSubscriptionsInvalidState(t *testing.T) {
	t.Parallel()

	resp, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(
		t.Context(), 1,
		&zenclient.GetProcessInstanceErrorSubscriptionsParams{
			State: ptr.To(zenclient.EventSubscriptionStateCompleted),
		})
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode())
}

func TestEventSubscriptionsEmptyResults(t *testing.T) {
	t.Parallel()

	definition, err := deployGetUniqueDefinition(t, "event-subscriptions-timer-test.bpmn")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceTimerSubscriptionsParams{Size: ptr.To(int32(100))})
		require.NoError(collect, err)
		require.NotNil(collect, resp.JSON200)
		assert.Equal(collect, 1, resp.JSON200.TotalCount)
	}, 5*time.Second, 100*time.Millisecond, "timer subscription should appear before cancellation")

	cancelResp, err := app.restClient.CancelProcessInstanceWithResponse(t.Context(), instance.Key)
	require.NoError(t, err)
	require.Equal(t, 204, cancelResp.StatusCode())

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateTerminated)

	active := ptr.To(zenclient.EventSubscriptionStateActive)

	t.Run("active timers returns empty after cancellation", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceTimerSubscriptionsParams{State: active})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 0, resp.JSON200.TotalCount)
		assert.Empty(t, resp.JSON200.Items)
	})

	t.Run("messages returns empty for timer process", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceMessageSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceMessageSubscriptionsParams{})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 0, resp.JSON200.TotalCount)
		assert.Empty(t, resp.JSON200.Items)
	})

	t.Run("errors returns empty for timer process", func(t *testing.T) {
		resp, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(
			t.Context(), instance.Key,
			&zenclient.GetProcessInstanceErrorSubscriptionsParams{})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, 0, resp.JSON200.TotalCount)
		assert.Empty(t, resp.JSON200.Items)
	})
}
