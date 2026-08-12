package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidStoredSubscriptionStateIsReportedOnce(t *testing.T) {
	t.Run("invalid timer state produces one request-scoped event", func(t *testing.T) {
		definition := deployAndGetUniqueProcessDefinition(t, "testdata/event-subscriptions/event-subscriptions-timer-test.bpmn")
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			response, requestErr := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
				t.Context(),
				instance.Key,
				&zenclient.GetProcessInstanceTimerSubscriptionsParams{},
			)
			assert.NoError(collect, requestErr)
			if assert.NotNil(collect, response) && assert.NotNil(collect, response.JSON200) {
				assert.Equal(collect, 1, response.JSON200.TotalCount)
			}
		}, 5*time.Second, 100*time.Millisecond, "timer subscription should be stored")

		db := partitionDBForProcessInstance(t, instance.Key)
		_, err = db.ExecContext(t.Context(),
			"UPDATE timer SET state = ? WHERE process_instance_key = ?",
			int64(999),
			instance.Key,
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, restoreErr := db.ExecContext(context.Background(),
				"UPDATE timer SET state = ? WHERE process_instance_key = ?",
				int64(runtime.TimerStateCreated),
				instance.Key,
			)
			require.NoError(t, restoreErr)
		})

		events := bindE2ERecordingClient(t)
		response, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(
			t.Context(),
			instance.Key,
			&zenclient.GetProcessInstanceTimerSubscriptionsParams{},
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, response.StatusCode())
		require.Len(t, events, 1)
		event := <-events
		require.NotNil(t, event.Request)
		assert.Equal(t, http.MethodGet, event.Request.Method)
		assert.Contains(t, event.Request.URL, "/event-subscriptions/timers")
	})

	t.Run("invalid error state produces one request-scoped event", func(t *testing.T) {
		definition := deployAndGetUniqueProcessDefinition(t, "testdata/user_task/user_task_with_error_boundary_event.bpmn")
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			response, requestErr := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(
				t.Context(),
				instance.Key,
				&zenclient.GetProcessInstanceErrorSubscriptionsParams{},
			)
			assert.NoError(collect, requestErr)
			if assert.NotNil(collect, response) && assert.NotNil(collect, response.JSON200) {
				assert.Equal(collect, 1, response.JSON200.TotalCount)
			}
		}, 5*time.Second, 100*time.Millisecond, "error subscription should be stored")

		db := partitionDBForProcessInstance(t, instance.Key)
		_, err = db.ExecContext(t.Context(),
			"UPDATE error_subscription SET state = ? WHERE process_instance_key = ?",
			int64(999),
			instance.Key,
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, restoreErr := db.ExecContext(context.Background(),
				"UPDATE error_subscription SET state = ? WHERE process_instance_key = ?",
				int64(runtime.ErrorStateCreated),
				instance.Key,
			)
			require.NoError(t, restoreErr)
		})

		events := bindE2ERecordingClient(t)
		response, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(
			t.Context(),
			instance.Key,
			&zenclient.GetProcessInstanceErrorSubscriptionsParams{},
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, response.StatusCode())
		require.Len(t, events, 1)
		event := <-events
		require.NotNil(t, event.Request)
		assert.Equal(t, http.MethodGet, event.Request.Method)
		assert.Contains(t, event.Request.URL, "/event-subscriptions/errors")
	})
}

func partitionDBForProcessInstance(t *testing.T, processInstanceKey int64) *partition.DB {
	t.Helper()
	partitionStore, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)
	db, ok := partitionStore.(*partition.DB)
	require.Truef(t, ok, "unexpected partition store type %T", partitionStore)
	return db
}

func bindE2ERecordingClient(t *testing.T) chan *sentry.Event {
	t.Helper()
	events := make(chan *sentry.Event, 4)
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		AttachStacktrace: true,
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			events <- event
			return nil
		},
	})
	require.NoError(t, err)

	previousClient := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() {
		sentry.CurrentHub().BindClient(previousClient)
		client.Close()
		if remaining := len(events); remaining != 0 {
			t.Errorf("unexpected additional captured events: %d", remaining)
		}
	})
	return events
}
