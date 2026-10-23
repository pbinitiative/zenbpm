package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeyRoutedEndpointsAnswerAKeyNamingNothingAlike shows an endpoint routed
// by the partition a key names answers a key of a partition the cluster does
// not have as it answers an unknown key of an existing partition: a request
// about the entity itself is not found, a listing of what belongs to it is
// empty. Neither is the transient cluster failure (502) a client would retry.
func TestKeyRoutedEndpointsAnswerAKeyNamingNothingAlike(t *testing.T) {
	keys := map[string]int64{
		"no entity in an existing partition":         int64(1) << int64(zenflake.StepBits),
		"partition 0, reserved for global resources": 42,
		"a partition the cluster does not have":      int64(1000) << int64(zenflake.StepBits),
	}
	for name, key := range keys {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()

			instance, err := app.restClient.GetProcessInstanceWithResponse(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, instance.StatusCode(), "get instance body: %s", string(instance.Body))

			cancel, err := app.restClient.CancelProcessInstanceWithResponse(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, cancel.StatusCode(), "cancel body: %s", string(cancel.Body))

			modify, err := app.restClient.ModifyProcessInstanceWithResponse(ctx, zenclient.ModifyProcessInstanceJSONRequestBody{ProcessInstanceKey: key})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, modify.StatusCode(), "modify body: %s", string(modify.Body))

			update, err := app.restClient.UpdateProcessInstanceVariablesWithResponse(ctx, key, zenclient.UpdateProcessInstanceVariablesJSONRequestBody{Variables: map[string]any{"a": 1}})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, update.StatusCode(), "update variables body: %s", string(update.Body))

			deleteVariable, err := app.restClient.DeleteProcessInstanceVariableWithResponse(ctx, key, "a")
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, deleteVariable.StatusCode(), "delete variable body: %s", string(deleteVariable.Body))

			statistics, err := app.restClient.GetProcessInstanceElementStatisticsWithResponse(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, statistics.StatusCode(), "element statistics body: %s", string(statistics.Body))

			resolve, err := app.restClient.ResolveIncidentWithResponse(ctx, key, zenclient.ResolveIncidentJSONRequestBody{})
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, resolve.StatusCode(), "resolve incident body: %s", string(resolve.Body))

			decision, err := app.restClient.GetDecisionInstanceWithResponse(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, decision.StatusCode(), "get decision instance body: %s", string(decision.Body))

			page, size := int32(2), int32(7)
			jobs, err := app.restClient.GetProcessInstanceJobsWithResponse(ctx, key, &zenclient.GetProcessInstanceJobsParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, jobs.StatusCode(), "jobs body: %s", string(jobs.Body))
			require.NotNil(t, jobs.JSON200)
			assert.Empty(t, jobs.JSON200.Items)
			assertEmptyPage(t, "jobs", page, size, jobs.JSON200.Page, jobs.JSON200.Size, jobs.JSON200.Count, jobs.JSON200.TotalCount)

			history, err := app.restClient.GetHistoryWithResponse(ctx, key, &zenclient.GetHistoryParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, history.StatusCode(), "history body: %s", string(history.Body))
			require.NotNil(t, history.JSON200)
			if history.JSON200.Items != nil {
				assert.Empty(t, *history.JSON200.Items)
			}
			assertEmptyPage(t, "history", page, size, history.JSON200.Page, history.JSON200.Size, history.JSON200.Count, history.JSON200.TotalCount)

			incidents, err := app.restClient.GetIncidentsWithResponse(ctx, key, &zenclient.GetIncidentsParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, incidents.StatusCode(), "incidents body: %s", string(incidents.Body))
			require.NotNil(t, incidents.JSON200)
			assert.Empty(t, incidents.JSON200.Items)
			assertEmptyPage(t, "incidents", page, size, incidents.JSON200.Page, incidents.JSON200.Size, incidents.JSON200.Count, incidents.JSON200.TotalCount)

			timers, err := app.restClient.GetProcessInstanceTimerSubscriptionsWithResponse(ctx, key, &zenclient.GetProcessInstanceTimerSubscriptionsParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, timers.StatusCode(), "timer subscriptions body: %s", string(timers.Body))
			require.NotNil(t, timers.JSON200)
			assert.Empty(t, timers.JSON200.Items)
			assertEmptyPage(t, "timer subscriptions", page, size, timers.JSON200.Page, timers.JSON200.Size, timers.JSON200.Count, timers.JSON200.TotalCount)

			errorSubscriptions, err := app.restClient.GetProcessInstanceErrorSubscriptionsWithResponse(ctx, key, &zenclient.GetProcessInstanceErrorSubscriptionsParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, errorSubscriptions.StatusCode(), "error subscriptions body: %s", string(errorSubscriptions.Body))
			require.NotNil(t, errorSubscriptions.JSON200)
			assert.Empty(t, errorSubscriptions.JSON200.Items)
			assertEmptyPage(t, "error subscriptions", page, size, errorSubscriptions.JSON200.Page, errorSubscriptions.JSON200.Size, errorSubscriptions.JSON200.Count, errorSubscriptions.JSON200.TotalCount)

			children, err := app.restClient.GetChildProcessInstancesWithResponse(ctx, key, &zenclient.GetChildProcessInstancesParams{Page: &page, Size: &size})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, children.StatusCode(), "child instances body: %s", string(children.Body))
			require.NotNil(t, children.JSON200)
			require.Len(t, children.JSON200.Partitions, 1, "the partition the key names, empty, whether it exists or not")
			assert.Equal(t, int(zenflake.GetPartitionId(key)), children.JSON200.Partitions[0].Partition)
			assert.Empty(t, children.JSON200.Partitions[0].Items)
			assert.Zero(t, children.JSON200.Partitions[0].TotalCount)
			assertEmptyPage(t, "child instances", page, size, children.JSON200.Page, children.JSON200.Size, children.JSON200.Count, children.JSON200.TotalCount)
		})
	}
}

// assertEmptyPage asserts a listing answered the page and size it was asked
// for with nothing on it and nothing in total.
func assertEmptyPage(t *testing.T, listing string, page, size int32, gotPage, gotSize, count, totalCount int) {
	t.Helper()
	assert.Equal(t, int(page), gotPage, "%s page", listing)
	assert.Equal(t, int(size), gotSize, "%s size", listing)
	assert.Zero(t, count, "%s count", listing)
	assert.Zero(t, totalCount, "%s total count", listing)
}
