package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

// TestErrorEventSubProcessIncident verifies incident behavior around an interrupting error event
// subprocess: a matching job error is caught and raises no incident, while a non-matching job error is
// uncaught, raises an incident on the failing element and can be resolved.
func TestErrorEventSubProcessIncident(t *testing.T) {

	t.Run("Matching error code is caught and creates no incident", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		assertProcessInstanceIncidentsLength(t, instance.Key, 0)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), nil)

		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)
		_ = requireChildEventSubProcessCompleted(t, instance.Key)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		assertProcessInstanceIncidentsLength(t, instance.Key, 0)
	})

	t.Run("Non-matching error code creates an incident on the failing element", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		assertProcessInstanceIncidentsLength(t, instance.Key, 0)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("99"), nil)

		waitForProcessInstanceJobByElementId(t, instance.Key, "service-task-error-event-subprocess", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		require.Equal(t, "service-task-error-event-subprocess", incidents[0].ElementId)
	})

	t.Run("Non-matching error code incident can be resolved", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("99"), nil)
		waitForProcessInstanceJobByElementId(t, instance.Key, "service-task-error-event-subprocess", public.JobStateFailed)

		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)

		resolveIncident(t, incidents[0].Key)

		incidentsAfterResolve, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.Len(t, incidentsAfterResolve, 1)
		require.NotNil(t, incidentsAfterResolve[0].ResolvedAt)
	})
}
