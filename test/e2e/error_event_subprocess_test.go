package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorEventSubProcess verifies the end-to-end behavior of an interrupting error event subprocess:
// when the main service task job is failed with an error code matching the error event subprocess start
// event, the failing job is terminated, the event subprocess runs to completion as a child instance and
// the parent process instance completes with the event subprocess output variables propagated, without
// raising an incident.
func TestErrorEventSubProcess(t *testing.T) {
	cleanProcessInstances(t)

	t.Run("test interrupting error event sub process catches a matching job error", func(t *testing.T) {
		definition, err := deployGetDefinition(t, "error_event_subprocess/error-event-subprocess-interrupting.bpmn", "error-event-subprocess-interrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), nil)

		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		_ = requireChildEventSubProcessCompleted(t, instance.Key)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetched, err := getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "error-caught", fetched.Variables["subProcessResult"],
				"subProcessResult should be set by the error event subprocess output mapping")
			assert.Equal(collect, "errorStartEventValue", fetched.Variables["errorStartEventVar"],
				"errorStartEventVar should be set by the error start event output mapping")
		}, 15*time.Second, 100*time.Millisecond, "the error event subprocess output variables should be propagated to the parent")
	})

	t.Run("test non-matching job error is not caught by the error event sub process", func(t *testing.T) {
		definition, err := deployGetDefinition(t, "error_event_subprocess/error-event-subprocess-interrupting.bpmn", "error-event-subprocess-interrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("99"), nil)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			incidents, err := getProcessInstanceIncidents(t, instance.Key)
			assert.NoError(collect, err)
			assert.Len(collect, incidents, 1)
		}, 15*time.Second, 100*time.Millisecond, "a non-matching error code should raise an incident")

		fetched, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetched.State)

		jobs, err := getJobs(t, zenclient.GetJobsParams{ProcessInstanceKey: new(instance.Key)})
		assert.NoError(t, err)
		require.Equal(t, 1, len(jobs.Partitions))
		require.Equal(t, 1, len(jobs.Partitions[0].Items))
		assert.Equal(t, zenclient.JobStateFailed, jobs.Partitions[0].Items[0].State)
	})
}
