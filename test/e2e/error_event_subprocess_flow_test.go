package e2e

import (
	"testing"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorEventSubProcessFlow verifies the token and history flow when an interrupting error event
// subprocess catches a matching job error: the failing main task job is terminated, its token is
// canceled, the error event subprocess runs to completion as a child instance and the parent completes
// without ever reaching the main success path.
func TestErrorEventSubProcessFlow(t *testing.T) {

	t.Run("Matching job error cancels the main task token and completes the parent through the event subprocess", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service-task-error-event-subprocess")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service-task-error-event-subprocess", bpmnruntime.TokenStateWaiting)

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("42"), nil)

		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		child := requireChildEventSubProcessCompleted(t, instance.Key)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, instance.Key, 0)

		assertProcessInstanceTokenState(t, instance.Key, "service-task-error-event-subprocess", bpmnruntime.TokenStateCanceled)
		assertProcessInstanceTokenElements(t, instance.Key, []string{"service-task-error-event-subprocess"}, []string{"should-not-happen-end"})
		assertProcessInstanceHistory(t, instance.Key, []string{
			"StartEvent_1",
			"Flow_start_main",
			"service-task-error-event-subprocess",
		})

		assertProcessInstanceTokenState(t, child.Key, "handled-end", bpmnruntime.TokenStateCompleted)
		assertProcessInstanceHistory(t, child.Key, []string{
			"error-start-event",
			"Flow_error_sub",
			"handled-end",
		})
	})

	t.Run("Non-matching job error holds the main task token in waiting state", func(t *testing.T) {
		instance := setupErrorEventSubProcessInstance(t, nil)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "service-task-error-event-subprocess")

		failJobForElementId(t, instance.Key, "service-task-error-event-subprocess", new("99"), nil)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "service-task-error-event-subprocess", bpmnruntime.TokenStateWaiting)
		assertProcessInstanceTokenElements(t, instance.Key, []string{"service-task-error-event-subprocess"}, []string{"should-not-happen-end"})

		page, err := getChildInstances(t, instance.Key)
		require.NoError(t, err)
		assert.Equal(t, 0, page.Count, "a non-matching error code should not start an event subprocess child instance")
	})
}
