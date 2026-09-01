package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessInstanceFlowNodeCountLoopCompletesUnderLimit verifies that a legitimate, bounded
// sequence-flow loop that stays under the configured flow node count limit completes
// without incidents.
func TestProcessInstanceFlowNodeCountLoopCompletesUnderLimit(t *testing.T) {
	definition, err := deployGetDefinition(t, "simple-flow-loop.bpmn", "simple-flow-loop")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{"done": false})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)

	// three loop iterations, well below the configured limit
	completeJobForElementId(t, instance.Key, "loopTask", map[string]any{"done": false})
	completeJobForElementId(t, instance.Key, "loopTask", map[string]any{"done": false})
	completeJobForElementId(t, instance.Key, "loopTask", map[string]any{"done": true})

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

	incidents, err := getProcessInstanceIncidents(t, instance.Key)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

// TestProcessInstanceFlowNodeCountExceededCreatesIncident verifies that a sequence-flow loop without a
// reachable exit condition is stopped at the configured maximum flow node count
// (e2eMaxProcessInstanceFlowNodeCount, set in TestMain): the instance fails with an incident, and
// resolving the incident resets the instance's flow node counter so the fixed loop can continue
// and complete.
func TestProcessInstanceFlowNodeCountExceededCreatesIncident(t *testing.T) {
	definition, err := deployGetDefinition(t, "simple-flow-loop.bpmn", "simple-flow-loop")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{"done": false})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	t.Cleanup(func() { cleanProcessInstances(t) })

	// Drive the loop without ever satisfying the exit condition until the guard trips.
	incident := driveLoopUntilFlowNodeCountIncident(t, instance.Key, "loopTask")
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed process instance flow node count of %d", e2eMaxProcessInstanceFlowNodeCount))
	// which loop element trips first is an engine scheduling detail; it must be one of the loop elements
	assert.Contains(t, []string{"joinGateway", "loopTask", "splitGateway"}, incident.ElementId)

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)

	assertProcessInstanceFlowNodeCount(t, instance.Key, e2eMaxProcessInstanceFlowNodeCount)

	// resolving the incident resets the instance's flow node counter, granting a fresh budget;
	// the loop then exits on the next iteration and the instance completes
	resolveIncident(t, incident.Key)
	completeLoopJob(t, instance.Key, "loopTask", map[string]any{"done": true})
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// driveLoopUntilFlowNodeCountIncident keeps completing the active job of the given
// element with an exit condition that never becomes true until an unresolved flow node
// count incident appears on the process instance, and returns that incident.
func driveLoopUntilFlowNodeCountIncident(t testing.TB, processInstanceKey int64, elementID string) public.Incident {
	t.Helper()

	var incident public.Incident
	require.Eventually(t, func() bool {
		incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
		if err == nil {
			for _, candidate := range incidents {
				if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "maximum allowed process instance flow node count") {
					incident = candidate
					return true
				}
			}
		}
		// no incident yet: complete the pending loop job (best effort) to advance the loop
		if job, err := findActiveJobForElement(t, processInstanceKey, elementID); err == nil && job != nil {
			// Completion errors are discarded deliberately: once the guard trips between listing
			// and completing, the engine rejects completions on the failed instance. The next
			// poll re-reads jobs and incidents. The completion that trips the guard itself
			// succeeds: the job is durably completed and the incident is the domain outcome.
			_ = completeJob(t, job.Key, map[string]any{"done": false})
		}
		return false
	}, 60*time.Second, 100*time.Millisecond,
		"process instance %d should raise a flow node count incident", processInstanceKey)
	return incident
}

// findActiveJobForElement returns the active job of the given element, or nil when there is none.
// It lists jobs with an explicit page size: completed jobs of previous loop iterations accumulate
// and must not push the single active job off the default page.
func findActiveJobForElement(t testing.TB, processInstanceKey int64, elementID string) (*public.Job, error) {
	t.Helper()

	resp, err := app.restClient.GetProcessInstanceJobsWithResponse(t.Context(), processInstanceKey, &zenclient.GetProcessInstanceJobsParams{
		Size: new(int32(100)),
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to read process instance jobs: %s", resp.Status())
	}
	jobPage := public.JobPage{}
	if err := json.Unmarshal(resp.Body, &jobPage); err != nil {
		return nil, err
	}
	for _, job := range jobPage.Items {
		if job.ElementId == elementID && job.State == public.JobStateActive {
			return &job, nil
		}
	}
	return nil, nil
}

// completeLoopJob waits for the active job of the given element (listing jobs page-aware, unlike
// completeJobForElementId, because the loop left many completed jobs behind) and completes it.
func completeLoopJob(t testing.TB, processInstanceKey int64, elementID string, vars map[string]any) {
	t.Helper()

	var job *public.Job
	require.Eventually(t, func() bool {
		found, err := findActiveJobForElement(t, processInstanceKey, elementID)
		if err != nil || found == nil {
			return false
		}
		job = found
		return true
	}, 10*time.Second, 100*time.Millisecond,
		"process instance %d should expose active job for element %s", processInstanceKey, elementID)
	require.NoError(t, completeJob(t, job.Key, vars))
}

// assertProcessInstanceFlowNodeCount reads the total flow node counter from the partition store
// of the process instance and asserts its value. Flow node counters are internal runtime-control
// state and are not exposed through the public REST API, so the assertion goes directly
// against the storage layer.
func assertProcessInstanceFlowNodeCount(t testing.TB, processInstanceKey int64, expectedCount int64) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		count, findErr := store.GetFlowNodeCount(t.Context(), processInstanceKey)
		if !assert.NoError(collect, findErr) {
			return
		}
		assert.Equal(collect, expectedCount, count,
			"process instance %d should have flow node count %d", processInstanceKey, expectedCount)
	}, 5*time.Second, 100*time.Millisecond,
		"process instance %d should have flow node count %d", processInstanceKey, expectedCount)
}
