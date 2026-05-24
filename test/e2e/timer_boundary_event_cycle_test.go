package e2e

import (
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimerBoundaryEventNonInterruptingTimeCycle verifies that a non-interrupting
// timeCycle boundary event (`R2/PT1S`) attached to a service task fires exactly twice
// while the service task remains active, producing 2 downstream jobs. After both
// downstream jobs and the main service task job are completed, the process instance
// must end in the Completed state.
//
// BPMN spec recap: a non-interrupting (cancelActivity="false") boundary event does
// NOT terminate the attached activity when it fires. A timeCycle of R2/PT1S therefore
// fires twice (1s apart) while the service task is still pending, each firing taking
// the outgoing flow once and spawning one downstream service task instance.
func TestTimerBoundaryEventNonInterruptingTimeCycle(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile             = "timer-boundary-event-noninterrupting-timeCycle.bpmn"
		bpmnProcessId        = "timer-boundary-event-noninterrupting-timeCycle"
		mainJobType          = "simple-job"
		downstreamJobType    = "simple-job-2"
		boundaryTimerElement = "Event_1n9fcqj" // boundary timers persist the boundary event's own id (attached to "service-task-id")
	)

	definition, err := deployGetDefinition(t, bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	instance, err := createProcessInstance(t, &definition.Key, nil)
	require.NoError(t, err)
	require.NotZero(t, instance.Key, "Process instance key should not be zero")

	// The main service task job must be Active and remain so for the entire test
	// (nothing completes it until the very end).
	assertActiveJobsOfType(t, instance.Key, mainJobType, 1)

	// Wait for the R2 cycle to produce 2 active downstream jobs.
	var downstreamJobs []zenclient.Job
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			JobType:            ptr.To(downstreamJobType),
			ProcessInstanceKey: ptr.To(instance.Key),
			State:              ptr.To(zenclient.JobStateActive),
		})
		if !assert.NoError(collect, err) {
			return
		}
		collected := collectJobs(jobs)
		if !assert.Equal(collect, 2, len(collected),
			"expected exactly 2 active %q jobs for R2/PT1S boundary cycle, got %d",
			downstreamJobType, len(collected)) {
			return
		}
		downstreamJobs = collected
	}, 10*time.Second, 100*time.Millisecond,
		"R2/PT1S non-interrupting boundary timeCycle should produce 2 downstream %q jobs", downstreamJobType)

	// The process instance must still be Active (non-interrupting cycle exhausted but the main service task is still pending).
	fetched, err := getProcessInstance(t, instance.Key)
	require.NoError(t, err)
	require.Equal(t, zenclient.ProcessInstanceStateActive, fetched.State,
		"process instance should remain Active while main service task is pending")

	// All boundary timers must be in Triggered state, and no other timer should remain
	// for this process instance (e.g. no leftover Created timer after R2 exhaustion).
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggered, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateTriggered)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, len(triggered),
			"expected exactly 2 Triggered timers for the R2 boundary cycle, got %+v", triggered)
		for _, tt := range triggered {
			assert.Equal(collect, boundaryTimerElement, tt.ElementId,
				"all triggered timers should belong to the boundary's attached element %q", boundaryTimerElement)
		}
	}, 10*time.Second, 100*time.Millisecond, "exactly 2 Triggered timers expected")

	for _, state := range []bpmnruntime.TimerState{bpmnruntime.TimerStateCreated, bpmnruntime.TimerStateCancelled} {
		timers, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, state)
		require.NoError(t, err)
		assert.Empty(t, timers, "no timers in state %s should remain for this process instance, got %+v", state, timers)
	}

	// Complete the two downstream jobs first.
	for _, job := range downstreamJobs {
		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
	}

	// Now complete the main service task job — this should drive the process to Completed.
	mainJobs, err := getJobs(t, zenclient.GetJobsParams{
		JobType:            ptr.To(mainJobType),
		ProcessInstanceKey: ptr.To(instance.Key),
		State:              ptr.To(zenclient.JobStateActive),
	})
	require.NoError(t, err)
	mainCollected := collectJobs(mainJobs)
	require.Equal(t, 1, len(mainCollected), "exactly one active %q job expected before completion", mainJobType)
	require.NoError(t, completeJob(t, mainCollected[0].Key, map[string]any{}))

	// The process instance must end in Completed state.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pi, err := getProcessInstance(t, instance.Key)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, pi.State,
			"process instance should be Completed after all jobs finish")
	}, 15*time.Second, 100*time.Millisecond, "process instance should reach Completed state")
}

// assertActiveJobsOfType asserts that there are exactly `expected` Active jobs of the
// given type for the given process instance.
func assertActiveJobsOfType(t *testing.T, instanceKey int64, jobType string, expected int) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			JobType:            ptr.To(jobType),
			ProcessInstanceKey: ptr.To(instanceKey),
			State:              ptr.To(zenclient.JobStateActive),
		})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, expected, len(collectJobs(jobs)),
			"expected %d active %q jobs for instance %d", expected, jobType, instanceKey)
	}, 10*time.Second, 100*time.Millisecond,
		"expected %d active %q jobs for instance %d", expected, jobType, instanceKey)
}

// collectJobs flattens all items across the partitions of a JobPartitionPage.
func collectJobs(page zenclient.JobPartitionPage) []zenclient.Job {
	out := make([]zenclient.Job, 0, page.TotalCount)
	for i := range page.Partitions {
		out = append(out, page.Partitions[i].Items...)
	}
	return out
}
