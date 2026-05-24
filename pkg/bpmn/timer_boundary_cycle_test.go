package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoninterruptingBoundaryEventTimerCycleFiresExpectedNumberOfTimes verifies a non-interrupting timer boundary event
// configured with timeCycle = R2/PT1S fires exactly 2 times, producing 2 active jobs of the downstream service task
// while the main service task is still pending.
func TestNoninterruptingBoundaryEventTimerCycleFiresExpectedNumberOfTimes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer-boundary-event-noninterrupting-timeCycle.bpmn")
	require.NoError(t, err)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// Boundary timers persist the boundary event's own id as their ElementId so that independent timer boundary events
	// attached to the same activity track their cycles separately. Filter by the boundary event id from the BPMN file.
	const boundaryTimerElementId = "Event_1n9fcqj"

	// Wait on the full downstream effect of R2/PT1S firing twice:
	//   - 2 boundary timers in Triggered state
	//   - no remaining Created boundary timer (R=2 is exhausted)
	//   - 2 active downstream jobs of type "simple-job-2"
	//
	// All three are committed by the engine in the same batch as the timer's Triggered
	// transition, but the timer-manager and job activation run on separate goroutines.
	// Waiting on the timer state alone would race the job activation under CPU load.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggered, err := boundaryTimersInState(t, piKey, runtime.TimerStateTriggered, boundaryTimerElementId)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, len(triggered), "expected R2 boundary cycle to produce exactly 2 triggered timers")

		created, err := boundaryTimersInState(t, piKey, runtime.TimerStateCreated, boundaryTimerElementId)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Empty(collect, created, "no further Created boundary timer should remain")

		activeJobs, err := activeJobsForInstance(t, piKey, "simple-job-2")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, len(activeJobs), "expected 2 active simple-job-2 jobs")
	}, 6*time.Second, 50*time.Millisecond, "R2/PT1S boundary cycle should fire twice and create 2 downstream jobs")
}

// TestMultipleNoninterruptingBoundaryEventTimerCyclesTrackIndependently verifies that when
// multiple non-interrupting timer boundary events are attached to the same activity, each
// boundary event's timeCycle repetition counter is tracked independently — i.e. their
// triggered counts are not aggregated together. This was previously broken because boundary
// timers persisted the attached activity's id as their ElementId, which made cycleTimerStats
// pool the triggered counts of all sibling boundary timers.
func TestMultipleNoninterruptingBoundaryEventTimerCyclesTrackIndependently(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer-boundary-event-multiple-noninterrupting-timeCycle.bpmn")
	require.NoError(t, err)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	require.NoError(t, err)
	piKey := instance.ProcessInstance().Key

	// Boundary_A: R2/PT1S -> exactly 2 fires + 2 active "job-A" jobs.
	// Boundary_B: R3/PT1S -> exactly 3 fires + 3 active "job-B" jobs.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggeredA, err := boundaryTimersInState(t, piKey, runtime.TimerStateTriggered, "Boundary_A")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, len(triggeredA), "Boundary_A R2 should produce exactly 2 triggered timers")
		createdA, err := boundaryTimersInState(t, piKey, runtime.TimerStateCreated, "Boundary_A")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Empty(collect, createdA, "Boundary_A should have no remaining Created timer")

		triggeredB, err := boundaryTimersInState(t, piKey, runtime.TimerStateTriggered, "Boundary_B")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 3, len(triggeredB), "Boundary_B R3 should produce exactly 3 triggered timers")
		createdB, err := boundaryTimersInState(t, piKey, runtime.TimerStateCreated, "Boundary_B")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Empty(collect, createdB, "Boundary_B should have no remaining Created timer")

		jobsA, err := activeJobsForInstance(t, piKey, "job-A")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, len(jobsA), "expected 2 active job-A jobs")

		jobsB, err := activeJobsForInstance(t, piKey, "job-B")
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 3, len(jobsB), "expected 3 active job-B jobs")
	}, 10*time.Second, 50*time.Millisecond, "each boundary cycle should fire its own configured repetitions independently")
}

// boundaryTimersInState returns the timers for the given process instance in the given state
// whose persisted ElementId equals boundaryEventId (which is how boundary timers are persisted).
func boundaryTimersInState(t *testing.T, piKey int64, state runtime.TimerState, boundaryEventId string) ([]runtime.Timer, error) {
	t.Helper()
	all, err := bpmnEngine.persistence.FindProcessInstanceTimers(t.Context(), piKey, state)
	if err != nil {
		return nil, err
	}
	filtered := make([]runtime.Timer, 0, len(all))
	for _, tt := range all {
		if tt.ElementId == boundaryEventId {
			filtered = append(filtered, tt)
		}
	}
	return filtered, nil
}

// activeJobsForInstance returns active jobs of the given type belonging to the given process
// instance. It uses the storage API (which locks the in-memory map) instead of iterating the
// map directly, so it is safe to call while the engine is concurrently writing jobs.
func activeJobsForInstance(t *testing.T, piKey int64, jobType string) ([]runtime.Job, error) {
	t.Helper()
	all, err := engineStorage.FindActiveJobsByType(t.Context(), jobType)
	if err != nil {
		return nil, err
	}
	filtered := make([]runtime.Job, 0, len(all))
	for _, job := range all {
		if job.ProcessInstanceKey == piKey {
			filtered = append(filtered, job)
		}
	}
	return filtered, nil
}
