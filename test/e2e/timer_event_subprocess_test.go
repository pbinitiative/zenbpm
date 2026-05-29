package e2e

import (
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimerEventSubProcess(t *testing.T) {
	cleanProcessInstances(t)

	t.Run("test job completion cancels event subprocess timer", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (timer start event with 1s duration) -> (end event)

		// When the main service task job is completed before the timer fires, the event subprocess
		// timer start event should be cancelled.

		// Deploy timer_event_subprocess/timer-event-subprocess-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-interrupting.bpmn", "Process_timerEventSubProcessInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		// Create a process instance
		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service task is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// Before completing the job, the event subprocess timer should be in TimerStateCreated
		assertTimerCreated(t, instance.Key, "subProcessTimerEvent_12i3m6f")

		// Read and complete the active job for the service task
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			JobType:            ptr.To("input-task-timer-event-subprocess-interrupting"),
			ProcessInstanceKey: ptr.To(instance.Key),
		})
		assert.NoError(t, err)
		require.Equal(t, 1, len(jobs.Partitions), "Should have exactly one partition with jobs")
		require.Equal(t, 1, len(jobs.Partitions[0].Items), "Should have exactly one active job")

		err = completeJob(t, jobs.Partitions[0].Items[0].Key, map[string]any{})
		assert.NoError(t, err)

		// After job completion the process instance should be completed
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State,
			"Process instance should be completed after the service task job is completed")

		// The event subprocess timer start event should be in TimerStateCancelled since the process completed
		assertTimerCancelled(t, instance.Key, "subProcessTimerEvent_12i3m6f")
	})

	t.Run("test interrupting timer event sub process", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (timer start event with 1s duration) -> (end event)

		// Deploy timer_event_subprocess/timer-event-subprocess-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-interrupting.bpmn", "Process_timerEventSubProcessInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		// Create a process instance
		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		// before the timer fires, the event subprocess timer should be in TimerStateCreated
		assertTimerCreated(t, instance.Key, "subProcessTimerEvent_12i3m6f")

		// the job of the main service task should be in TERMINATED state as the event subprocess should have interrupted it
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		// Verify the event subprocess child instance was created and completed
		_ = requireChildEventSubProcessCompleted(t, instance.Key)

		// Verify the parent process instance is also completed

		var fetchedInstance *zenclient.ProcessInstance

		assert.Eventually(t, func() bool {
			processInstance, errGetProcessInstance := getProcessInstance(t, instance.Key)

			if errGetProcessInstance != nil {
				return false
			}

			if processInstance.State != zenclient.ProcessInstanceStateCompleted {
				return false
			}

			fetchedInstance = &processInstance
			return true
		}, 15*time.Second, 100*time.Millisecond, "Parent process instance should be completed as the event subprocess is interrupting and was completed")

		// after process completion, root instance timer should be in TimerStateTriggered
		assertTimerTriggered(t, instance.Key, "subProcessTimerEvent_12i3m6f")

		vars := fetchedInstance.Variables

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, "timer-fired", vars["subProcessResult"],
				"subProcessResult should be set by the event subprocess output mapping")
			assert.Equal(collect, true, vars["timerInterrupted"],
				"timerInterrupted should be set by the event subprocess output mapping")
			assert.Equal(collect, "timer-event-subprocess", vars["interruptedBy"],
				"interruptedBy should be set by the event subprocess output mapping")
			assert.Equal(collect, "timerStartEventValue", vars["timerStartEventVar"],
				"timer start event output variable timerStartEventVar must be persisted on the parent process instance")
		}, 15*time.Second, 100*time.Millisecond, "Should have triggered a timer event")
	})

	t.Run("test non-interrupting timer event sub process", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (timer start event with 1s duration) -> (end event)

		// Deploy timer_event_subprocess/timer-event-subprocess-non-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-non-interrupting.bpmn", "Process_timerEventSubProcessNonInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		// Create a process instance
		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// before the timer fires, the event subprocess timer should be in TimerStateCreated
		assertTimerCreated(t, instance.Key, "eventSubprocessTimerEvent_12i3m6f")

		subProcessInstanceWithTimer := waitForChildProcessInstance(t, instance.Key, 0)
		assertProcessInstanceTokenState(t, subProcessInstanceWithTimer.Key, "end_event_non_interrupting", bpmnruntime.TokenStateCompleted)
		waitForProcessInstanceState(t, subProcessInstanceWithTimer.Key, zenclient.ProcessInstanceStateCompleted)

		// the job of the main service task should be still in ACTIVE state as the event subprocess is not-interrupting
		assertSingleJobState(t, instance.Key, zenclient.JobStateActive)

		// Verify the parent process instance is still active as the event subprocess is non-interrupting
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State,
			"Parent process instance should be active as the event subprocess is non-interrupting and parent process is still waiting for a job worker on service-task-1")

		// after the event subprocess completes, root instance timer should be in TimerStateTriggered
		assertTimerTriggered(t, instance.Key, "eventSubprocessTimerEvent_12i3m6f")

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "non-interrupting-done", fetchedInstance.Variables["eventSubProcessResult"],
				"eventSubProcessResult should be propagated from the non-interrupting event subprocess to the parent")
			assert.Equal(collect, true, fetchedInstance.Variables["nonInterruptingExecuted"],
				"nonInterruptingExecuted should be propagated from the non-interrupting event subprocess to the parent")
			assert.Equal(collect, "timerStartEventValue", fetchedInstance.Variables["timerStartEventVar"],
				"timerStartEventVar should be propagated from the non-interrupting timer-start event to the parent")
		}, 15*time.Second, 100*time.Millisecond, "the event subprocess output variables should be propagated to the still-active parent instance")
	})

	t.Run("test non-interrupting timer event sub process with cycle (R3/PT1S)", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (cycle timer start event R3/PT1S) -> (end event)
		//
		// The cycle timer is non-interrupting and configured to fire 3 times with a 1s interval.
		// Each firing spawns a new child event sub-process instance while the parent stays
		// active (its main service task job has no worker and remains ACTIVE).

		definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-non-interrupting-cycle.bpmn", "Process_timerEventSubProcessNonInterruptingCycle")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		// Before the first firing, the cycle timer should be in TimerStateCreated
		assertTimerCreated(t, instance.Key, "eventSubprocessCycleStart")

		// Poll the parent's timerStartEventVar while waiting for 3 completed children. Each
		// cycle firing propagates a new timestamp to the parent; we collect every distinct
		// value we observe so we can later assert there are exactly 3 of them.
		distinctTimestamps := make(map[string]struct{})
		var children []zenclient.ProcessInstancesSimple
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			if v, err := getProcessInstance(t, instance.Key); err == nil {
				if ts, ok := v.Variables["timerStartEventVar"].(string); ok && ts != "" {
					distinctTimestamps[ts] = struct{}{}
				}
			}
			page, err := getChildInstances(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			matched := matchedCompletedChildren(page, instance.Key)
			if !assert.GreaterOrEqual(collect, len(matched), 3,
				"expected at least 3 completed child sub-process instances, got %d", len(matched)) {
				return
			}
			children = matched
		}, 20*time.Second, 100*time.Millisecond,
			"expected 3 completed child sub-process instances for parent %d", instance.Key)

		// The main service task job should still be ACTIVE since the event sub process is
		// non-interrupting and there is no worker consuming the job.
		assertSingleJobState(t, instance.Key, zenclient.JobStateActive)

		// Parent must still be active (non-interrupting + no job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State,
			"Parent process instance should remain active as the event subprocess is non-interrupting")

		// Exactly 3 triggered timers for the cycle start event must exist (R3 = 3 repetitions)
		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)
		assertTriggeredTimerCountForElement(t, store, instance.Key, "eventSubprocessCycleStart", 3)
		// No further Created timer for the cycle start event should remain after R3 exhausted
		assertNoCreatedTimerForElement(t, store, instance.Key, "eventSubprocessCycleStart")

		// The timer for the cycle start element must end up in TimerStateTriggered after the last firing.
		assertTimerTriggered(t, instance.Key, "eventSubprocessCycleStart")

		// Each child sub-process must have a distinct Key — confirming 3 distinct firings of the cycle.
		seenKeys := make(map[int64]struct{}, len(children))
		for _, c := range children {
			seenKeys[c.Key] = struct{}{}
		}
		assert.Equal(t, 3, len(seenKeys),
			"the R3 cycle should produce 3 distinct child sub-process instances, got keys: %v", seenKeys)

		// Capture one final value of the parent's timerStartEventVar to make sure we observed
		// the last firing's timestamp too (in case our polling missed an in-between value).
		if ts, ok := fetchedInstance.Variables["timerStartEventVar"].(string); ok && ts != "" {
			distinctTimestamps[ts] = struct{}{}
		}

		// Three firings 1s apart with second-precision timestamps must yield 3 distinct values.
		assert.Equal(t, 3, len(distinctTimestamps),
			"each cycle firing should propagate a distinct timerStartEventVar timestamp to the parent, got: %v", distinctTimestamps)

		lastFiredTimestamp := maxString(distinctTimestamps)

		// Parent variables must reflect propagation from the cycle event sub-process io-mapping.
		// The event sub-process io-mapping pushes eventSubProcessResult and nonInterruptingCycleExecuted
		// up to the parent on each child completion; the timer-start event pushes timerStartEventVar.
		// The final value of timerStartEventVar must be from the last fired (i.e. latest) child.
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "non-interrupting-cycle-done", fetchedInstance.Variables["eventSubProcessResult"],
				"eventSubProcessResult should be propagated from the cycle event subprocess to the parent")
			assert.Equal(collect, true, fetchedInstance.Variables["nonInterruptingCycleExecuted"],
				"nonInterruptingCycleExecuted should be propagated from the cycle event subprocess to the parent")
			ts, ok := fetchedInstance.Variables["timerStartEventVar"].(string)
			assert.True(collect, ok && ts != "",
				"timerStartEventVar should be a non-empty ISO-8601 timestamp propagated from the cycle timer-start event, got %#v",
				fetchedInstance.Variables["timerStartEventVar"])
			assert.Equal(collect, lastFiredTimestamp, ts,
				"timerStartEventVar must reflect the last fired event subprocess (latest timestamp), got %q, want %q (all observed: %v)",
				ts, lastFiredTimestamp, distinctTimestamps)
		}, 15*time.Second, 100*time.Millisecond,
			"event sub-process output variables should be propagated to the still-active parent instance")
	})

	t.Run("test interrupting timer event nested sub processes", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task 1) -> (end event)
		// event sub process L1: (timer start event with 1s duration) -> (service task 2) -> (end event)
		// event sub process L2: (timer start event with 1s duration) -> (service task 3) -> (end event)
		// event sub process L3: (timer start event with 1s duration) -> (end event)

		// Deploy timer_event_subprocess/timer-event-subprocess-nested-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-nested-interrupting.bpmn", "Process_timerEventSubProcessInterruptingNested")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		// Create a process instance
		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// root process timer (that would start L1 sub process) should be created
		assertTimerCreated(t, instance.Key, "eventSubprocessL1TimerEvent_12i3m6f")

		// L1 child instance should now exist and be active; its L2 timer should be in TimerStateCreated
		l1Instance := getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, l1Instance.Key, "eventSubProcessL2TimerEvent_075kpin")

		// L2 child instance should now exist and be active; its L3 timer should be in TimerStateCreated
		l2Instance := getFirstChildInstance(t, l1Instance.Key)
		assertTimerCreated(t, l2Instance.Key, "eventSubProcessL3TimerEvent_0zi70w1")

		// ROOT parent instance: verify jobs are terminated
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		// ROOT parent instance: Verify the parent process instance is also completed
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State,
				"Parent process instance should be completed as the event subprocess is interrupting and was completed")

			// verify that event subprocess output variables were propagated through all levels to root
			assert.Equal(collect, "l1-completed", fetchedInstance.Variables["l1Result"],
				"l1Result should be propagated from L1 event subprocess to root")
			assert.Equal(collect, "l2-completed", fetchedInstance.Variables["l2Result"],
				"l2Result should be propagated through L1 from L2 event subprocess to root")
			assert.Equal(collect, "l3-completed", fetchedInstance.Variables["l3Result"],
				"l3Result should be propagated through L2 and L1 from L3 event subprocess to root")
			// Each level's timer-start event sets timerStartEventVar = "timerStartEventValue" on its
			// own subprocess scope, and each subprocess io-mapping appends "-l3", "-l2", "-l1"
			// respectively as it propagates the variable up. The final concatenated value must end
			// up on the root parent.
			assert.Equal(collect, "timerStartEventValue-l3-l2-l1", fetchedInstance.Variables["timerStartEventVar"],
				"timerStartEventVar must cascade L3 -> L2 -> L1 -> root with each level appending its suffix")
		}, 15*time.Second, 100*time.Millisecond, "Parent process instance should be completed with output variables as the event subprocess is interrupting and was completed")

		// after process completion, root instance timers should be in TimerStateTriggered
		assertTimerTriggered(t, instance.Key, "eventSubprocessL1TimerEvent_12i3m6f")

		// L1: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL1 := requireChildEventSubProcessCompleted(t, instance.Key)
		// L1: Verify the jobs of L1 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL1.Key, zenclient.JobStateTerminated)
		// L1: verify timer state
		assertTimerTriggered(t, eventSubProcessInstanceL1.Key, "eventSubProcessL2TimerEvent_075kpin")

		// L2: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL2 := requireChildEventSubProcessCompleted(t, eventSubProcessInstanceL1.Key)
		// L2: Verify the jobs of L2 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL2.Key, zenclient.JobStateTerminated)
		// L2: verify timer state
		assertTimerTriggered(t, eventSubProcessInstanceL2.Key, "eventSubProcessL3TimerEvent_0zi70w1")

		// L3: Verify the event subprocess child instance was created and completed. L3 event sub process does not have any jobs to verify
		requireChildEventSubProcessCompleted(t, eventSubProcessInstanceL2.Key)
	})

}

// requireChildEventSubProcessCompleted finds the completed child event subprocess instance of the given
// parent instance, requires it to exist, asserts it is in completed state, and returns it.
func requireChildEventSubProcessCompleted(t *testing.T, parentInstanceKey int64) *zenclient.ProcessInstancesSimple {
	t.Helper()
	var instance *zenclient.ProcessInstancesSimple

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		childrenPage, err := getChildInstances(t, parentInstanceKey)
		if !assert.NoError(collect, err) {
			return
		}
		if !assert.GreaterOrEqual(collect, childrenPage.Count, 1, "There should be at least one child process instance (the event subprocess)") {
			return
		}

		var found *zenclient.ProcessInstancesSimple
		for i := range childrenPage.Partitions {
			for j := range childrenPage.Partitions[i].Items {
				item := &childrenPage.Partitions[i].Items[j]
				if item.ParentProcessInstanceKey != nil && *item.ParentProcessInstanceKey == parentInstanceKey {
					found = item
					break
				}
			}
			if found != nil {
				break
			}
		}

		if !assert.NotNil(collect, found, "Event subprocess instance should exist as a child of the parent instance") {
			return
		}
		if !assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, found.State, "Event subprocess should have completed") {
			return
		}

		instance = found
	}, 15*time.Second, 100*time.Millisecond, "child event subprocess should complete")

	return instance
}

// assertSingleJobState verifies that the given process instance has exactly one partition
// with one job, and that job is in expectedJobState state.
func assertSingleJobState(t *testing.T, instanceKey int64, expectedJobState zenclient.JobState) {
	t.Helper()

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			ProcessInstanceKey: ptr.To(instanceKey),
		})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(jobs.Partitions), "Should have one partition with jobs")
		assert.Equal(collect, 1, len(jobs.Partitions[0].Items), "Should have one job for the process instance")
		assert.Equal(collect, expectedJobState, jobs.Partitions[0].Items[0].State, "The job should be in "+expectedJobState+" state as the event subprocess should have interrupted it")
	}, 15*time.Second, 100*time.Millisecond, "The job should be in "+expectedJobState+" state")
}

// assertTimerTriggered verifies that a timer with the given elementId exists in TimerStateTriggered
// for the given process instance.
func assertTimerTriggered(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggeredTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateTriggered)
		if !assert.NoError(collect, err) {
			return
		}

		found := false
		for _, timer := range triggeredTimers {
			if timer.ElementId == elementId {
				found = true
				break
			}
		}

		assert.True(collect, found, "expected timer with elementId %q to be in TimerStateTriggered for process instance %d, got: %+v", elementId, instanceKey, triggeredTimers)
	}, 20*time.Second, 100*time.Millisecond, "timer should be triggered")
}

// assertTimerCreated verifies that a timer with the given elementId exists in TimerStateCreated
// for the given process instance (i.e. the timer has not yet fired). Retries until the timer shows up
func assertTimerCreated(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		createdTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
		if !assert.NoError(collect, err) {
			return
		}
		found := false
		for _, timer := range createdTimers {
			if timer.ElementId == elementId {
				found = true
				break
			}
		}
		assert.True(collect, found, "expected timer with elementId %q to be in TimerStateCreated for process instance %d, got: %+v", elementId, instanceKey, createdTimers)
	}, 20*time.Second, 100*time.Millisecond, "timer should be created")
}

// assertTimerCancelled verifies that a timer with the given elementId exists in TimerStateCancelled
// for the given process instance.
func assertTimerCancelled(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		cancelledTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCancelled)
		if !assert.NoError(collect, err) {
			return
		}
		found := false
		for _, timer := range cancelledTimers {
			if timer.ElementId == elementId {
				found = true
				break
			}
		}
		assert.True(collect, found, "expected timer with elementId %q to be in TimerStateCancelled for process instance %d, got: %+v", elementId, instanceKey, cancelledTimers)
	}, 20*time.Second, 100*time.Millisecond, "timer should be cancelled")
}

// matchedCompletedChildren returns the completed children of parentInstanceKey from the given page.
func matchedCompletedChildren(page zenclient.ProcessInstancePage, parentInstanceKey int64) []zenclient.ProcessInstancesSimple {
	matched := make([]zenclient.ProcessInstancesSimple, 0)
	for i := range page.Partitions {
		for j := range page.Partitions[i].Items {
			item := page.Partitions[i].Items[j]
			if item.ParentProcessInstanceKey == nil || *item.ParentProcessInstanceKey != parentInstanceKey {
				continue
			}
			if item.State != zenclient.ProcessInstanceStateCompleted {
				continue
			}
			matched = append(matched, item)
		}
	}
	return matched
}

// assertTriggeredTimerCountForElement asserts that exactly expected timers in TimerStateTriggered
// exist for the given element id on the given process instance.
func assertTriggeredTimerCountForElement(t *testing.T, store storage.Storage, instanceKey int64, elementId string, expected int) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggered, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateTriggered)
		if !assert.NoError(collect, err) {
			return
		}
		count := 0
		for _, tt := range triggered {
			if tt.ElementId == elementId {
				count++
			}
		}
		assert.Equal(collect, expected, count,
			"expected %d triggered timers for elementId %q on instance %d, got %d", expected, elementId, instanceKey, count)
	}, 15*time.Second, 100*time.Millisecond,
		"expected %d triggered timers for elementId %q on instance %d", expected, elementId, instanceKey)
}

// assertNoCreatedTimerForElement asserts that no timer in TimerStateCreated exists for the given
// element id on the given process instance (used after a cycle has exhausted all repetitions).
func assertNoCreatedTimerForElement(t *testing.T, store storage.Storage, instanceKey int64, elementId string) {
	t.Helper()
	created, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	for _, c := range created {
		assert.NotEqual(t, elementId, c.ElementId,
			"no further Created timer should exist for elementId %q on instance %d", elementId, instanceKey)
	}
}

// maxString returns the lexicographically largest key in a string set.
// ISO-8601 timestamps sort lexicographically in chronological order, so this correctly
// identifies the timestamp from the last cycle firing.
func maxString(set map[string]struct{}) string {
	largest := ""
	for s := range set {
		if s > largest {
			largest = s
		}
	}
	return largest
}
