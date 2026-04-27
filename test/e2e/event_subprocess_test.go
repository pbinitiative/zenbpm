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

func TestEventSubProcess(t *testing.T) {
	cleanProcessInstances(t)

	t.Run("test job completion cancels event subprocess timer", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (timer start event with 1s duration) -> (end event)

		// When the main service task job is completed before the timer fires, the event subprocess
		// timer start event should be cancelled.

		// Deploy timer-event-subprocess-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer-event-subprocess-interrupting.bpmn", "Process_timerEventSubProcessInterrupting")
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

		// Deploy timer-event-subprocess-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer-event-subprocess-interrupting.bpmn", "Process_timerEventSubProcessInterrupting")
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
		assertTimerCreated(t, instance.Key, "subProcessTimerEvent_12i3m6f")

		// Wait for 2s till timer start event of event sub process fires and completes the event sub process
		// which should interrupt the main process and complete it as well
		time.Sleep(2 * time.Second)

		// the job of the main service task should be in TERMINATED state as the event subprocess should have interrupted it
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		// Verify the event subprocess child instance was created and completed
		_ = requireChildEventSubProcessCompleted(t, instance.Key)

		// Verify the parent process instance is also completed
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State,
			"Parent process instance should be completed as the event subprocess is interrupting and was completed")

		// after process completion, root instance timer should be in TimerStateTriggered
		assertTimerTriggered(t, instance.Key, "subProcessTimerEvent_12i3m6f")

		vars := fetchedInstance.Variables
		assert.Equal(t, "timer-fired", vars["subProcessResult"],
			"subProcessResult should be set by the event subprocess output mapping")
		assert.Equal(t, true, vars["timerInterrupted"],
			"timerInterrupted should be set by the event subprocess output mapping")
		assert.Equal(t, "timer-event-subprocess", vars["interruptedBy"],
			"interruptedBy should be set by the event subprocess output mapping")
	})

	t.Run("test non-interrupting timer event sub process", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// timer event sub process definition: (timer start event with 1s duration) -> (end event)

		// Deploy timer-event-subprocess-non-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer-event-subprocess-non-interrupting.bpmn", "Process_timerEventSubProcessNonInterrupting")
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

		// Wait for 2s till timer start event of event sub process fires and completes the event sub process
		// that should NOT interrupt the main process and complete it
		time.Sleep(2 * time.Second)

		// the job of the main service task should be still in ACTIVE state as the event subprocess is not-interrupting
		assertSingleJobState(t, instance.Key, zenclient.JobStateActive)

		// Verify the parent process instance is still active as the event subprocess is non-interrupting
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State,
			"Parent process instance should be active as the event subprocess is non-interrupting and parent process is still waiting for a job worker on service-task-1")

		// after the event subprocess completes, root instance timer should be in TimerStateTriggered
		assertTimerTriggered(t, instance.Key, "eventSubprocessTimerEvent_12i3m6f")

		// verify that the event subprocess output variables were propagated to the still-active parent instance
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, "non-interrupting-done", fetchedInstance.Variables["eventSubProcessResult"],
			"eventSubProcessResult should be propagated from the non-interrupting event subprocess to the parent")
		assert.Equal(t, true, fetchedInstance.Variables["nonInterruptingExecuted"],
			"nonInterruptingExecuted should be propagated from the non-interrupting event subprocess to the parent")
	})

	t.Run("test interrupting timer event nested sub processes", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task 1) -> (end event)
		// event sub process L1: (timer start event with 1s duration) -> (service task 2) -> (end event)
		// event sub process L2: (timer start event with 1s duration) -> (service task 3) -> (end event)
		// event sub process L3: (timer start event with 1s duration) -> (end event)

		// Deploy timer-event-subprocess-nested-interrupting.bpmn
		definition, err := deployGetDefinition(t, "timer-event-subprocess-nested-interrupting.bpmn", "Process_timerEventSubProcessInterruptingNested")
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

		// Wait for L1 event subprocess timer (PT1S) to fire and create the L1 child instance
		time.Sleep(1500 * time.Millisecond)

		// L1 child instance should now exist and be active; its L2 timer should be in TimerStateCreated
		l1Instance := getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, l1Instance.Key, "eventSubProcessL2TimerEvent_075kpin")

		// Wait for L2 event subprocess timer (PT1S) to fire and create the L2 child instance
		time.Sleep(1000 * time.Millisecond)

		// L2 child instance should now exist and be active; its L3 timer should be in TimerStateCreated
		l2Instance := getFirstChildInstance(t, l1Instance.Key)
		assertTimerCreated(t, l2Instance.Key, "eventSubProcessL3TimerEvent_0zi70w1")

		// Wait for L3 event subprocess timer (PT1S) to fire and all instances to complete
		time.Sleep(2 * time.Second)

		// ROOT parent instance: verify jobs are terminated
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)
		// ROOT parent instance: Verify the parent process instance is also completed
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State,
			"Parent process instance should be completed as the event subprocess is interrupting and was completed")

		// verify that event subprocess output variables were propagated through all levels to root
		assert.Equal(t, "l1-completed", fetchedInstance.Variables["l1Result"],
			"l1Result should be propagated from L1 event subprocess to root")
		assert.Equal(t, "l2-completed", fetchedInstance.Variables["l2Result"],
			"l2Result should be propagated through L1 from L2 event subprocess to root")
		assert.Equal(t, "l3-completed", fetchedInstance.Variables["l3Result"],
			"l3Result should be propagated through L2 and L1 from L3 event subprocess to root")

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
func requireChildEventSubProcessCompleted(t *testing.T, parentInstanceKey int64) *zenclient.ProcessInstance {
	t.Helper()
	childrenPage, err := getChildInstances(t, parentInstanceKey)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, childrenPage.Count, 1, "There should be at least one child process instance (the event subprocess)")

	var instance *zenclient.ProcessInstance
	for i := range childrenPage.Partitions {
		for j := range childrenPage.Partitions[i].Items {
			item := &childrenPage.Partitions[i].Items[j]
			if item.ParentProcessInstanceKey != nil && *item.ParentProcessInstanceKey == parentInstanceKey {
				instance = item
				break
			}
		}
	}
	require.NotNil(t, instance, "Event subprocess instance should exist as a child of the parent instance")
	assert.Equal(t, zenclient.ProcessInstanceStateCompleted, instance.State, "Event subprocess should have completed")
	return instance
}

// assertSingleJobState verifies that the given process instance has exactly one partition
// with one job, and that job is in expectedJobState state.
func assertSingleJobState(t *testing.T, instanceKey int64, expectedJobState zenclient.JobState) {
	t.Helper()
	jobs, err := getJobs(t, zenclient.GetJobsParams{
		ProcessInstanceKey: ptr.To(instanceKey),
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs.Partitions), "Should have one partition with jobs")
	assert.Equal(t, 1, len(jobs.Partitions[0].Items), "Should have one job for the process instance")
	assert.Equal(t, expectedJobState, jobs.Partitions[0].Items[0].State, "The job should be in "+expectedJobState+" state as the event subprocess should have interrupted it")
}

// assertTimerTriggered verifies that a timer with the given elementId exists in TimerStateTriggered
// for the given process instance.
func assertTimerTriggered(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	triggeredTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateTriggered)
	require.NoError(t, err)
	found := false
	for _, timer := range triggeredTimers {
		if timer.ElementId == elementId {
			found = true
			break
		}
	}
	assert.True(t, found, "expected timer with elementId %q to be in TimerStateTriggered for process instance %d, got: %+v", elementId, instanceKey, triggeredTimers)
}

// assertTimerCreated verifies that a timer with the given elementId exists in TimerStateCreated
// for the given process instance (i.e. the timer has not yet fired).
func assertTimerCreated(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	createdTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	found := false
	for _, timer := range createdTimers {
		if timer.ElementId == elementId {
			found = true
			break
		}
	}
	assert.True(t, found, "expected timer with elementId %q to be in TimerStateCreated for process instance %d, got: %+v", elementId, instanceKey, createdTimers)
}

// assertTimerCancelled verifies that a timer with the given elementId exists in TimerStateCancelled
// for the given process instance.
func assertTimerCancelled(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	cancelledTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCancelled)
	require.NoError(t, err)
	found := false
	for _, timer := range cancelledTimers {
		if timer.ElementId == elementId {
			found = true
			break
		}
	}
	assert.True(t, found, "expected timer with elementId %q to be in TimerStateCancelled for process instance %d, got: %+v", elementId, instanceKey, cancelledTimers)
}
