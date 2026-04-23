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
		assertTimersCreated(t, instance.Key)

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
		assertTimersTriggered(t, instance.Key)
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
		assertTimersCreated(t, instance.Key)

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
		assertTimersTriggered(t, instance.Key)
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
		assertTimersCreated(t, instance.Key)

		// Wait for L1 event subprocess timer (PT1S) to fire and create the L1 child instance
		time.Sleep(1500 * time.Millisecond)

		// L1 child instance should now exist and be active; its L2 timer should be in TimerStateCreated
		l1Instance := getFirstChildInstance(t, instance.Key)
		assertTimersCreated(t, l1Instance.Key)

		// Wait for L2 event subprocess timer (PT1S) to fire and create the L2 child instance
		time.Sleep(1000 * time.Millisecond)

		// L2 child instance should now exist and be active; its L3 timer should be in TimerStateCreated
		l2Instance := getFirstChildInstance(t, l1Instance.Key)
		assertTimersCreated(t, l2Instance.Key)

		// Wait for L3 event subprocess timer (PT1S) to fire and all instances to complete
		time.Sleep(2 * time.Second)

		// ROOT parent instance: verify jobs are terminated
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)
		// ROOT parent instance: Verify the parent process instance is also completed
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State,
			"Parent process instance should be completed as the event subprocess is interrupting and was completed")

		// after process completion, root instance timers should be in TimerStateTriggered
		assertTimersTriggered(t, instance.Key)

		// L1: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL1 := requireChildEventSubProcessCompleted(t, instance.Key)
		// L1: Verify the jobs of L1 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL1.Key, zenclient.JobStateTerminated)
		// L1: verify timer state
		assertTimersTriggered(t, eventSubProcessInstanceL1.Key)

		// L2: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL2 := requireChildEventSubProcessCompleted(t, eventSubProcessInstanceL1.Key)
		// L2: Verify the jobs of L2 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL2.Key, zenclient.JobStateTerminated)
		// L2: verify timer state
		assertTimersTriggered(t, eventSubProcessInstanceL2.Key)

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

// assertTimersTriggered verifies that at least one timer exists in TimerStateTriggered
// and no timers exist in TimerStateCreated for the given process instance.
func assertTimersTriggered(t *testing.T, instanceKey int64) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	triggeredTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateTriggered)
	require.NoError(t, err)
	assert.NotEmpty(t, triggeredTimers, "at least one timer should be in TimerStateTriggered for process instance %d", instanceKey)
	createdTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, createdTimers, "no timers should remain in TimerStateCreated for process instance %d", instanceKey)
}

// assertTimersCreated verifies that at least one timer exists in TimerStateCreated
// for the given process instance (i.e. the timer has not yet fired).
func assertTimersCreated(t *testing.T, instanceKey int64) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	createdTimers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	assert.NotEmpty(t, createdTimers, "at least one timer should be in TimerStateCreated for process instance %d before it fires", instanceKey)
}
