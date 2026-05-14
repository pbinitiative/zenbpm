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

// TestMessageEventSubProcess mirrors TestTimerEventSubProcess but uses message
// start events on event subprocesses instead of timer start events. Behavior
// is analogous: an interrupting message-start event subprocess cancels its
// parent's running activities and propagates output variables; a non-interrupting
// one runs in parallel; nested message event subprocesses cascade up.
func TestMessageEventSubProcess(t *testing.T) {
	cleanProcessInstances(t)

	t.Run("test job completion cancels event subprocess message subscription", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// message event sub process definition: (message start event) -> (end event)

		// When the main service task job is completed before the message arrives, the
		// event subprocess message subscription should be terminated together with the
		// parent process instance (analogous to timer-start subscriptions being moved
		// to TimerStateCancelled).

		definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-interrupting.bpmn", "Process_messageEventSubProcessInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service task is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		assertMessageSubscriptionActive(t, instance.Key, "subProcessMessageEvent_12i3m6f")

		// Read and complete the active job for the service task
		jobs, err := getJobs(t, zenclient.GetJobsParams{
			JobType:            ptr.To("input-task-message-event-subprocess-interrupting"),
			ProcessInstanceKey: ptr.To(instance.Key),
		})
		assert.NoError(t, err)
		require.Equal(t, 1, len(jobs.Partitions), "Should have exactly one partition with jobs")
		require.Equal(t, 1, len(jobs.Partitions[0].Items), "Should have exactly one active job")

		err = completeJob(t, jobs.Partitions[0].Items[0].Key, map[string]any{})
		assert.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		assertMessageSubscriptionTerminated(t, instance.Key, "subProcessMessageEvent_12i3m6f")
	})

	t.Run("test interrupting message event sub process", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// message event sub process definition: (message start event) -> (end event)

		definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-interrupting.bpmn", "Process_messageEventSubProcessInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		assertMessageSubscriptionActive(t, instance.Key, "subProcessMessageEvent_12i3m6f")

		err = publishMessage(t, "globalMessageRef", "correlation-key-event-subprocess-1", &map[string]any{})
		assert.NoError(t, err)

		// the job of the main service task should be in TERMINATED state as the event subprocess should have interrupted it
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		_ = requireChildEventSubProcessCompleted(t, instance.Key)

		// Verify the parent process instance is also completed
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		fetchedProcessInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)

		assertMessageSubscriptionCompleted(t, instance.Key, "subProcessMessageEvent_12i3m6f")

		vars := fetchedProcessInstance.Variables

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, "message-fired", vars["subProcessResult"],
				"subProcessResult should be set by the event subprocess output mapping")
			assert.Equal(collect, true, vars["messageInterrupted"],
				"messageInterrupted should be set by the event subprocess output mapping")
			assert.Equal(collect, "message-event-subprocess", vars["interruptedBy"],
				"interruptedBy should be set by the event subprocess output mapping")
			assert.Equal(collect, "messageStartEventOutputValue", vars["messageStartEventOutputVar"],
				"messageStartEventOutputVar should be set by the event subprocess output mapping")
		}, 15*time.Second, 100*time.Millisecond, "Should have triggered a message event")
	})

	t.Run("test non-interrupting message event sub process", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task) -> (end event)
		// message event sub process definition: (non-interrupting message start event) -> (end event)

		definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-non-interrupting.bpmn", "Process_messageEventSubProcessNonInterrupting")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// before the message is published, the event subprocess message subscription should be active
		assertMessageSubscriptionActive(t, instance.Key, "eventSubprocessMessageEvent_12i3m6f")

		err = publishMessage(t, "messageNonInterruptingRef", "correlation-key-event-subprocess-non-interrupting", &map[string]any{})
		assert.NoError(t, err)

		subProcessInstanceWithMessage := waitForChildProcessInstance(t, instance.Key)
		assertProcessInstanceTokenState(t, subProcessInstanceWithMessage.Key, "end_event_non_interrupting", bpmnruntime.TokenStateCompleted)
		waitForProcessInstanceState(t, subProcessInstanceWithMessage.Key, zenclient.ProcessInstanceStateCompleted)

		// the job of the main service task should be still in ACTIVE state as the event subprocess is non-interrupting
		assertSingleJobState(t, instance.Key, zenclient.JobStateActive)

		// Verify the parent process instance is still active as the event subprocess is non-interrupting
		fetchedInstance, err = getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State,
			"Parent process instance should be active as the event subprocess is non-interrupting and parent process is still waiting for a job worker on service-task-1")

		// after the event subprocess completes, the message subscription should be completed
		assertMessageSubscriptionCompleted(t, instance.Key, "eventSubprocessMessageEvent_12i3m6f")

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "non-interrupting-done", fetchedInstance.Variables["eventSubProcessResult"],
				"eventSubProcessResult should be propagated from the non-interrupting event subprocess to the parent")
			assert.Equal(collect, true, fetchedInstance.Variables["nonInterruptingExecuted"],
				"nonInterruptingExecuted should be propagated from the non-interrupting event subprocess to the parent")
			assert.Equal(collect, "messageStartEventOutputValue", fetchedInstance.Variables["messageStartEventOutputVar"],
				"messageStartEventOutputVar (non-interrupting message start event output mapping) should be propagated to the parent")
		}, 15*time.Second, 100*time.Millisecond, "the event subprocess output variables should be propagated to the still-active parent instance")
	})

	t.Run("test interrupting message event nested sub processes", func(t *testing.T) {
		// parent process definition: (plain start event) -> (service task 1) -> (end event)
		// event sub process L1: (message start event) -> (service task 2) -> (end event)
		// event sub process L2: (message start event) -> (service task 3) -> (end event)
		// event sub process L3: (message start event) -> (end event)

		definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-nested-interrupting.bpmn", "Process_messageEventSubProcessInterruptingNested")
		assert.NoError(t, err)
		assert.NotZero(t, definition.Key, "Definition key should not be zero")

		instance, err := createProcessInstance(t, &definition.Key, nil)
		assert.NoError(t, err)
		assert.NotZero(t, instance.Key, "Process instance key should not be zero")

		// Verify the instance is active (service-task-1 is waiting for a job worker)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// root process L1 message subscription should be active
		assertMessageSubscriptionActive(t, instance.Key, "eventSubprocessL1MessageEvent_12i3m6f")

		// Publish L1 message to start the L1 event subprocess (interrupts root service task)
		err = publishMessage(t, "messageL1Ref", "correlation-key-message-l1", &map[string]any{})
		assert.NoError(t, err)

		// L1 child instance should now exist; its L2 message subscription should be active
		l1Instance := getFirstChildInstance(t, instance.Key)
		assertMessageSubscriptionActive(t, l1Instance.Key, "eventSubProcessL2MessageEvent_075kpin")

		// Publish L2 message to start the L2 event subprocess (interrupts L1 service task)
		err = publishMessage(t, "messageL2Ref", "correlation-key-message-l2", &map[string]any{})
		assert.NoError(t, err)

		// L2 child instance should now exist; its L3 message subscription should be active
		l2Instance := getFirstChildInstance(t, l1Instance.Key)
		assertMessageSubscriptionActive(t, l2Instance.Key, "eventSubProcessL3MessageEvent_0zi70w1")

		// Publish L3 message to start the L3 event subprocess (interrupts L2 service task).
		// The L3 event subprocess goes start->end and cascades completion all the way to root.
		err = publishMessage(t, "messageL3Ref", "correlation-key-message-l3", &map[string]any{})
		assert.NoError(t, err)

		// ROOT parent instance: verify root job is terminated
		assertSingleJobState(t, instance.Key, zenclient.JobStateTerminated)

		// ROOT parent instance: Verify the parent process instance is also completed and has all level outputs
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "l1-completed", fetchedInstance.Variables["l1Result"],
				"l1Result should be propagated from L1 event subprocess to root")
			assert.Equal(collect, "l2-completed", fetchedInstance.Variables["l2Result"],
				"l2Result should be propagated through L1 from L2 event subprocess to root")
			assert.Equal(collect, "l3-completed", fetchedInstance.Variables["l3Result"],
				"l3Result should be propagated through L2 and L1 from L3 event subprocess to root")
			// L1 message start event output mapping targets the root parent
			// instance directly (PublishMessageOnEventSubprocess sets the
			// mapped target on the parent of the event subprocess).
			assert.Equal(collect, "l1MessageStartEventValue", fetchedInstance.Variables["l1MessageStartEventVar"],
				"l1MessageStartEventVar from the L1 message start event output mapping must be propagated to the root parent")
			// l3MessageStartEventVar is set by the L3 message start event on the
			// L2 instance, then each ancestor event subprocess (L2, L1) appends
			// its own suffix via output mappings, so the root parent observes
			// the fully cascaded value.
			assert.Equal(collect, "l3-l2-l1", fetchedInstance.Variables["l3MessageStartEventVar"],
				"l3MessageStartEventVar must be propagated all the way to the root parent with each event subprocess level appending its suffix")
		}, 15*time.Second, 100*time.Millisecond, "Parent process instance should be completed with output variables as the event subprocess is interrupting and was completed")

		// after process completion, root instance L1 message subscription should be completed
		assertMessageSubscriptionCompleted(t, instance.Key, "eventSubprocessL1MessageEvent_12i3m6f")

		// L1: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL1 := requireChildEventSubProcessCompleted(t, instance.Key)
		// L1: Verify the jobs of L1 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL1.Key, zenclient.JobStateTerminated)
		// L1: verify L2 message subscription state
		assertMessageSubscriptionCompleted(t, eventSubProcessInstanceL1.Key, "eventSubProcessL2MessageEvent_075kpin")
		// L1: the L2 message start event output mapping targets the L1 instance
		// (the parent of the L2 event subprocess). It is NOT cascaded further up
		// because the L1 subprocess output mapping does not include it.
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedL1, err := getProcessInstance(t, eventSubProcessInstanceL1.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "l2MessageStartEventValue", fetchedL1.Variables["l2MessageStartEventVar"],
				"l2MessageStartEventVar from the L2 message start event output mapping must be set on the L1 instance")
		}, 15*time.Second, 100*time.Millisecond, "L2 message start event output variable should be set on the L1 instance")

		// L2: Verify the event subprocess child instance was created and completed
		eventSubProcessInstanceL2 := requireChildEventSubProcessCompleted(t, eventSubProcessInstanceL1.Key)
		// L2: Verify the jobs of L2 event sub process are terminated
		assertSingleJobState(t, eventSubProcessInstanceL2.Key, zenclient.JobStateTerminated)
		// L2: verify L3 message subscription state
		assertMessageSubscriptionCompleted(t, eventSubProcessInstanceL2.Key, "eventSubProcessL3MessageEvent_0zi70w1")
		// L2: the L3 message start event output mapping targets the L2 instance
		// (the parent of the L3 event subprocess). The L2 subprocess output
		// mapping then appends "-l2" when propagating it to L1, but the L2
		// instance itself still observes the unappended base value.
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedL2, err := getProcessInstance(t, eventSubProcessInstanceL2.Key)
			assert.NoError(collect, err)
			assert.Equal(collect, "l3", fetchedL2.Variables["l3MessageStartEventVar"],
				"l3MessageStartEventVar from the L3 message start event output mapping must be set on the L2 instance with the unappended base value")
		}, 15*time.Second, 100*time.Millisecond, "L3 message start event output variable should be set on the L2 instance")

		// L3: Verify the event subprocess child instance was created and completed. L3 has no jobs to verify.
		requireChildEventSubProcessCompleted(t, eventSubProcessInstanceL2.Key)
	})
}

func assertMessageSubscriptionActive(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	assertMessageSubscriptionInState(t, instanceKey, elementId, bpmnruntime.ActivityStateActive)
}

func assertMessageSubscriptionTerminated(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	assertMessageSubscriptionInState(t, instanceKey, elementId, bpmnruntime.ActivityStateTerminated)
}

func assertMessageSubscriptionCompleted(t *testing.T, instanceKey int64, elementId string) {
	t.Helper()
	assertMessageSubscriptionInState(t, instanceKey, elementId, bpmnruntime.ActivityStateCompleted)
}

func assertMessageSubscriptionInState(t *testing.T, instanceKey int64, elementId string, expectedState bpmnruntime.ActivityState) {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instanceKey, expectedState)
		if !assert.NoError(collect, err) {
			return
		}
		found := false
		for _, sub := range subs {
			if sub.MessageSubscription().ElementId == elementId {
				found = true
				break
			}
		}
		assert.True(collect, found, "expected message subscription with elementId %q to be in state %v for process instance %d, got: %+v", elementId, expectedState, instanceKey, subs)
	}, 15*time.Second, 100*time.Millisecond, "message subscription should be in state %v", expectedState)
}
