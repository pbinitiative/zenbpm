package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReceiveTaskBoundaryWithTimer exercises a BPMN ReceiveTask that has an interrupting timer
// boundary event attached (give-up timer). The diagram (receive-task-boundary-timer-interrupting.bpmn):
//
//	StartEvent -> ReceiveTask_1efx577 -> ServiceTask_157wf2b -> SuccessEndEvent
//	                    |
//	          (boundary timer PT10S, interrupting)
//	                    v
//	             GiveUpEndEvent
//
// A ReceiveTask behaves like a Message Intermediate Catch Event: it waits for a message
// to be correlated. Two scenarios are covered:
//  1. The message arrives first -> the receive task completes, the boundary timer is
//     cancelled and the flow continues to the service task.
//  2. The message never arrives -> the interrupting boundary timer fires, the receive
//     task is interrupted (its message subscription is terminated) and the flow continues
//     to the give-up end event, completing the process instance.
func TestReceiveTaskBoundaryWithTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile           = "receive-task-boundary-timer-interrupting.bpmn"
		bpmnProcessId      = "Process_0anusn1"
		receiveTaskElement = "ReceiveTask_1efx577"
		serviceTaskElement = "ServiceTask_157wf2b"
		messageName        = "globalMsgRef"
		correlationKey     = "correlation-key-receive-1"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	t.Run("message correlated before interrupting timer completes the receive task", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)
		require.Equal(t, zenclient.ProcessInstanceStateActive, instance.State,
			"process instance should be Active while waiting on the receive task")

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		// The receive task must create an active message subscription and the boundary timer.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, bpmnruntime.ActivityStateActive)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(subs), "receive task should have exactly one active message subscription")
			if len(subs) == 1 {
				assert.Equal(collect, receiveTaskElement, subs[0].MessageSubscription().ElementId)
			}
			timers, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(timers), "receive task should have one created boundary timer")
		}, 1*time.Second, 100*time.Millisecond, "receive task subscription and boundary timer should be created")

		// Correlate the message: this must drive the flow past the receive task into the service task.
		require.NoError(t, publishMessage(t, messageName, correlationKey, &map[string]any{
			"approved": true,
		}))

		// A job for the downstream service task must be created, proving the receive task completed.
		job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, serviceTaskElement)
		require.Equal(t, serviceTaskElement, job.ElementId)

		// The message variables must be propagated onto the process instance.
		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "message variables should be propagated to the instance")

		// The boundary timer must be cancelled once the receive task is completed.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Empty(collect, created, "no created boundary timer should remain after the receive task completes")
		}, 1*time.Second, 100*time.Millisecond, "boundary timer should be cancelled")

		// Completing the service task job drives the instance to Completed.
		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("interrupting timer fires when no message arrives", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		// The boundary timer must be created while the receive task waits.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(created), "the give-up boundary timer should be created")
		}, 1*time.Second, 100*time.Millisecond, "boundary timer should be created")

		// No message is published. The interrupting boundary timer (PT1S) must fire,
		// take the give-up path and complete the process instance.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			pi, err := getProcessInstance(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, pi.State,
				"process instance should be Completed after the give-up timer fires")
		}, 2*time.Second, 200*time.Millisecond, "process instance should reach Completed via the give-up timer")

		// The output variable from the boundary timer must be propagated to the process instance.
		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["timerFired"], "boundary timer output variable should be propagated to the instance")

		// The boundary timer must no longer be in the Created state: it fired and interrupted
		// the receive task. An interrupting boundary timer ends up Cancelled because firing
		// also cancels the activity's boundary subscriptions (including its own timer).
		created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
		require.NoError(t, err)
		require.Empty(t, created, "no Created boundary timer should remain after the give-up timer fires")

		// The receive task message subscription must have been terminated (interrupted).
		active, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		require.Empty(t, active, "receive task message subscription should be terminated after the interrupting timer fires")
	})
}

// activeReceiveTaskSubscriptions returns the active message subscriptions owned by the given receive
// task element. Both the receive task's own message subscription and the subscriptions of any
// attached boundary message events are persisted with the receive task's element id (the engine
// registers boundary message subscriptions against the activity they are attached to), so they are
// counted together here.
func activeReceiveTaskSubscriptions(t testing.TB, store storage.Storage, processInstanceKey int64, receiveTaskElement string) int {
	t.Helper()
	subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, bpmnruntime.ActivityStateActive)
	require.NoError(t, err)
	count := 0
	for _, sub := range subs {
		if sub.MessageSubscription().ElementId == receiveTaskElement {
			count++
		}
	}
	return count
}

// TestReceiveTaskBoundaryWithNonInterruptingTimer exercises a BPMN ReceiveTask that has a
// NON-interrupting timer boundary event attached. The diagram
// (receive-task-boundary-timer-noninterrupting.bpmn):
//
//	StartEvent -> ReceiveTask_1efx577 -> ServiceTask_157wf2b -> SuccessEndEvent
//	                    |
//	      (boundary timer PT1S, non-interrupting)
//	                    v
//	          GiveUpServiceTask_0ab12cd -> GiveUpEndEvent
//
// Because the boundary timer is non-interrupting the receive task keeps waiting after the timer
// fires; the give-up branch runs in parallel and flows through an extra service task so that the
// process instance is NOT completed immediately, which makes the variable propagation observable.
// Two scenarios are covered, each propagating a different output variable depending on the flow:
//  1. Main flow: the message arrives first -> "approved" (from the message) is propagated and the
//     boundary timer is cancelled before it can fire ("timerFired" must NOT be set).
//  2. Give-up flow: the non-interrupting timer fires -> "timerFired" (boundary output) is
//     propagated, the receive task stays active and "approved" must NOT be set.
func TestReceiveTaskBoundaryWithNonInterruptingTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile                 = "receive-task-boundary-timer-noninterrupting.bpmn"
		bpmnProcessId            = "Process_timer_ni"
		receiveTaskElement       = "ReceiveTask_1efx577"
		mainServiceTaskElement   = "ServiceTask_157wf2b"
		giveUpServiceTaskElement = "GiveUpServiceTask_0ab12cd"
		messageName              = "globalMsgRefTimerNI"
		correlationKey           = "correlation-key-receive-timer-ni"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	t.Run("message correlated before non-interrupting timer fires drives the main flow", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)
		require.Equal(t, zenclient.ProcessInstanceStateActive, instance.State,
			"process instance should be Active while waiting on the receive task")

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		// The receive task must create an active message subscription and the boundary timer.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, bpmnruntime.ActivityStateActive)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(subs), "receive task should have exactly one active message subscription")
			if len(subs) == 1 {
				assert.Equal(collect, receiveTaskElement, subs[0].MessageSubscription().ElementId)
			}
			timers, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(timers), "receive task should have one created boundary timer")
		}, 1*time.Second, 100*time.Millisecond, "receive task subscription and boundary timer should be created")

		// Correlate the message: this must drive the flow past the receive task into the main service task.
		require.NoError(t, publishMessage(t, messageName, correlationKey, &map[string]any{
			"approved": true,
		}))

		// A job for the downstream main service task must be created, proving the receive task completed.
		job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		// Only the message (main flow) variables must be propagated; the give-up branch must not run.
		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["timerFired"], "boundary timer output must not be set when the main flow runs")

		// The boundary timer must be cancelled once the receive task is completed.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Empty(collect, created, "no created boundary timer should remain after the receive task completes")
		}, 1*time.Second, 100*time.Millisecond, "boundary timer should be cancelled")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("non-interrupting timer fires while the receive task keeps waiting", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			created, err := store.FindProcessInstanceTimers(t.Context(), instance.Key, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(created), "the give-up boundary timer should be created")
		}, 1*time.Second, 100*time.Millisecond, "boundary timer should be created")

		// No receive message is published. The non-interrupting timer (PT1S) fires and the give-up
		// branch must reach its own service task without completing the instance.
		giveUpJob := waitForProcessInstanceJobByElementId(t, instance.Key, giveUpServiceTaskElement, public.JobStateActive)
		require.Equal(t, giveUpServiceTaskElement, giveUpJob.ElementId)

		// Only the boundary timer output must be propagated for the give-up flow.
		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["timerFired"], "boundary timer output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		// Because the timer is non-interrupting the receive task must remain active.
		require.Equal(t, 1, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
			"receive task message subscription must remain active after a non-interrupting timer fires")

		// Completing the give-up job ends the give-up branch but the instance must stay Active
		// because the receive task is still waiting for its message.
		require.NoError(t, completeJob(t, giveUpJob.Key, map[string]any{}))
		require.Never(t, func() bool {
			pi, err := getProcessInstance(t, instance.Key)
			return err == nil && pi.State == zenclient.ProcessInstanceStateCompleted
		}, 500*time.Millisecond, 100*time.Millisecond,
			"instance must not complete while the receive task is still waiting")

		// Finally correlate the receive message to complete the main flow and the instance.
		require.NoError(t, publishMessage(t, messageName, correlationKey, &map[string]any{
			"approved": true,
		}))
		completeJobForElementId(t, instance.Key, mainServiceTaskElement, map[string]any{})
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})
}

// TestReceiveTaskBoundaryWithNonInterruptingMessage exercises a BPMN ReceiveTask that has a
// NON-interrupting message boundary event attached. The diagram
// (receive-task-boundary-message-noninterrupting.bpmn):
//
//	StartEvent -> ReceiveTask_1efx577 -> ServiceTask_157wf2b -> SuccessEndEvent
//	                    |
//	    (boundary message "boundaryMsgRefNI", non-interrupting)
//	                    v
//	          GiveUpServiceTask_0ab12cd -> GiveUpEndEvent
//
// The receive task waits for "globalMsgRefMsgNI" while the boundary waits for a different message
// "boundaryMsgRefNI". Because the boundary is non-interrupting the give-up branch runs through an
// extra service task without completing the instance. Two scenarios are covered, each propagating
// a different output variable depending on the flow:
//  1. Main flow: the receive message arrives -> "approved" is propagated and "boundaryFired" must NOT be set.
//  2. Give-up flow: the boundary message arrives -> "boundaryFired" (boundary output) is propagated,
//     the receive task stays active and "approved" must NOT be set.
func TestReceiveTaskBoundaryWithNonInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile                 = "receive-task-boundary-message-noninterrupting.bpmn"
		bpmnProcessId            = "Process_msg_ni"
		receiveTaskElement       = "ReceiveTask_1efx577"
		mainServiceTaskElement   = "ServiceTask_157wf2b"
		giveUpServiceTaskElement = "GiveUpServiceTask_0ab12cd"
		receiveMessageName       = "globalMsgRefMsgNI"
		receiveCorrelationKey    = "correlation-key-receive-msg-ni"
		boundaryMessageName      = "boundaryMsgRefNI"
		boundaryCorrelationKey   = "correlation-key-boundary-msg-ni"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	t.Run("receive message correlated drives the main flow", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		// Both the receive task's own subscription and the boundary message subscription must be
		// active. They are both registered against the receive task element, so we expect two.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 1*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		// Correlate the receive task message: this must drive the main flow into the main service task.
		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))

		job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "receive message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set when the main flow runs")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("boundary message correlated runs the give-up flow while the receive task keeps waiting", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 1*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		// Correlate the boundary message: the non-interrupting give-up branch must reach its
		// own service task without completing the instance.
		require.NoError(t, publishMessage(t, boundaryMessageName, boundaryCorrelationKey, &map[string]any{}))

		giveUpJob := waitForProcessInstanceActiveJobByElementId(t, instance.Key, giveUpServiceTaskElement)
		require.Equal(t, giveUpServiceTaskElement, giveUpJob.ElementId)

		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["boundaryFired"], "boundary output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		// Because the boundary is non-interrupting the receive task must remain active and waiting.
		require.GreaterOrEqual(t, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement), 1,
			"receive task message subscription must remain active after a non-interrupting boundary message")

		require.NoError(t, completeJob(t, giveUpJob.Key, map[string]any{}))
		require.Never(t, func() bool {
			pi, err := getProcessInstance(t, instance.Key)
			return err == nil && pi.State == zenclient.ProcessInstanceStateCompleted
		}, 500*time.Millisecond, 100*time.Millisecond,
			"instance must not complete while the receive task is still waiting")

		// Finally correlate the receive message to complete the main flow and the instance.
		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))
		completeJobForElementId(t, instance.Key, mainServiceTaskElement, map[string]any{})
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})
}

// TestReceiveTaskBoundaryWithInterruptingMessage exercises a BPMN ReceiveTask that has an
// INTERRUPTING message boundary event attached. The diagram
// (receive-task-boundary-message-interrupting.bpmn):
//
//	StartEvent -> ReceiveTask_1efx577 -> ServiceTask_157wf2b -> SuccessEndEvent
//	                    |
//	    (boundary message "boundaryMsgRefI", interrupting)
//	                    v
//	             GiveUpEndEvent
//
// The receive task waits for "globalMsgRefMsgI" while the boundary waits for "boundaryMsgRefI".
// Two scenarios are covered, each propagating a different output variable depending on the flow:
//  1. Main flow: the receive message arrives -> "approved" is propagated, the boundary subscription
//     is cancelled and "boundaryFired" must NOT be set.
//  2. Give-up flow: the boundary message arrives -> it interrupts the receive task (its subscription
//     is terminated), "boundaryFired" is propagated, "approved" must NOT be set and the instance
//     completes via the give-up end event.
func TestReceiveTaskBoundaryWithInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile               = "receive-task-boundary-message-interrupting.bpmn"
		bpmnProcessId          = "Process_msg_i"
		receiveTaskElement     = "ReceiveTask_1efx577"
		mainServiceTaskElement = "ServiceTask_157wf2b"
		receiveMessageName     = "globalMsgRefMsgI"
		receiveCorrelationKey  = "correlation-key-receive-msg-i"
		boundaryMessageName    = "boundaryMsgRefI"
		boundaryCorrelationKey = "correlation-key-boundary-msg-i"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	t.Run("receive message correlated drives the main flow and cancels the boundary", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 1*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))

		job := waitForProcessInstanceActiveJobByElementId(t, instance.Key, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "receive message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set when the main flow runs")

		// Completing the receive task must cancel the boundary subscription as well, leaving none active.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 0, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
				"boundary subscription should be cancelled after the receive task completes")
		}, 1*time.Second, 100*time.Millisecond, "boundary subscription should be cancelled")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("boundary message interrupts the receive task and runs the give-up flow", func(t *testing.T) {
		instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		require.NoError(t, err)
		require.NotZero(t, instance.Key)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instance.Key, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 1*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		// Correlate the boundary message: the interrupting boundary must terminate the receive task
		// and take the give-up path which completes the process instance.
		require.NoError(t, publishMessage(t, boundaryMessageName, boundaryCorrelationKey, &map[string]any{}))

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		fetched, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["boundaryFired"], "boundary output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		// The receive task subscription must have been terminated by the interrupting boundary.
		active, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instance.Key, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		require.Empty(t, active, "no active message subscription should remain after the interrupting boundary fires")
	})
}
