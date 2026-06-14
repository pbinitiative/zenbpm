package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishInstantiatingMessage publishes a message with a nil correlation key. Instantiating receive tasks
// register a definition-level message subscription (no correlation key), exactly like message start events,
// so the engine routes a nil-correlation-key publish to that subscription and creates a new process instance.
// The publish is retried because, after a previous instance completes, the definition subscription is
// re-armed asynchronously and may briefly be unavailable.
func publishInstantiatingMessage(t testing.TB, messageName string, vars *map[string]any) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			CorrelationKey: nil,
			MessageName:    messageName,
			Variables:      vars,
		})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, http.StatusCreated, resp.StatusCode(),
			"publishing the instantiating message %s should create a new process instance", messageName)
	}, 10*time.Second, 100*time.Millisecond, "instantiating message %s should be publishable", messageName)
}

// waitForActiveInstanceKeyByBpmnProcessId waits until exactly one Active process instance exists for the given
// bpmn process id and returns its key. Previously completed instances (from earlier scenarios) are ignored.
func waitForActiveInstanceKeyByBpmnProcessId(t testing.TB, bpmnProcessId string) int64 {
	t.Helper()

	var activeKey int64
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &bpmnProcessId,
		})
		if !assert.NoError(collect, err) || !assert.NotNil(collect, resp.JSON200) {
			return
		}
		found := int64(0)
		count := 0
		for _, partition := range resp.JSON200.Partitions {
			for _, item := range partition.Items {
				if item.State == zenclient.ProcessInstanceStateActive {
					found = item.Key
					count++
				}
			}
		}
		if !assert.Equal(collect, 1, count, "exactly one active instance should exist for %s", bpmnProcessId) {
			return
		}
		activeKey = found
	}, 10*time.Second, 100*time.Millisecond, "an active process instance for %s should be created by the instantiating message", bpmnProcessId)
	return activeKey
}

// assertManualStartRejected verifies that a process definition backed by an instantiating receive task
// (with no plain start event) cannot be started manually via the create-process-instance API.
func assertManualStartRejected(t testing.TB, definitionKey int64) {
	t.Helper()
	resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(), zenclient.CreateProcessInstanceJSONRequestBody{
		ProcessDefinitionKey: &definitionKey,
		Variables:            &map[string]any{},
	})
	require.NoError(t, err)
	require.NotEqual(t, http.StatusCreated, resp.StatusCode(),
		"manually starting a process definition with an instantiating receive task must be rejected")
}

// TestInstantiatingReceiveTaskBoundaryWithInterruptingMessage covers
// receive-task-boundary-message-interrupting-instantiating.bpmn, where the process starts directly with an
// instantiating ReceiveTask (instantiate="true") that has an interrupting boundary message attached.
//
//	(no start event) -> ReceiveTask_1efx577 (instantiate) -> ServiceTask_157wf2b -> SuccessEndEvent
//	                          |
//	         (boundary message "boundaryMsgRefIInst", interrupting)
//	                          v
//	                   GiveUpEndEvent
func TestInstantiatingReceiveTaskBoundaryWithInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile               = "receive-task-boundary-message-interrupting-instantiating.bpmn"
		bpmnProcessId          = "Process_msg_i_instantiating"
		receiveTaskElement     = "ReceiveTask_1efx577"
		mainServiceTaskElement = "ServiceTask_157wf2b"
		receiveMessageName     = "globalMsgRefMsgIInst"
		receiveCorrelationKey  = "correlation-key-receive-msg-i-inst"
		boundaryMessageName    = "boundaryMsgRefIInst"
		boundaryCorrelationKey = "correlation-key-boundary-msg-i-inst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	// Such a process definition must NOT be startable manually.
	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and the receive message drives the main flow", func(t *testing.T) {
		// Publishing the instantiating message creates a new process instance that waits on the receive task.
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		// The new instance must wait on the receive task: its own subscription plus the boundary subscription.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 2*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		// Correlate the receive task message (with its real correlation key) to drive the main flow.
		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "receive message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set when the main flow runs")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("instantiating message creates the instance and the boundary message interrupts it", func(t *testing.T) {
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 2*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		// Correlate the boundary message: the interrupting boundary must terminate the receive task and
		// take the give-up path which completes the process instance.
		require.NoError(t, publishMessage(t, boundaryMessageName, boundaryCorrelationKey, &map[string]any{}))

		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["boundaryFired"], "boundary output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		active, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instanceKey, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		require.Empty(t, active, "no active message subscription should remain after the interrupting boundary fires")
	})
}

// TestInstantiatingReceiveTaskBoundaryWithNonInterruptingMessage covers
// receive-task-boundary-message-noninterrupting-instantiating.bpmn.
func TestInstantiatingReceiveTaskBoundaryWithNonInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile                 = "receive-task-boundary-message-noninterrupting-instantiating.bpmn"
		bpmnProcessId            = "Process_msg_ni_instantiating"
		receiveTaskElement       = "ReceiveTask_1efx577"
		mainServiceTaskElement   = "ServiceTask_157wf2b"
		giveUpServiceTaskElement = "GiveUpServiceTask_0ab12cd"
		receiveMessageName       = "globalMsgRefMsgNIInst"
		receiveCorrelationKey    = "correlation-key-receive-msg-ni-inst"
		boundaryMessageName      = "boundaryMsgRefNIInst"
		boundaryCorrelationKey   = "correlation-key-boundary-msg-ni-inst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and the receive message drives the main flow", func(t *testing.T) {
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 2*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "receive message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set when the main flow runs")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("instantiating message creates the instance and a boundary message runs the give-up flow", func(t *testing.T) {
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Equal(collect, 2, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
				"receive task and boundary message subscriptions should both be active")
		}, 2*time.Second, 100*time.Millisecond, "receive task and boundary subscriptions should be created")

		require.NoError(t, publishMessage(t, boundaryMessageName, boundaryCorrelationKey, &map[string]any{}))

		giveUpJob := waitForProcessInstanceActiveJobByElementId(t, instanceKey, giveUpServiceTaskElement)
		require.Equal(t, giveUpServiceTaskElement, giveUpJob.ElementId)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["boundaryFired"], "boundary output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		// Non-interrupting: the receive task must remain active and waiting.
		require.GreaterOrEqual(t, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement), 1,
			"receive task message subscription must remain active after a non-interrupting boundary message")

		require.NoError(t, completeJob(t, giveUpJob.Key, map[string]any{}))
		require.Never(t, func() bool {
			pi, err := getProcessInstance(t, instanceKey)
			return err == nil && pi.State == zenclient.ProcessInstanceStateCompleted
		}, 500*time.Millisecond, 100*time.Millisecond,
			"instance must not complete while the receive task is still waiting")

		// Finally correlate the receive message to complete the main flow and the instance.
		require.NoError(t, publishMessage(t, receiveMessageName, receiveCorrelationKey, &map[string]any{
			"approved": true,
		}))
		completeJobForElementId(t, instanceKey, mainServiceTaskElement, map[string]any{})
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})
}

// TestInstantiatingReceiveTaskBoundaryWithInterruptingTimer covers
// receive-task-boundary-timer-interrupting-instantiating.bpmn.
func TestInstantiatingReceiveTaskBoundaryWithInterruptingTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile           = "receive-task-boundary-timer-interrupting-instantiating.bpmn"
		bpmnProcessId      = "Process_0anusn1_instantiating"
		serviceTaskElement = "ServiceTask_157wf2b"
		messageName        = "globalMsgRefTimerIInst"
		correlationKey     = "correlation-key-receive-timer-i-inst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and the receive message arrives before the timer", func(t *testing.T) {
		publishInstantiatingMessage(t, messageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			subs, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instanceKey, bpmnruntime.ActivityStateActive)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(subs), "receive task should have exactly one active message subscription")
			timers, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(timers), "receive task should have one created boundary timer")
		}, 2*time.Second, 100*time.Millisecond, "receive task subscription and boundary timer should be created")

		require.NoError(t, publishMessage(t, messageName, correlationKey, &map[string]any{
			"approved": true,
		}))

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, serviceTaskElement)
		require.Equal(t, serviceTaskElement, job.ElementId)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["timerFired"], "boundary timer output must not be set when the main flow runs")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})

	t.Run("instantiating message creates the instance and the interrupting timer fires", func(t *testing.T) {
		publishInstantiatingMessage(t, messageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		// No receive message is published; the interrupting boundary timer must fire and complete the instance.
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["timerFired"], "boundary timer output variable should be propagated to the instance")

		active, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instanceKey, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		require.Empty(t, active, "receive task message subscription should be terminated after the interrupting timer fires")
	})
}

// TestInstantiatingReceiveTaskBoundaryWithNonInterruptingTimer covers
// receive-task-boundary-timer-noninterrupting-instantiating.bpmn.
func TestInstantiatingReceiveTaskBoundaryWithNonInterruptingTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile                 = "receive-task-boundary-timer-noninterrupting-instantiating.bpmn"
		bpmnProcessId            = "Process_timer_ni_instantiating"
		receiveTaskElement       = "ReceiveTask_1efx577"
		mainServiceTaskElement   = "ServiceTask_157wf2b"
		giveUpServiceTaskElement = "GiveUpServiceTask_0ab12cd"
		messageName              = "globalMsgRefTimerNIInst"
		correlationKey           = "correlation-key-receive-timer-ni-inst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and the non-interrupting timer fires", func(t *testing.T) {
		publishInstantiatingMessage(t, messageName, &map[string]any{})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			created, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, 1, len(created), "the give-up boundary timer should be created")
		}, 2*time.Second, 100*time.Millisecond, "boundary timer should be created")

		// The non-interrupting timer fires and the give-up branch reaches its own service task while the
		// receive task keeps waiting.
		giveUpJob := waitForProcessInstanceJobByElementId(t, instanceKey, giveUpServiceTaskElement, public.JobStateActive)
		require.Equal(t, giveUpServiceTaskElement, giveUpJob.ElementId)

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["timerFired"], "boundary timer output variable should be propagated to the instance")
		require.Nil(t, fetched.Variables["approved"], "main flow message variables must not be set when the give-up flow runs")

		require.Equal(t, 1, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
			"receive task message subscription must remain active after a non-interrupting timer fires")

		require.NoError(t, completeJob(t, giveUpJob.Key, map[string]any{}))
		require.Never(t, func() bool {
			pi, err := getProcessInstance(t, instanceKey)
			return err == nil && pi.State == zenclient.ProcessInstanceStateCompleted
		}, 500*time.Millisecond, 100*time.Millisecond,
			"instance must not complete while the receive task is still waiting")

		// Finally correlate the receive message to complete the main flow and the instance.
		require.NoError(t, publishMessage(t, messageName, correlationKey, &map[string]any{
			"approved": true,
		}))
		completeJobForElementId(t, instanceKey, mainServiceTaskElement, map[string]any{})
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})
}
