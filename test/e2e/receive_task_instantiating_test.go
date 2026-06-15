package e2e

import (
	"net/http"
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
//	ReceiveTask_1efx577 (instantiate) -> ServiceTask_157wf2b -> SuccessEndEvent
//	                          |
//	         (boundary message "boundaryMsgRefIInst", interrupting)
//	                          v
//	                   GiveUpEndEvent
//
// Per BPMN semantics the instantiating message both creates the instance and satisfies the receive task, so the
// token transitions straight to the service task and the boundary subscription is cancelled without ever firing.
func TestInstantiatingReceiveTaskBoundaryWithInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile               = "receive-task-boundary-message-interrupting-instantiating.bpmn"
		bpmnProcessId          = "Process_msg_i_instantiating"
		receiveTaskElement     = "ReceiveTask_1efx577"
		mainServiceTaskElement = "ServiceTask_157wf2b"
		receiveMessageName     = "globalMsgRefMsgIInst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	// Such a process definition must NOT be startable manually.
	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and drives the main flow past the receive task", func(t *testing.T) {
		// Publishing the instantiating message creates a new process instance AND satisfies the receive task.
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{"approved": true})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		require.Equal(t, 0, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
			"the receive task and its boundary subscription must be consumed by the instantiating message")

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "instantiating message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set; the receive task completed on instantiation")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)

		active, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), instanceKey, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		require.Empty(t, active, "no active message subscription should remain after the instance completes")
	})
}

func TestInstantiatingReceiveTaskBoundaryWithNonInterruptingMessage(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile               = "receive-task-boundary-message-noninterrupting-instantiating.bpmn"
		bpmnProcessId          = "Process_msg_ni_instantiating"
		receiveTaskElement     = "ReceiveTask_1efx577"
		mainServiceTaskElement = "ServiceTask_157wf2b"
		receiveMessageName     = "globalMsgRefMsgNIInst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and drives the main flow past the receive task", func(t *testing.T) {
		publishInstantiatingMessage(t, receiveMessageName, &map[string]any{"approved": true})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, mainServiceTaskElement)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		require.Equal(t, 0, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
			"the receive task and its boundary subscription must be consumed by the instantiating message")

		fetched, err := getProcessInstance(t, instanceKey)
		require.NoError(t, err)
		require.Equal(t, true, fetched.Variables["approved"], "instantiating message variables should be propagated to the instance")
		require.Nil(t, fetched.Variables["boundaryFired"], "boundary output must not be set; the receive task completed on instantiation")

		require.NoError(t, completeJob(t, job.Key, map[string]any{}))
		waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
	})
}

// assertInstantiatingTimerBoundaryConsumed verifies that, after the instantiating message has satisfied the
// receive task, no receive-task subscription and no created boundary timer remain, the expected message
// variables are present on the instance, and then completes the job and waits for the instance to finish.
func assertInstantiatingTimerBoundaryConsumed(t *testing.T, store storage.Storage, instanceKey int64, receiveTaskElement string, jobKey int64) {
	t.Helper()
	require.Equal(t, 0, activeReceiveTaskSubscriptions(t, store, instanceKey, receiveTaskElement),
		"the receive task subscription must be consumed by the instantiating message")
	created, err := store.FindProcessInstanceTimers(t.Context(), instanceKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	require.Empty(t, created, "the boundary timer must be cancelled once the instantiating message satisfies the receive task")

	fetched, err := getProcessInstance(t, instanceKey)
	require.NoError(t, err)
	require.Equal(t, true, fetched.Variables["approved"], "instantiating message variables should be propagated to the instance")
	require.Nil(t, fetched.Variables["timerFired"], "boundary timer output must not be set; the receive task completed on instantiation")

	require.NoError(t, completeJob(t, jobKey, map[string]any{}))
	waitForProcessInstanceState(t, instanceKey, zenclient.ProcessInstanceStateCompleted)
}

func TestInstantiatingReceiveTaskBoundaryWithInterruptingTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile           = "receive-task-boundary-timer-interrupting-instantiating.bpmn"
		bpmnProcessId      = "Process_0anusn1_instantiating"
		receiveTaskElement = "ReceiveTask_1efx577"
		serviceTaskElement = "ServiceTask_157wf2b"
		messageName        = "globalMsgRefTimerIInst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and drives the main flow past the receive task", func(t *testing.T) {
		publishInstantiatingMessage(t, messageName, &map[string]any{"approved": true})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		job := waitForProcessInstanceActiveJobByElementId(t, instanceKey, serviceTaskElement)
		require.Equal(t, serviceTaskElement, job.ElementId)

		assertInstantiatingTimerBoundaryConsumed(t, store, instanceKey, receiveTaskElement, job.Key)
	})
}

func TestInstantiatingReceiveTaskBoundaryWithNonInterruptingTimer(t *testing.T) {
	cleanProcessInstances(t)

	const (
		bpmnFile               = "receive-task-boundary-timer-noninterrupting-instantiating.bpmn"
		bpmnProcessId          = "Process_timer_ni_instantiating"
		receiveTaskElement     = "ReceiveTask_1efx577"
		mainServiceTaskElement = "ServiceTask_157wf2b"
		messageName            = "globalMsgRefTimerNIInst"
	)

	definition, err := deployGetDefinition(t, "receive_task/"+bpmnFile, bpmnProcessId)
	require.NoError(t, err)
	require.NotZero(t, definition.Key, "Definition key should not be zero")

	assertManualStartRejected(t, definition.Key)

	t.Run("instantiating message creates the instance and drives the main flow past the receive task", func(t *testing.T) {
		publishInstantiatingMessage(t, messageName, &map[string]any{"approved": true})
		instanceKey := waitForActiveInstanceKeyByBpmnProcessId(t, bpmnProcessId)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
		require.NoError(t, err)

		job := waitForProcessInstanceJobByElementId(t, instanceKey, mainServiceTaskElement, public.JobStateActive)
		require.Equal(t, mainServiceTaskElement, job.ElementId)

		assertInstantiatingTimerBoundaryConsumed(t, store, instanceKey, receiveTaskElement, job.Key)
	})
}
