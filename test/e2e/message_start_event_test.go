package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessageStartEvent mirrors TestTimerStartEvent for the message-start-event-process.bpmn
// fixture: deploying the definition registers a definition-level message subscription, and
// publishing the corresponding message creates a new active process instance from the
// message start event branch only (the plain start event must NOT fire). After completing
// the active service-task job the process instance reaches the Completed state.
func TestMessageStartEvent(t *testing.T) {
	// process definition:
	// (Message start event "messageStartEventProcessRef") -> (service task with job type "input-task-for-message-start-event-test") -> (end event)
	//                                                          ^
	//                                                          |
	//                                  (Plain start event) ----+
	var definition zenclient.ProcessDefinitionSimple
	var processInstances *zenclient.GetProcessInstancesResponse
	var err error

	t.Run("create message-start-event-process.bpmn process definition", func(t *testing.T) {
		definition, err = deployGetDefinition(t, "process_definition_start_event/message-start-event-process.bpmn", "message-start-event-process-1")
		assert.NoError(t, err)

		// No process instance creation is needed yet. The definition-level message
		// subscription is registered on deployment; publishing the message creates the instance.
		// CorrelationKey must be nil so the engine routes the publish to the
		// DefinitionMessageSubscription (top-level message start event).
		publishResp, err := app.restClient.PublishMessageWithResponse(t.Context(), zenclient.PublishMessageJSONRequestBody{
			CorrelationKey: nil,
			MessageName:    "messageStartEventProcessRef",
			Variables: &map[string]any{
				"messagePayloadVar": "messagePayloadValue",
			},
		})
		require.NoError(t, err)
		require.Equal(t, 201, publishResp.StatusCode(), "publishing the message must succeed")

		require.Eventually(t, func() bool {
			processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
				BpmnProcessId: &definition.BpmnProcessId,
			})
			if err != nil || processInstances == nil || processInstances.JSON200 == nil {
				return false
			}
			if processInstances.JSON200.TotalCount != 1 {
				return false
			}
			return processInstances.JSON200.Partitions[0].Items[0].State == zenclient.ProcessInstanceStateActive
		}, 20*time.Second, 100*time.Millisecond, "publishing the message should create one active process instance")
		// there should be only 1 token as the process should start only from the message
		// start event branch and not from the plain start event branch
		fetchedProcessInstance, store := requireFirstActiveInstanceWithSingleToken(t, processInstances)

		// verify that only the message start event element was executed, not the plain start event
		assertStartEventExecuted(t, store, fetchedProcessInstance.Key, "messageStartEvent_1234", "plainStartEvent_0mtr7my")

		assert.Equal(t, "messagePayloadValue", fetchedProcessInstance.Variables["messagePayloadVar"],
			"variables published with the message must be propagated to the new process instance")

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			sub, errSub := store.FindMessageSubscriptionByName(t.Context(), "messageStartEventProcessRef", nil, bpmnruntime.ActivityStateCompleted)
			if !assert.NoError(collect, errSub) {
				return
			}
			if !assert.NotNil(collect, sub, "expected to find a Completed message subscription for the message start event") {
				return
			}
			_, isDef := sub.(*bpmnruntime.DefinitionMessageSubscription)
			assert.True(collect, isDef, "completed subscription must be a DefinitionMessageSubscription")
			assert.Equal(collect, definition.Key, sub.MessageSubscription().ProcessDefinitionKey)
		}, 5*time.Second, 100*time.Millisecond, "definition-level message subscription should be Completed after publishing the message")
	})

	// find the input-task-for-message-start-event-test job and verify it is Active
	var jobToComplete zenclient.Job
	t.Run("read waiting jobs", func(t *testing.T) {

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			jobsPartitionPage, errJobs := readWaitingJobs(t, "input-task-for-message-start-event-test")

			if errJobs != nil {
				return
			}

			require.NotEmpty(collect, jobsPartitionPage)
			require.NotEmpty(collect, jobsPartitionPage.Partitions[0].Items)

			jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		}, 15*time.Second, 100*time.Millisecond, "message start event should have at least one job")

		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, zenclient.JobStateActive, jobToComplete.State)
	})

	t.Run("complete job and verify instance is completed", func(t *testing.T) {

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			errCompleteJob := completeJob(t, jobToComplete.Key, map[string]any{})
			assert.NoError(t, errCompleteJob)

			processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
				BpmnProcessId: &definition.BpmnProcessId,
			})
			assert.NoError(t, err)
			assert.Equal(t, zenclient.ProcessInstanceStateCompleted, processInstances.JSON200.Partitions[0].Items[0].State)
		}, 15*time.Second, 100*time.Millisecond, "message start event instance should have completed")
	})
}

// TestPlainStartEvent_WithMessageStartEvent mirrors TestPlainStartEvent_WithFutureTimerStartEvent
// for the message-start-event variant: when the message has NOT been published, manually
// creating a process instance only triggers the plain start event branch (single active token).
func TestPlainStartEvent_WithMessageStartEvent(t *testing.T) {
	// Process: (Message start event) -> (service task) -> (end event)
	//                                     ^
	//                                     |
	//          (Plain start event) -------+
	//
	// Because the message has not been published, only the plain start event fires
	// when the process instance is created manually. There should be exactly 1 active token.

	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	uniqueProcessId := fmt.Sprintf("message-start-event-process-noPublish-%d", time.Now().UnixNano())
	uniqueMessageName := fmt.Sprintf("messageStartEventProcessRef-noPublish-%d", time.Now().UnixNano())

	modifiedBpmn := []byte(strings.NewReplacer(
		"message-start-event-process-1", uniqueProcessId,
		"messageStartEventProcessRef", uniqueMessageName,
	).Replace(string(bpmnData)))

	deployResp, err := deployDefinitionFromBytes(t, modifiedBpmn, "process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)

	var definitionKey int64
	if deployResp.JSON201 != nil {
		definitionKey = deployResp.JSON201.ProcessDefinitionKey
	} else {
		require.NotNil(t, deployResp.JSON200, "expected either 200 or 201 response from deploy")
		definitionKey = deployResp.JSON200.ProcessDefinitionKey
	}

	instance, err := createProcessInstance(t, &definitionKey, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assert.Equal(t, zenclient.ProcessInstanceStateActive, instance.State)

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instance.Key))
	require.NoError(t, err)
	// Only the plain start event branch should be executing — the message has not been published.
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), instance.Key)
	require.NoError(t, err)
	assert.Equal(t, 1, len(tokens), "only the plain start event branch should execute since the message has not been published")

	// verify that only the plain start event element was executed, not the message start event
	assertStartEventExecuted(t, store, instance.Key, "plainStartEvent_0mtr7my", "messageStartEvent_1234")
}

// TestMessageEventSubprocessNonInterruptingNested mirrors TestTimerEventSubprocessNonInterruptingNested
// but uses a non-interrupting message start event instead of a timer start event
// to trigger the nested event subprocess.
func TestMessageEventSubprocessNonInterruptingNested(t *testing.T) {
	// Process: Start -> SubProcess(Subprocess_15s23yn) -> End
	// SubProcess contains:
	//   - taskA (job type "msgSubProcessJobType")
	//   - non-interrupting event subprocess with message start event
	//     ("messageNestedNonIntRef", correlation key "correlation-key-msg-nested-1")
	//     containing taskB (job type "msgEventSubProcessJobType")

	definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-non-interrupting-nested.bpmn", "Process_msgNested1")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstancesSimple

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// before the message is published, the event subprocess message subscription
		// must be Active on the SubProcess child instance.
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertMessageSubscriptionActive(t, subProcessChild.Key, "eventSubprocessMessageEvent_010eof4")
	})

	t.Run("publish message to start the non-interrupting event subprocess", func(t *testing.T) {
		err = publishMessage(t, "messageNestedNonIntRef", "correlation-key-msg-nested-1", &map[string]any{})
		assert.NoError(t, err)
	})

	var subProcessJob zenclient.Job
	t.Run("find and complete msgSubProcessJobType job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "msgSubProcessJobType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		subProcessJob = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, subProcessJob.State)

		err = completeJob(t, subProcessJob.Key, map[string]any{})
		assert.NoError(t, err)
	})

	t.Run("verify subprocess Subprocess_15s23yn is not completed and process is not completed", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		// Process should still be active because the non-interrupting event subprocess is still running
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		// The subprocess Subprocess_15s23yn should still have active element instances (event subprocess)
		hasActiveSubprocess := false
		for _, ei := range fetchedInstance.ActiveElementInstances {
			if ei.ElementId == "Subprocess_15s23yn" || ei.ElementId == "ServiceTaskB_00m7f5d" || ei.ElementId == "EventSubprocess_00v1rg6" {
				hasActiveSubprocess = true
				break
			}
		}
		assert.True(t, hasActiveSubprocess, "subprocess Subprocess_15s23yn should not be completed yet")
	})

	var eventSubProcessJob zenclient.Job
	t.Run("find and complete msgEventSubProcessJobType job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "msgEventSubProcessJobType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubProcessJob = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubProcessJob.State)

		err = completeJob(t, eventSubProcessJob.Key, map[string]any{})
		assert.NoError(t, err)
	})

	t.Run("verify subprocess Subprocess_15s23yn is completed and process is completed", func(t *testing.T) {
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)

		// after process completion, the non-interrupting event subprocess message subscription
		// should be Completed
		assertMessageSubscriptionCompleted(t, subProcessChild.Key, "eventSubprocessMessageEvent_010eof4")

		fetchedInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)
		assert.Equal(t, "nested-event-subprocess-done", fetchedInstance.Variables["eventSubProcessVariable"],
			"eventSubProcessVariable should be propagated from the nested event subprocess through the subprocess to the root process")
		// The message-start event's io-mapping writes messageStartEventVar onto Subprocess_15s23yn's
		// holder. Subprocess_15s23yn has an explicit io-mapping that only exposes
		// eventSubProcessVariable, so messageStartEventVar is not propagated further to the root.
		assert.Nil(t, fetchedInstance.Variables["messageStartEventVar"],
			"messageStartEventVar must not leak to the root process because Subprocess_15s23yn does not map it")
	})
}

// TestMessageEventSubprocessNonInterruptingNested2 mirrors TestTimerEventSubprocessNonInterruptingNested2
// but uses non-interrupting message start events instead of timer start events
// for both nested event subprocesses.
func TestMessageEventSubprocessNonInterruptingNested2(t *testing.T) {
	// Process: Start -> SubProcess_0rohbe2 -> End
	// SubProcess_0rohbe2 contains:
	//   - service task1 (job type "msgEventSubprocessType")
	//   - EventSubprocessA_00bugpj (non-interrupting message start "messageNestedARef" / "correlation-key-msg-nested-2-A"):
	//       - message start -> intermediate catch timer PT2S -> EndEventA_0evifiy
	//       - EventSubprocessB_16e6pei (non-interrupting message start "messageNestedBRef" / "correlation-key-msg-nested-2-B"):
	//           - message start -> service task2 (job type "msgEventSubprocessBtype") -> EndEventB_1pttfqp

	definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-non-interrupting-nested-2.bpmn", "Process_msgNested2")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstancesSimple

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// before any message is published, the EventSubprocessA message subscription
		// must be Active on the SubProcess child instance.
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertMessageSubscriptionActive(t, subProcessChild.Key, "eventSubProcessAMessageEvent_1i1fx2b")
	})

	t.Run("publish messageNestedARef to start EventSubprocessA", func(t *testing.T) {
		err = publishMessage(t, "messageNestedARef", "correlation-key-msg-nested-2-A", &map[string]any{})
		assert.NoError(t, err)
	})

	// after publishing, EventSubprocessA child should exist with EventSubprocessB
	// message subscription Active. The EventSubprocessA-level subscription on the
	// parent SubProcess child becomes Completed after consuming the message.
	eventSubprocessAChild := getFirstChildInstance(t, subProcessChild.Key)
	assertMessageSubscriptionActive(t, eventSubprocessAChild.Key, "eventSubProcessBMessageEvent_0a3aipv")
	assertMessageSubscriptionCompleted(t, subProcessChild.Key, "eventSubProcessAMessageEvent_1i1fx2b")

	t.Run("publish messageNestedBRef to start EventSubprocessB", func(t *testing.T) {
		err = publishMessage(t, "messageNestedBRef", "correlation-key-msg-nested-2-B", &map[string]any{})
		assert.NoError(t, err)
	})

	// after publishing the B message, the EventSubprocessB-level subscription
	// in EventSubprocessA child should have become Completed
	assertMessageSubscriptionCompleted(t, eventSubprocessAChild.Key, "eventSubProcessBMessageEvent_0a3aipv")

	// Wait for the intermediate catch timer PT2S inside EventSubprocessA to fire
	time.Sleep(2500 * time.Millisecond)

	t.Run("verify EventSubprocessA_00bugpj is not completed because EventSubprocessB is waiting", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		fetchedEventSubprocessA, err := getProcessInstance(t, eventSubprocessAChild.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedEventSubprocessA.State)
	})

	t.Run("complete msgEventSubprocessBtype job and verify EventSubprocessA is completed", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "msgEventSubprocessBtype")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubprocessBJob := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubprocessBJob.State)

		err = completeJob(t, eventSubprocessBJob.Key, map[string]any{})
		assert.NoError(t, err)

		// After completing msgEventSubprocessBtype, EventSubprocessA_00bugpj should complete
		// but the process should still be active because msgEventSubprocessType (service task1) is still waiting
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)

		fetchedEventSubprocessA, err := getProcessInstance(t, eventSubprocessAChild.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedEventSubprocessA.State, "EventSubprocessA_00bugpj should be completed after msgEventSubprocessBtype job completion")
	})

	t.Run("complete msgEventSubprocessType job and verify process is completed", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "msgEventSubprocessType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubprocessJob := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubprocessJob.State)

		err = completeJob(t, eventSubprocessJob.Key, map[string]any{})
		assert.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		require.NoError(t, err)

		assert.Equal(t, "event-subprocess-a-done", fetchedInstance.Variables["eventSubProcessAVariable"],
			"eventSubProcessAVariable should be propagated from EventSubprocessA through SubProcess to root")
		assert.Equal(t, "event-subprocess-b-done", fetchedInstance.Variables["eventSubProcessBVariable"],
			"eventSubProcessBVariable should be propagated from EventSubprocessB through EventSubprocessA and SubProcess to root")
		// SubProcess_0rohbe2's io-mapping only exposes eventSubProcessAVariable and
		// eventSubProcessBVariable; under the new activity propagation rules, only mapped
		// variables reach the parent scope, so neither messageStartEventVarA nor
		// messageStartEventVarB make it to the root process.
		assert.Nil(t, fetchedInstance.Variables["messageStartEventVarA"],
			"messageStartEventVarA must not leak to the root process because SubProcess_0rohbe2 does not map it")
		assert.Nil(t, fetchedInstance.Variables["messageStartEventVarB"],
			"messageStartEventVarB must not leak to the root process because SubProcess_0rohbe2 does not map it")
	})
}
