package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimerStartEvent(t *testing.T) {
	// process definition:
	// (Timer start event with DateTime in past) -> (service task with job type "input-task-for-timer-start-event-test") -> (end event)
	//                                               ^
	//                                               |
	//                       (Plain start event) ----+
	var definition zenclient.ProcessDefinitionSimple
	var processInstances *zenclient.GetProcessInstancesResponse
	var err error

	t.Run("create timer-start-event-process.bpmn process definition", func(t *testing.T) {
		definition, err = deployGetDefinition(t, "timer-start-event-process.bpmn", "timer-start-event-process-1")
		assert.NoError(t, err)

		// No process instance creation is needed. Timer start event should create the instance itself after definition deployment and timer activation duration
		time.Sleep(2 * time.Second)

		// verify that process instance exists and is in Active state
		processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, processInstances.JSON200.TotalCount)
		fetchedProcessInstance := processInstances.JSON200.Partitions[0].Items[0]
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedProcessInstance.State)

		store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(fetchedProcessInstance.Key))
		require.NoError(t, err)
		// there should be only 1 token as the process should start only from timer start event and not from other start events
		tokens, err := store.GetAllTokensForProcessInstance(t.Context(), fetchedProcessInstance.Key)
		require.NoError(t, err)
		assert.Equal(t, 1, len(tokens))

		// verify that only the timer start event element was executed, not the plain start event
		assertStartEventExecuted(t, store, fetchedProcessInstance.Key, "timerStartEvent_1234", "plainStartEvent_0mtr7my")

		// the start timer was in the past, so it should already be triggered
		triggeredTimers, err := store.FindProcessDefinitionTimers(t.Context(), definition.Key, bpmnruntime.TimerStateTriggered)
		require.NoError(t, err)
		assert.Equal(t, 1, len(triggeredTimers), "timer should be in TimerStateTriggered for process definition %d", definition.Key)
		createdTimers, err := store.FindProcessInstanceTimers(t.Context(), definition.Key, bpmnruntime.TimerStateCreated)
		require.NoError(t, err)
		assert.Empty(t, createdTimers, "no timers should remain in TimerStateCreated for process definition %d", definition.Key)
	})

	// find the input-task-for-timer-start-event-test job and verify it is Active
	var jobToComplete zenclient.Job
	t.Run("read waiting jobs", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "input-task-for-timer-start-event-test")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, zenclient.JobStateActive, jobToComplete.State)
	})

	t.Run("complete job and verify instance is completed", func(t *testing.T) {
		err := completeJob(t, jobToComplete, map[string]any{})
		assert.NoError(t, err)

		processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &definition.BpmnProcessId,
		})
		assert.NoError(t, err)
		completedInstance := processInstances.JSON200.Partitions[0].Items[0]
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, completedInstance.State)
	})

}

// TestPlainStartEvent_WithFutureTimerStartEvent verifies that when the timer start event has a
// date in the future, starting a process instance manually triggers only the plain start event branch
func TestPlainStartEvent_WithFutureTimerStartEvent(t *testing.T) {
	// Process: (Timer start event with DateTime in FUTURE) -> (service task) -> (end event)
	//                                                          ^
	//                                                          |
	//          (Plain start event) -------------------------+
	//
	// Because the timer date is in the future, only the plain start event fires
	// when the process instance is created manually. There should be exactly 1 active token.

	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/timer-start-event-process.bpmn")
	require.NoError(t, err)

	futureTimerDate := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	uniqueProcessId := fmt.Sprintf("timer-start-event-process-future-%d", time.Now().UnixNano())

	modifiedBpmn := []byte(strings.NewReplacer(
		"2026-03-28T10:00:00Z", futureTimerDate,
		"timer-start-event-process-1", uniqueProcessId,
	).Replace(string(bpmnData)))

	deployResp, err := deployDefinitionFromBytes(t, modifiedBpmn, "timer-start-event-process.bpmn")
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
	// Only the plain start event branch should be executing — the timer start event is in the future.
	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), instance.Key)
	require.NoError(t, err)
	assert.Equal(t, 1, len(tokens), "only the plain start event branch should execute since the timer date is in the future")

	// verify that only the plain start event element was executed, not the timer start event
	assertStartEventExecuted(t, store, instance.Key, "plainStartEvent_0mtr7my", "timerStartEvent_1234")
}

func TestTimerEventSubprocessNonInterruptingNested(t *testing.T) {
	// Process: Start -> SubProcess(Subprocess_15s23yn) -> End
	// SubProcess contains:
	//   - taskA (job type "subProcessJobType")
	//   - non-interrupting event subprocess with timer PT1S containing taskB (job type "eventSubProcessJobType")

	definition, err := deployGetDefinition(t, "timer-event-subprocess-non-interrupting-nested.bpmn", "Process_1c1lgem")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstance

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// before the non-interrupting timer fires, it should be in TimerStateCreated
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, subProcessChild.Key, "eventSubprocessTimerEvent_010eof4")
	})

	// Wait for the non-interrupting timer (PT1S) to fire
	time.Sleep(2 * time.Second)

	var subProcessJob zenclient.Job
	t.Run("find and complete subProcessJobType job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "subProcessJobType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		subProcessJob = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, subProcessJob.State)

		err = completeJob(t, subProcessJob, map[string]any{})
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
	t.Run("find and complete eventSubProcessJobType job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "eventSubProcessJobType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubProcessJob = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubProcessJob.State)

		err = completeJob(t, eventSubProcessJob, map[string]any{})
		assert.NoError(t, err)
	})

	t.Run("verify subprocess Subprocess_15s23yn is completed and process is completed", func(t *testing.T) {
		// Give a moment for completion to propagate
		time.Sleep(100 * time.Millisecond)

		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State)

		// after process completion, the non-interrupting event subprocess timer should be in TimerStateTriggered
		assertTimerTriggered(t, subProcessChild.Key, "eventSubprocessTimerEvent_010eof4")

		// verify that the event subprocess output variable was propagated to the main process
		assert.Equal(t, "nested-event-subprocess-done", fetchedInstance.Variables["eventSubProcessVariable"],
			"eventSubProcessVariable should be propagated from the nested event subprocess through the subprocess to the root process")
	})
}

func TestTimerEventSubprocessNonInterruptingNested2(t *testing.T) {
	// Process: Start -> SubProcess_0rohbe2 -> End
	// SubProcess_0rohbe2 contains:
	//   - service task1 (job type "EventSubprocessType")
	//   - EventSubprocessA_00bugpj (non-interrupting timer PT1S):
	//       - timer start -> intermediate catch timer PT2S -> EndEventA_0evifiy
	//       - EventSubprocessB_16e6pei (non-interrupting timer PT1S):
	//           - timer start -> service task2 (job type "EventSubprocessBtype") -> EndEventB_1pttfqp

	definition, err := deployGetDefinition(t, "timer-event-subprocess-non-interrupting-nested-2.bpmn", "Process_0383xaq")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstance

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// timer to start EventSubprocessA_00bugpj should be created
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, subProcessChild.Key, "eventSubProcessATimerEvent_1i1fx2b")
	})

	// timer to start EventSubprocessB_16e6pei should be created, timer to start EventSubprocessA_00bugpj should be triggered
	time.Sleep(1500 * time.Millisecond)
	eventSubprocessAChild := getFirstChildInstance(t, subProcessChild.Key)
	assertTimerCreated(t, eventSubprocessAChild.Key, "eventSubProcessBTimerEvent_0a3aipv")
	assertTimerTriggered(t, subProcessChild.Key, "eventSubProcessATimerEvent_1i1fx2b")

	// Wait for timers to fire: EventSubprocessA PT1S, then EventSubprocessB PT1S, then intermediate catch PT2S
	time.Sleep(2500 * time.Millisecond)
	assertTimerTriggered(t, eventSubprocessAChild.Key, "eventSubProcessBTimerEvent_0a3aipv")

	t.Run("verify EventSubprocessA_00bugpj is not completed because EventSubprocessB is waiting", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		fetchedEventSubprocessA, err := getProcessInstance(t, eventSubprocessAChild.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedEventSubprocessA.State)
	})

	t.Run("complete EventSubprocessBtype job and verify EventSubprocessA is completed", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "EventSubprocessBtype")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubprocessBJob := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubprocessBJob.State)

		err = completeJob(t, eventSubprocessBJob, map[string]any{})
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// After completing EventSubprocessBtype, EventSubprocessA_00bugpj should complete
		// but the process should still be active because EventSubprocessType (service task1) is still waiting
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		fetchedEventSubprocessA, err := getProcessInstance(t, eventSubprocessAChild.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedEventSubprocessA.State, "EventSubprocessA_00bugpj should be completed after EventSubprocessBtype job completion")
	})

	t.Run("complete EventSubprocessType job and verify process is completed", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "EventSubprocessType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubprocessJob := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubprocessJob.State)

		err = completeJob(t, eventSubprocessJob, map[string]any{})
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State)

		// verify that event subprocess output variables were propagated to the root process
		assert.Equal(t, "event-subprocess-a-done", fetchedInstance.Variables["eventSubProcessAVariable"],
			"eventSubProcessAVariable should be propagated from EventSubprocessA through SubProcess to root")
		assert.Equal(t, "event-subprocess-b-done", fetchedInstance.Variables["eventSubProcessBVariable"],
			"eventSubProcessBVariable should be propagated from EventSubprocessB through EventSubprocessA and SubProcess to root")
	})
}

// assertStartEventExecuted verifies that expectedElementId is present among the flow element
// instances for the given process instance, and that unexpectedElementId is absent.
func assertStartEventExecuted(t testing.TB, store storage.Storage, processInstanceKey int64, expectedElementId, unexpectedElementId string) {
	t.Helper()
	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
	require.NoError(t, err)

	var found, notFound bool
	for _, fe := range flowElements {
		if fe.ElementId == expectedElementId {
			found = true
		}
		if fe.ElementId == unexpectedElementId {
			notFound = true
		}
	}
	assert.True(t, found, "expected start event %q to have a flow element instance for process instance %d", expectedElementId, processInstanceKey)
	assert.False(t, notFound, "unexpected start event %q should not have a flow element instance for process instance %d", unexpectedElementId, processInstanceKey)
}

// getFirstChildInstance retrieves the first child process instance of the given parent and asserts exactly one exists.
func getFirstChildInstance(t testing.TB, parentKey int64) zenclient.ProcessInstance {
	t.Helper()
	children, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), parentKey, &zenclient.GetChildProcessInstancesParams{})
	require.NoError(t, err)
	require.NotEmpty(t, children.JSON200.Partitions)
	require.NotEmpty(t, children.JSON200.Partitions[0].Items)
	require.Equal(t, 1, len(children.JSON200.Partitions[0].Items))
	return children.JSON200.Partitions[0].Items[0]
}
