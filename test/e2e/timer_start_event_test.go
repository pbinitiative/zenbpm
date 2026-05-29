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
		definition, err = deployGetDefinition(t, "process_definition_start_event/timer-start-event-process.bpmn", "timer-start-event-process-1")
		assert.NoError(t, err)

		// No process instance creation is needed. Timer start event should create the instance itself after definition deployment and timer activation duration
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
		}, 20*time.Second, 100*time.Millisecond, "timer start event should create one active process instance")
		// there should be only 1 token as the process should start only from timer start event and not from other start events
		fetchedProcessInstance, store := requireFirstActiveInstanceWithSingleToken(t, processInstances)

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

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			jobsPartitionPage, errJobs := readWaitingJobs(t, "input-task-for-timer-start-event-test")

			if errJobs != nil {
				return
			}

			require.NotEmpty(collect, jobsPartitionPage)
			require.NotEmpty(collect, jobsPartitionPage.Partitions[0].Items)

			jobToComplete = jobsPartitionPage.Partitions[0].Items[0]
		}, 15*time.Second, 100*time.Millisecond, "timer start event should have at least one job")

		assert.NotEmpty(t, jobToComplete.Key)
		assert.NotEmpty(t, jobToComplete.ProcessInstanceKey)
		assert.Equal(t, zenclient.JobStateActive, jobToComplete.State)
	})

	t.Run("complete job and verify instance is completed", func(t *testing.T) {

		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			errCompleteJob := completeJob(t, jobToComplete.Key, map[string]any{})
			assert.NoError(collect, errCompleteJob)

			processInstances, err = app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
				BpmnProcessId: &definition.BpmnProcessId,
			})
			assert.NoError(collect, err)
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, processInstances.JSON200.Partitions[0].Items[0].State)
		}, 15*time.Second, 100*time.Millisecond, "timer start event should have completed")
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

	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/process_definition_start_event/timer-start-event-process.bpmn")
	require.NoError(t, err)

	futureTimerDate := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	uniqueProcessId := fmt.Sprintf("timer-start-event-process-future-%d", time.Now().UnixNano())

	modifiedBpmn := []byte(strings.NewReplacer(
		"2026-03-28T10:00:00Z", futureTimerDate,
		"timer-start-event-process-1", uniqueProcessId,
	).Replace(string(bpmnData)))

	deployResp, err := deployDefinitionFromBytes(t, modifiedBpmn, "process_definition_start_event/timer-start-event-process.bpmn")
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

	definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn", "Process_1c1lgem")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstancesSimple

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// before the non-interrupting timer fires, it should be in TimerStateCreated
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, subProcessChild.Key, "eventSubprocessTimerEvent_010eof4")
	})

	// Wait for the non-interrupting event-subprocess timer (PT1S) to fire
	assertTimerTriggered(t, subProcessChild.Key, "eventSubprocessTimerEvent_010eof4")

	var subProcessJob zenclient.Job
	t.Run("find and complete subProcessJobType job", func(t *testing.T) {
		var jobsPartitionPage zenclient.JobPartitionPage
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			page, err := readWaitingJobs(t, "subProcessJobType")
			if !assert.NoError(collect, err) {
				return
			}
			if !assert.NotEmpty(collect, page.Partitions) {
				return
			}
			if !assert.NotEmpty(collect, page.Partitions[0].Items) {
				return
			}
			jobsPartitionPage = page
		}, 20*time.Second, 100*time.Millisecond, "subProcessJobType job should be Active")
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
	t.Run("find and complete eventSubProcessJobType job", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "eventSubProcessJobType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubProcessJob = jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubProcessJob.State)

		err = completeJob(t, eventSubProcessJob.Key, map[string]any{})
		assert.NoError(t, err)
	})

	t.Run("verify subprocess Subprocess_15s23yn is completed and process is completed", func(t *testing.T) {
		// Poll until completion propagates rather than relying on a fixed sleep.
		var fetchedInstance zenclient.ProcessInstance
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var err error
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State)
		}, 5*time.Second, 100*time.Millisecond, "process instance should reach Completed state after the event subprocess job is completed")

		// after process completion, the non-interrupting event subprocess timer should be in TimerStateTriggered
		assertTimerTriggered(t, subProcessChild.Key, "eventSubprocessTimerEvent_010eof4")

		// verify that the event subprocess output variable was propagated to the main process
		assert.Equal(t, "nested-event-subprocess-done", fetchedInstance.Variables["eventSubProcessVariable"],
			"eventSubProcessVariable should be propagated from the nested event subprocess through the subprocess to the root process")
		// The timer-start event's io-mapping writes timerStartEventVar onto Subprocess_15s23yn's
		// holder. Subprocess_15s23yn has an explicit io-mapping that only exposes
		// eventSubProcessVariable, so timerStartEventVar is not propagated further to the root.
		assert.Nil(t, fetchedInstance.Variables["timerStartEventVar"],
			"timerStartEventVar must not leak to the root process because Subprocess_15s23yn does not map it")
	})
}

func TestTimerEventSubprocessNonInterruptingNested2(t *testing.T) {
	// Process: Start -> SubProcess_0rohbe2 -> End
	// SubProcess_0rohbe2 contains:
	//   - service task1 (job type "EventSubprocessType")
	//   - EventSubprocessA_00bugpj (non-interrupting timer PT1S):
	//       - timer start -> Event_1u5vukv intermediate catch timer PT2S -> EndEventA_0evifiy
	//       - EventSubprocessB_16e6pei (non-interrupting timer PT1S):
	//           - timer start -> service task2 (job type "EventSubprocessBtype") -> EndEventB_1pttfqp

	definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-non-interrupting-nested-2.bpmn", "Process_0383xaq")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance
	var subProcessChild zenclient.ProcessInstancesSimple

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)

		// timer to start EventSubprocessA_00bugpj should be created
		subProcessChild = getFirstChildInstance(t, instance.Key)
		assertTimerCreated(t, subProcessChild.Key, "eventSubProcessATimerEvent_1i1fx2b")
	})

	// timer to start EventSubprocessB_16e6pei should be created, timer to start EventSubprocessA_00bugpj should be triggered.
	eventSubprocessAChild := getFirstChildInstance(t, subProcessChild.Key)
	assertTimerCreated(t, eventSubprocessAChild.Key, "eventSubProcessBTimerEvent_0a3aipv")
	assertTimerTriggered(t, subProcessChild.Key, "eventSubProcessATimerEvent_1i1fx2b")

	// Wait for EventSubprocessB's start-event timer (PT1S) to fire — that spawns the EventSubprocessBtype job.
	assertTimerTriggered(t, eventSubprocessAChild.Key, "eventSubProcessBTimerEvent_0a3aipv")

	// Wait for EventSubprocessA's intermediate catch timer (PT2S, id=Event_1u5vukv) to fire.
	assertTimerTriggered(t, eventSubprocessAChild.Key, "Event_1u5vukv")

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

		err = completeJob(t, eventSubprocessBJob.Key, map[string]any{})
		assert.NoError(t, err)

		// After completing EventSubprocessBtype, EventSubprocessA_00bugpj should complete
		// but the process should still be active because EventSubprocessType (service task1) is still waiting
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			fetchedEventSubprocessA, err := getProcessInstance(t, eventSubprocessAChild.Key)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, fetchedEventSubprocessA.State,
				"EventSubprocessA_00bugpj should be completed after EventSubprocessBtype job completion")
		}, 5*time.Second, 100*time.Millisecond)

		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)
	})

	t.Run("complete EventSubprocessType job and verify process is completed", func(t *testing.T) {
		jobsPartitionPage, err := readWaitingJobs(t, "EventSubprocessType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobsPartitionPage.Partitions)
		assert.NotEmpty(t, jobsPartitionPage.Partitions[0].Items)
		eventSubprocessJob := jobsPartitionPage.Partitions[0].Items[0]
		assert.Equal(t, zenclient.JobStateActive, eventSubprocessJob.State)

		err = completeJob(t, eventSubprocessJob.Key, map[string]any{})
		assert.NoError(t, err)

		var fetchedInstance zenclient.ProcessInstance
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var err error
			fetchedInstance, err = getProcessInstance(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, zenclient.ProcessInstanceStateCompleted, fetchedInstance.State)
		}, 5*time.Second, 100*time.Millisecond, "process instance should reach Completed state after the EventSubprocessType job is completed")

		// verify that event subprocess output variables were propagated to the root process
		assert.Equal(t, "event-subprocess-a-done", fetchedInstance.Variables["eventSubProcessAVariable"],
			"eventSubProcessAVariable should be propagated from EventSubprocessA through SubProcess to root")
		assert.Equal(t, "event-subprocess-b-done", fetchedInstance.Variables["eventSubProcessBVariable"],
			"eventSubProcessBVariable should be propagated from EventSubprocessB through EventSubprocessA and SubProcess to root")
		// SubProcess_0rohbe2's io-mapping only exposes eventSubProcessAVariable and
		// eventSubProcessBVariable; under the new activity propagation rules, only mapped
		// variables reach the parent scope, so neither timerStartEventVarA nor
		// timerStartEventVarB make it to the root process.
		assert.Nil(t, fetchedInstance.Variables["timerStartEventVarA"],
			"timerStartEventVarA must not leak to the root process because SubProcess_0rohbe2 does not map it")
		assert.Nil(t, fetchedInstance.Variables["timerStartEventVarB"],
			"timerStartEventVarB must not leak to the root process because SubProcess_0rohbe2 does not map it")
	})
}

// assertStartEventExecuted verifies that expectedElementId is presented among the flow element
// instances for the given process instance, and that unexpectedElementId is absent.
func assertStartEventExecuted(t testing.TB, store storage.Storage, processInstanceKey int64, expectedElementId, unexpectedElementId string) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), processInstanceKey, false)
		if !assert.NoError(collect, err) {
			return
		}

		var found, notFound bool
		for _, fe := range flowElements {
			if fe.ElementId == expectedElementId {
				found = true
			}
			if fe.ElementId == unexpectedElementId {
				notFound = true
			}
		}

		assert.True(collect, found, "expected start event %q to have a flow element instance for process instance %d", expectedElementId, processInstanceKey)
		assert.False(collect, notFound, "unexpected start event %q should not have a flow element instance for process instance %d", unexpectedElementId, processInstanceKey)
	}, 5*time.Second, 100*time.Millisecond, "timer start event should create one active process instance")
}

func getFirstChildInstance(t testing.TB, parentKey int64) zenclient.ProcessInstancesSimple {
	t.Helper()

	processInstanceListItem := zenclient.ProcessInstancesSimple{}

	require.Eventually(t, func() bool {
		children, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), parentKey, &zenclient.GetChildProcessInstancesParams{})

		if err != nil {
			return false
		}

		if children.JSON200 == nil || len(children.JSON200.Partitions) == 0 {
			return false
		}

		if len(children.JSON200.Partitions[0].Items) != 1 {
			return false
		}

		processInstanceListItem = children.JSON200.Partitions[0].Items[0]
		return true
	}, 5*time.Second, 100*time.Millisecond, "The child process instance list should contain exactly one child process instance.")
	return processInstanceListItem
}

// TestTimerCycleStartEvent_InstanceAndDurationSubprocess is the timer-cycle analogue of
// TestMessageStartEvent_SameMessageForInstanceAndSubprocess (see message_start_event_test.go).
//
// The deployed BPMN has:
//   - a definition-level timer-start event with timeCycle = R2/PT1S — so the engine itself
//     creates exactly 2 process instances, ~1s apart, without any external trigger;
//   - an interrupting event subprocess whose timer-start event uses timeDuration = PT1S —
//     so for every spawned parent instance the event subprocess fires ~1s after creation,
//     interrupts the parent's service-task job, and completes the parent.
//
// The test asserts the cycle produced exactly 2 instances and then runs the full
// interruption verification per instance (job Terminated, child event subprocess Completed, parent variables propagated, root timer Triggered).
func TestTimerCycleStartEvent_InstanceAndDurationSubprocess(t *testing.T) {
	bpmnData, err := os.ReadFile("../../pkg/bpmn/test-cases/process_definition_start_event/timer-cycle-start-event-instance-and-duration-subprocess.bpmn")
	require.NoError(t, err)

	// Make the deployment unique so this test does not interfere with other e2e tests that may
	// share the same BPMN id or with multiple invocations of this test.
	uniqueProcessId := fmt.Sprintf("process-timerCycleInstanceAndDurationSubprocess-%d", time.Now().UnixNano())
	modifiedBpmn := []byte(strings.NewReplacer(
		"Process_timerEventSubProcessInterrupting2_cycle", uniqueProcessId,
	).Replace(string(bpmnData)))

	deployResp, err := deployDefinitionFromBytes(t, modifiedBpmn, "process_definition_start_event/timer-cycle-start-event-instance-and-duration-subprocess.bpmn")
	require.NoError(t, err)

	var definitionKey int64
	if deployResp.JSON201 != nil {
		definitionKey = deployResp.JSON201.ProcessDefinitionKey
	} else {
		require.NotNil(t, deployResp.JSON200, "expected either 200 or 201 response from deploy")
		definitionKey = deployResp.JSON200.ProcessDefinitionKey
	}

	// Wait until the R2/PT1S cycle has produced exactly 2 process instances.
	var parentInstances []zenclient.ProcessInstancesSimple
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		page, errGet := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &uniqueProcessId,
		})
		if !assert.NoError(collect, errGet) {
			return
		}
		if !assert.NotNil(collect, page) || !assert.NotNil(collect, page.JSON200) {
			return
		}
		if !assert.Equal(collect, 2, page.JSON200.TotalCount,
			"definition-level R2/PT1S cycle must create exactly 2 process instances (got %d)", page.JSON200.TotalCount) {
			return
		}
		parentInstances = parentInstances[:0]
		for _, p := range page.JSON200.Partitions {
			parentInstances = append(parentInstances, p.Items...)
		}
		assert.Equal(collect, 2, len(parentInstances),
			"flattened instance count must match TotalCount=2, got %d", len(parentInstances))
	}, 20*time.Second, 100*time.Millisecond, "definition-level R2/PT1S cycle must create exactly 2 process instances")

	// Per-instance verification: the event subprocess timer (PT1S) must fire, interrupt the
	// service-task job, complete the event subprocess child and the parent process, and
	// propagate the io-mapping variables up.
	for _, parent := range parentInstances {
		parent := parent // capture
		t.Run(fmt.Sprintf("instance %d", parent.Key), func(t *testing.T) {
			// Parent eventually completes once the PT1S event-subprocess timer fires and interrupts.
			waitForProcessInstanceState(t, parent.Key, zenclient.ProcessInstanceStateCompleted)

			// The main service-task job must have been Terminated by the interrupting event subprocess.
			assertSingleJobState(t, parent.Key, zenclient.JobStateTerminated)

			// Event subprocess timer-start must end up Triggered on this parent instance.
			assertTimerTriggered(t, parent.Key, "subProcessTimerEvent_12i3m6f")

			// The event subprocess child instance must exist and be Completed.
			_ = requireChildEventSubProcessCompleted(t, parent.Key)

			// Variables produced by the event subprocess io-mappings must be propagated to the parent.
			fetched, err := getProcessInstance(t, parent.Key)
			require.NoError(t, err)
			assert.Equal(t, "timer-fired", fetched.Variables["subProcessResult"],
				"subProcessResult must be propagated from the interrupting timer event subprocess")
			assert.Equal(t, true, fetched.Variables["timerInterrupted"],
				"timerInterrupted must be propagated from the interrupting timer event subprocess")
			assert.Equal(t, "timer-event-subprocess", fetched.Variables["interruptedBy"],
				"interruptedBy must be propagated from the interrupting timer event subprocess")
			assert.Equal(t, "timerStartEventOutputValue", fetched.Variables["timerStartEventOutputVar"],
				"timerStartEventOutputVar must be propagated from the timer-start event io-mapping")
		})
	}

	// Definition-level R2 cycle must be fully exhausted: 2 Triggered timers and no remaining
	// Created timer for the definition-level start event. The definition key is not partition-encoded,
	// so route the lookup through one of the parent instances' partition store.
	require.NotEmpty(t, parentInstances, "expected at least one parent instance to look up the partition store")
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(parentInstances[0].Key))
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggered, errFind := store.FindProcessDefinitionTimers(t.Context(), definitionKey, bpmnruntime.TimerStateTriggered)
		if !assert.NoError(collect, errFind) {
			return
		}
		defLevelTriggered := filterDefinitionLevelTimers(triggered)
		assert.Equal(collect, 2, len(defLevelTriggered),
			"R2 cycle should produce exactly 2 triggered definition-level timers, got %d (all triggered: %d)", len(defLevelTriggered), len(triggered))
	}, 15*time.Second, 100*time.Millisecond,
		"definition-level cycle should produce exactly 2 triggered timers")

	created, err := store.FindProcessDefinitionTimers(t.Context(), definitionKey, bpmnruntime.TimerStateCreated)
	require.NoError(t, err)
	defLevelCreated := filterDefinitionLevelTimers(created)
	assert.Empty(t, defLevelCreated,
		"no further Created definition-level timer should remain after the R2 cycle is exhausted, got: %+v", defLevelCreated)
}

// filterDefinitionLevelTimers returns only timers that are scoped to a process definition (not to a specific process instance).
// Definition-level timer-start events have ProcessInstanceKey == nil, whereas event-subprocess / boundary timers carry the parent instance's key.
func filterDefinitionLevelTimers(timers []bpmnruntime.Timer) []bpmnruntime.Timer {
	filtered := make([]bpmnruntime.Timer, 0, len(timers))
	for _, t := range timers {
		if t.ProcessInstanceKey == nil {
			filtered = append(filtered, t)
		}
	}
	return filtered
}
