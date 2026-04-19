package e2e

import (
	"testing"
	"time"

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
		assert.Equal(t, zenclient.ProcessInstanceStateCompleted, processInstances.JSON200.Partitions[0].Items[0].State)
	})

}

func TestTimerEventSubprocessNonInterruptingNested(t *testing.T) {
	// Process: Start -> SubProcess(Subprocess_15s23yn) -> End
	// SubProcess contains:
	//   - taskA (job type "subProcessJobType")
	//   - non-interrupting event subprocess with timer PT1S containing taskB (job type "eventSubProcessJobType")

	definition, err := deployGetDefinition(t, "timer-event-subprocess-non-interrupting-nested.bpmn", "Process_1c1lgem")
	assert.NoError(t, err)

	var instance zenclient.ProcessInstance

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
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

	t.Run("create process instance", func(t *testing.T) {
		instance, err = createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, instance.Key)
	})

	// Wait for timers to fire: EventSubprocessA PT1S, then EventSubprocessB PT1S, then intermediate catch PT2S
	time.Sleep(4 * time.Second)

	t.Run("verify EventSubprocessA_00bugpj is not completed because EventSubprocessB is waiting", func(t *testing.T) {
		fetchedInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.Equal(t, zenclient.ProcessInstanceStateActive, fetchedInstance.State)

		subProcessChild := getFirstChildInstance(t, instance.Key)
		eventSubprocessAChild := getFirstChildInstance(t, subProcessChild.Key)

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

		subProcessChild := getFirstChildInstance(t, instance.Key)
		eventSubprocessAChild := getFirstChildInstance(t, subProcessChild.Key)

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
	})
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
