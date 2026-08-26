package bpmn

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngineUsesDefaultMaxExecutionDepth(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.contextCancel)

	assert.Equal(t, DefaultMaxExecutionDepth, engine.maxExecutionDepth)
}

func TestRecursiveCallActivityStopsAtMaxExecutionDepthAndCreatesIncident(t *testing.T) {
	const maxDepth = int64(3)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/call-activity/call-activity-recursive.bpmn")
	require.NoError(t, err)

	rootInstance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rootInstance.ProcessInstance().ExecutionDepth)

	// wait until the engine detects the loop and raises an incident
	incident := waitForExecutionDepthIncident(t, store, 5*time.Second)
	assert.Equal(t, "recursiveCallActivity", incident.ElementId)
	assertExecutionDepthIncident(t, store, incident, maxDepth)

	// the chain must stop at depth == maxDepth: root (0) plus maxDepth children
	instances := store.ProcessInstancesSnapshot()
	assert.Len(t, instances, int(maxDepth)+1)
	seenDepths := make(map[int64]int, len(instances))
	for _, pi := range instances {
		seenDepths[pi.ProcessInstance().ExecutionDepth]++
	}
	for depth := int64(0); depth <= maxDepth; depth++ {
		assert.Equal(t, 1, seenDepths[depth], "expected exactly one process instance at depth %d", depth)
	}
}

func TestChildProcessInstancesTrackExecutionDepth(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity/call-activity-simple.bpmn")
	require.NoError(t, err)
	handler := func(job ActivatedJob) {
		job.Complete()
	}
	taskHandler := bpmnEngine.NewTaskHandler().Id("id").Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)

	childInstance := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
	assert.Equal(t, int64(1), childInstance.ProcessInstance().ExecutionDepth)
}

// TestNestedPlainSubProcessesTrackExecutionDepth verifies that plain (embedded) subprocesses
// nested three levels deep get an execution depth incremented by one per nesting level.
func TestNestedPlainSubProcessesTrackExecutionDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn")
	require.NoError(t, err)

	handler := bpmnEngine.NewTaskHandler().Id("service_task").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	outerSubProcess := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	middleSubProcess := waitForChildInstanceOfType(t, outerSubProcess.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	innerSubProcess := waitForChildInstanceOfType(t, middleSubProcess.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)

	assert.Equal(t, int64(1), outerSubProcess.ProcessInstance().ExecutionDepth)
	assert.Equal(t, int64(2), middleSubProcess.ProcessInstance().ExecutionDepth)
	assert.Equal(t, int64(3), innerSubProcess.ProcessInstance().ExecutionDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestMultiInstanceSubProcessTracksExecutionDepth verifies that a multi-instance subprocess
// creates a multi-instance child at depth 1 and the subprocess body instance at depth 2.
func TestMultiInstanceSubProcessTracksExecutionDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	require.NoError(t, err)

	handler := bpmnEngine.NewTaskHandler().Type("TestType").Handler(func(job ActivatedJob) {
		job.SetOutputVariable("testJobOutput", "done")
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"testInputCollection": []string{"item-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	multiInstanceChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeMultiInstance, 5*time.Second)
	subProcessBody := waitForChildInstanceOfType(t, multiInstanceChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)

	assert.Equal(t, int64(1), multiInstanceChild.ProcessInstance().ExecutionDepth)
	assert.Equal(t, int64(2), subProcessBody.ProcessInstance().ExecutionDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedMessageEventSubProcessTracksExecutionDepth verifies that a message event subprocess
// nested inside an embedded subprocess runs at execution depth 2. It uses a dedicated copy of
// the nested message BPMN (unique message name and correlation key) because active message
// subscriptions are unique per message + correlation key, so sharing them with other tests
// would make instance creations collide.
func TestNestedMessageEventSubProcessTracksExecutionDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-nested-depth.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	correlationKey := "correlation-key-msg-nested-depth"
	require.NoError(t, bpmnEngine.PublishMessageByName(t.Context(), "messageNestedDepthRef", &correlationKey, map[string]any{}))

	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().ExecutionDepth)

	// complete the pending jobs so the whole instance tree can finish
	subProcessJob := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), subProcessJob.Key, nil))
	eventSubProcessJob := waitForPendingJob(t, eventSubProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), eventSubProcessJob.Key, nil))

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedTimerEventSubProcessTracksExecutionDepth verifies that a timer event subprocess
// nested inside an embedded subprocess runs at execution depth 2.
func TestNestedTimerEventSubProcessTracksExecutionDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	// the non-interrupting timer start event (PT1S) fires on its own and starts the event subprocess
	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 15*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().ExecutionDepth)

	// complete the pending jobs so the whole instance tree can finish
	subProcessJob := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), subProcessJob.Key, nil))
	eventSubProcessJob := waitForPendingJob(t, eventSubProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), eventSubProcessJob.Key, nil))

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedErrorEventSubProcessTracksExecutionDepth verifies that an error event subprocess
// declared inside an embedded subprocess runs at execution depth 2 when a job error is caught.
func TestNestedErrorEventSubProcessTracksExecutionDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_event_subprocess/error-event-subprocess-nested-in-subprocess.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	job := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().ExecutionDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedPlainSubProcessExceedingMaxExecutionDepthCreatesIncident verifies that creating a
// plain (embedded) subprocess beyond the configured maximum execution depth raises an incident.
func TestNestedPlainSubProcessExceedingMaxExecutionDepthCreatesIncident(t *testing.T) {
	const maxDepth = int64(2)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	// the innermost subprocess would run at depth 3 (> maxDepth) and must be rejected
	incident := waitForExecutionDepthIncident(t, store, 5*time.Second)
	assertExecutionDepthIncident(t, store, incident, maxDepth)
}

// TestMultiInstanceSubProcessExceedingMaxExecutionDepthCreatesIncident verifies that creating a
// multi-instance subprocess body beyond the configured maximum execution depth raises an incident.
func TestMultiInstanceSubProcessExceedingMaxExecutionDepthCreatesIncident(t *testing.T) {
	const maxDepth = int64(1)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"testInputCollection": []string{"item-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().ExecutionDepth)

	// the multi-instance child runs at depth 1, its subprocess body would run at depth 2 (> maxDepth)
	incident := waitForExecutionDepthIncident(t, store, 5*time.Second)
	assertExecutionDepthIncident(t, store, incident, maxDepth)
}

// TestNestedMessageEventSubProcessExceedingMaxExecutionDepthCreatesIncident verifies that
// activating a message event subprocess beyond the configured maximum execution depth raises
// an incident instead of failing the message publish.
func TestNestedMessageEventSubProcessExceedingMaxExecutionDepthCreatesIncident(t *testing.T) {
	const maxDepth = int64(1)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-nested-depth.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	parentJob := waitForActiveJobByTypeInStore(t, store, "msgDepthSubProcessJobType", 5*time.Second)
	subProcessChild, err := store.FindProcessInstanceByKey(t.Context(), parentJob.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	// the event subprocess would run at depth 2 (> maxDepth); the publish itself must succeed
	correlationKey := "correlation-key-msg-nested-depth"
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageNestedDepthRef", &correlationKey, map[string]any{}))

	incident := waitForExecutionDepthIncident(t, store, 5*time.Second)
	assertExecutionDepthIncident(t, store, incident, maxDepth)

	activeSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), subProcessChild.ProcessInstance().Key, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, activeSubscriptions, "the rejected event-subprocess message trigger must not remain active")
	completedSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), subProcessChild.ProcessInstance().Key, runtime.ActivityStateCompleted)
	require.NoError(t, err)
	assert.Len(t, completedSubscriptions, 1)
}

// TestNestedTimerEventSubProcessExceedingMaxExecutionDepthCreatesIncident verifies that
// activating a timer event subprocess beyond the configured maximum execution depth raises
// an incident when the timer fires.
func TestNestedTimerEventSubProcessExceedingMaxExecutionDepthCreatesIncident(t *testing.T) {
	const maxDepth = int64(1)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth, EngineWithPollTimerDelay(time.Second))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	parentJob := waitForActiveJobByTypeInStore(t, store, "subProcessJobType", 5*time.Second)
	subProcessChild, err := store.FindProcessInstanceByKey(t.Context(), parentJob.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	// the non-interrupting timer start event (PT1S) fires on its own; the event subprocess
	// would run at depth 2 (> maxDepth) and must be rejected with an incident
	incident := waitForExecutionDepthIncident(t, store, 15*time.Second)
	assertExecutionDepthIncident(t, store, incident, maxDepth)

	createdTimers, err := store.FindProcessInstanceTimers(t.Context(), subProcessChild.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, createdTimers, "the rejected event-subprocess timer must not remain eligible for polling")
	triggeredTimers, err := store.FindProcessInstanceTimers(t.Context(), subProcessChild.ProcessInstance().Key, runtime.TimerStateTriggered)
	require.NoError(t, err)
	assert.Len(t, triggeredTimers, 1)

	incidentCount := executionDepthIncidentCount(t, store, incident.ProcessInstanceKey)
	started := time.Now()
	require.Eventually(t, func() bool {
		return time.Since(started) >= 2500*time.Millisecond &&
			executionDepthIncidentCount(t, store, incident.ProcessInstanceKey) == incidentCount
	}, 3*time.Second, 100*time.Millisecond, "a consumed timer must not create repeated execution-depth incidents")
}

// TestNestedErrorEventSubProcessExceedingMaxExecutionDepthCreatesIncident verifies that
// activating an error event subprocess beyond the configured maximum execution depth fails
// the job with an incident describing the potential infinite loop.
func TestNestedErrorEventSubProcessExceedingMaxExecutionDepthCreatesIncident(t *testing.T) {
	const maxDepth = int64(1)
	engine, store := startEngineWithMaxExecutionDepth(t, maxDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/error_event_subprocess/error-event-subprocess-nested-in-subprocess.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	subProcessChild := waitForChildInstanceOfTypeInStore(t, store, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().ExecutionDepth)

	job := waitForPendingJobInStore(t, store, subProcessChild.ProcessInstance().Key)
	// the error event subprocess would run at depth 2 (> maxDepth); the job fail must succeed
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	incident := waitForExecutionDepthIncident(t, store, 5*time.Second)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed execution depth of %d", maxDepth))

	// the job carrying the incident must have been failed instead of activating the event subprocess
	failedJob, err := store.FindJobByJobKey(t.Context(), job.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, failedJob.State)
}

// startEngineWithMaxExecutionDepth starts a dedicated engine backed by a fresh in-memory
// storage with the given maximum execution depth. The engine is stopped on test cleanup.
func startEngineWithMaxExecutionDepth(t *testing.T, maxDepth int64, extraOptions ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	options := append([]EngineOption{
		EngineWithStorage(store),
		EngineWithMaxExecutionDepth(maxDepth),
	}, extraOptions...)
	engine := NewEngine(options...)
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

// waitForExecutionDepthIncident waits for and returns the first unresolved incident in the
// given store whose message reports a potential infinite loop (max execution depth breach).
func waitForExecutionDepthIncident(t *testing.T, store *inmemory.Storage, timeout time.Duration) runtime.Incident {
	t.Helper()
	var incident runtime.Incident
	require.Eventually(t, func() bool {
		for _, pi := range store.ProcessInstancesSnapshot() {
			incidents, findErr := store.FindIncidentsByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key)
			if findErr != nil {
				continue
			}
			for _, candidate := range incidents {
				if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "potential infinite loop detected") {
					incident = candidate
					return true
				}
			}
		}
		return false
	}, timeout, 50*time.Millisecond, "expected an execution depth incident to be created")
	return incident
}

func executionDepthIncidentCount(t *testing.T, store *inmemory.Storage, processInstanceKey int64) int {
	t.Helper()
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	count := 0
	for _, incident := range incidents {
		if strings.Contains(incident.Message, "potential infinite loop detected") {
			count++
		}
	}
	return count
}

// assertExecutionDepthIncident asserts that the given incident reports the configured maximum
// execution depth and that the instance carrying it (the instance that attempted to spawn the
// too-deep child) sits at exactly maxDepth and has been marked failed.
func assertExecutionDepthIncident(t *testing.T, store *inmemory.Storage, incident runtime.Incident, maxDepth int64) {
	t.Helper()
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed execution depth of %d", maxDepth))
	failedInstance, err := store.FindProcessInstanceByKey(t.Context(), incident.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, maxDepth, failedInstance.ProcessInstance().ExecutionDepth)
	assert.Equal(t, runtime.ActivityStateFailed, failedInstance.ProcessInstance().State)
}

// waitForPendingJobInStore waits for and returns the first pending job of the given process
// instance in the given store.
func waitForPendingJobInStore(t *testing.T, store *inmemory.Storage, instanceKey int64) runtime.Job {
	t.Helper()
	var job runtime.Job
	require.Eventually(t, func() bool {
		jobs, jobErr := store.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		if jobErr != nil || len(jobs) == 0 {
			return false
		}
		job = jobs[0]
		return true
	}, 2*time.Second, 50*time.Millisecond, "expected a pending job for process instance %d", instanceKey)
	return job
}

func waitForActiveJobByTypeInStore(t *testing.T, store *inmemory.Storage, jobType string, timeout time.Duration) runtime.Job {
	t.Helper()
	var job runtime.Job
	require.Eventually(t, func() bool {
		jobs, err := store.FindActiveJobsByType(t.Context(), jobType)
		if err != nil || len(jobs) == 0 {
			return false
		}
		job = jobs[0]
		return true
	}, timeout, 50*time.Millisecond, "expected active job of type %s", jobType)
	return job
}

// waitForChildInstanceOfType waits for and returns a child process instance of the given type
// whose parent execution token belongs to the given parent process instance.
func waitForChildInstanceOfType(t *testing.T, parentInstanceKey int64, processType runtime.ProcessType, timeout time.Duration) runtime.ProcessInstance {
	t.Helper()
	return waitForChildInstanceOfTypeInStore(t, engineStorage, parentInstanceKey, processType, timeout)
}

// waitForChildInstanceOfTypeInStore waits for and returns a child process instance of the given
// type in the given store whose parent execution token belongs to the given parent process instance.
func waitForChildInstanceOfTypeInStore(t *testing.T, store *inmemory.Storage, parentInstanceKey int64, processType runtime.ProcessType, timeout time.Duration) runtime.ProcessInstance {
	t.Helper()
	var found runtime.ProcessInstance
	require.Eventually(t, func() bool {
		for _, pi := range store.ProcessInstancesSnapshot() {
			if pi.Type() != processType {
				continue
			}
			var parentKey int64
			switch typed := pi.(type) {
			case *runtime.SubProcessInstance:
				parentKey = typed.ParentProcessExecutionToken.ProcessInstanceKey
			case *runtime.MultiInstanceInstance:
				parentKey = typed.ParentProcessExecutionToken.ProcessInstanceKey
			case *runtime.CallActivityInstance:
				parentKey = typed.ParentProcessExecutionToken.ProcessInstanceKey
			default:
				continue
			}
			if parentKey == parentInstanceKey {
				found = pi
				return true
			}
		}
		return false
	}, timeout, 50*time.Millisecond, "expected a %s child instance under parent %d", processType, parentInstanceKey)
	return found
}
