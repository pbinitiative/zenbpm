package bpmn

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngineUsesDefaultMaxProcessInstanceNestingDepth(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.contextCancel)

	assert.Equal(t, DefaultMaxProcessInstanceNestingDepth, engine.maxProcessInstanceNestingDepth)
}

func TestRecursiveCallActivityStopsAtMaxProcessInstanceNestingDepthAndCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(3)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/call-activity/call-activity-recursive.bpmn")
	require.NoError(t, err)

	rootInstance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rootInstance.ProcessInstance().NestingDepth)

	// wait until the engine detects the loop and raises an incident
	incident := waitForNestingDepthIncident(t, engine, store, 5*time.Second)
	assert.Equal(t, "recursiveCallActivity", incident.ElementId)
	assertNestingDepthIncident(t, engine, store, incident, maxNestingDepth)

	// The chain must stop at nestingDepth == maxNestingDepth: root (0) plus maxNestingDepth children.
	instances := processInstancesSnapshot(t, engine, store)
	assert.Len(t, instances, int(maxNestingDepth)+1)
	seenNestingDepths := make(map[int64]int, len(instances))
	for _, pi := range instances {
		seenNestingDepths[pi.ProcessInstance().NestingDepth]++
	}
	for nestingDepth := int64(0); nestingDepth <= maxNestingDepth; nestingDepth++ {
		assert.Equal(t, 1, seenNestingDepths[nestingDepth], "expected exactly one process instance at nesting depth %d", nestingDepth)
	}
}

func TestChildProcessInstancesTrackNestingDepth(t *testing.T) {
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
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)

	childInstance := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
	assert.Equal(t, int64(1), childInstance.ProcessInstance().NestingDepth)
}

// TestNestedPlainSubProcessesTrackNestingDepth verifies that plain (embedded) subprocesses
// nested three levels deep get a nesting depth incremented by one per nesting level.
func TestNestedPlainSubProcessesTrackNestingDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn")
	require.NoError(t, err)

	handler := bpmnEngine.NewTaskHandler().Id("service_task").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	outerSubProcess := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	middleSubProcess := waitForChildInstanceOfType(t, outerSubProcess.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	innerSubProcess := waitForChildInstanceOfType(t, middleSubProcess.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)

	assert.Equal(t, int64(1), outerSubProcess.ProcessInstance().NestingDepth)
	assert.Equal(t, int64(2), middleSubProcess.ProcessInstance().NestingDepth)
	assert.Equal(t, int64(3), innerSubProcess.ProcessInstance().NestingDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestMultiInstanceSubProcessTracksNestingDepth verifies that a multi-instance subprocess
// creates a multi-instance child at depth 1 and the subprocess body instance at depth 2.
func TestMultiInstanceSubProcessTracksNestingDepth(t *testing.T) {
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
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	multiInstanceChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeMultiInstance, 5*time.Second)
	subProcessBody := waitForChildInstanceOfType(t, multiInstanceChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)

	assert.Equal(t, int64(1), multiInstanceChild.ProcessInstance().NestingDepth)
	assert.Equal(t, int64(2), subProcessBody.ProcessInstance().NestingDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedMessageEventSubProcessTracksNestingDepth verifies that a message event subprocess
// nested inside an embedded subprocess runs at nesting depth 2. It uses a dedicated copy of
// the nested message BPMN (unique message name and correlation key) because active message
// subscriptions are unique per message + correlation key, so sharing them with other tests
// would make instance creations collide.
func TestNestedMessageEventSubProcessTracksNestingDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-nesting-depth.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	correlationKey := "correlation-key-msg-nesting-depth"
	require.NoError(t, bpmnEngine.PublishMessageByName(t.Context(), "messageNestingDepthRef", &correlationKey, map[string]any{}))

	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().NestingDepth)

	// complete the pending jobs so the whole instance tree can finish
	subProcessJob := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), subProcessJob.Key, nil))
	eventSubProcessJob := waitForPendingJob(t, eventSubProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), eventSubProcessJob.Key, nil))

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedTimerEventSubProcessTracksNestingDepth verifies that a timer event subprocess
// nested inside an embedded subprocess runs at nesting depth 2.
func TestNestedTimerEventSubProcessTracksNestingDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	// the non-interrupting timer start event (PT1S) fires on its own and starts the event subprocess
	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 15*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().NestingDepth)

	// complete the pending jobs so the whole instance tree can finish
	subProcessJob := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), subProcessJob.Key, nil))
	eventSubProcessJob := waitForPendingJob(t, eventSubProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), eventSubProcessJob.Key, nil))

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedErrorEventSubProcessTracksNestingDepth verifies that an error event subprocess
// declared inside an embedded subprocess runs at nesting depth 2 when a job error is caught.
func TestNestedErrorEventSubProcessTracksNestingDepth(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/error_event_subprocess/error-event-subprocess-nested-in-subprocess.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	subProcessChild := waitForChildInstanceOfType(t, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	job := waitForPendingJob(t, subProcessChild.ProcessInstance().Key)
	require.NoError(t, bpmnEngine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	eventSubProcessChild := waitForChildInstanceOfType(t, subProcessChild.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(2), eventSubProcessChild.ProcessInstance().NestingDepth)

	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5*time.Second, 50*time.Millisecond)
}

// TestNestedPlainSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident verifies that creating a
// plain (embedded) subprocess beyond the configured maximum nesting depth raises an incident.
func TestNestedPlainSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(2)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	// the innermost subprocess would run at depth 3 (> maxNestingDepth) and must be rejected
	incident := waitForNestingDepthIncident(t, engine, store, 5*time.Second)
	assertNestingDepthIncident(t, engine, store, incident, maxNestingDepth)
}

// TestMultiInstanceSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident verifies that creating a
// multi-instance subprocess body beyond the configured maximum nesting depth raises an incident.
func TestMultiInstanceSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(1)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"testInputCollection": []string{"item-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), instance.ProcessInstance().NestingDepth)

	// the multi-instance child runs at depth 1, its subprocess body would run at depth 2 (> maxNestingDepth)
	incident := waitForNestingDepthIncident(t, engine, store, 5*time.Second)
	assertNestingDepthIncident(t, engine, store, incident, maxNestingDepth)
}

// TestNestedMessageEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident verifies that
// activating a message event subprocess beyond the configured maximum nesting depth raises
// an incident instead of failing the message publish.
func TestNestedMessageEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(1)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/message_event_subprocess/message-event-subprocess-non-interrupting-nesting-depth.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	parentJob := waitForActiveJobByTypeInStore(t, store, "msgNestingDepthSubProcessJobType", 5*time.Second)
	subProcessChild, err := store.FindProcessInstanceByKey(t.Context(), parentJob.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	// the event subprocess would run at depth 2 (> maxNestingDepth); the publish itself must succeed
	correlationKey := "correlation-key-msg-nesting-depth"
	require.NoError(t, engine.PublishMessageByName(t.Context(), "messageNestingDepthRef", &correlationKey, map[string]any{
		"rejectedPayload": "must-not-propagate",
	}))

	incident := waitForNestingDepthIncident(t, engine, store, 5*time.Second)
	assertNestingDepthIncident(t, engine, store, incident, maxNestingDepth)
	assert.Zero(t, incident.Token.Key, "event-subprocess depth incidents must not resume an unrelated token")
	assert.Equal(t, "eventSubprocessMessageEvent_010eof4", incident.ElementId)
	parentSnapshot, err := processInstanceSnapshot(t, engine, store, subProcessChild.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Nil(t, parentSnapshot.ProcessInstance().GetVariable("rejectedPayload"), "rejected trigger variables must not mutate the parent")

	activeSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), subProcessChild.ProcessInstance().Key, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, activeSubscriptions, "the rejected event-subprocess message trigger must not remain active")
	completedSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), subProcessChild.ProcessInstance().Key, runtime.ActivityStateCompleted)
	require.NoError(t, err)
	assert.Len(t, completedSubscriptions, 1)

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	activeSubscriptions, err = store.FindProcessInstanceMessageSubscriptions(t.Context(), subProcessChild.ProcessInstance().Key, runtime.ActivityStateActive)
	require.NoError(t, err)
	require.Len(t, activeSubscriptions, 1, "resolving the incident must recreate the consumed message subscription")
	assert.Equal(t, incident.ElementId, activeSubscriptions[0].MessageSubscription().ElementId)
}

// TestNestedTimerEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident verifies that
// activating a timer event subprocess beyond the configured maximum nesting depth raises
// an incident when the timer fires.
func TestNestedTimerEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(1)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth, EngineWithPollTimerDelay(time.Second))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	parentJob := waitForActiveJobByTypeInStore(t, store, "subProcessJobType", 5*time.Second)
	subProcessChild, err := store.FindProcessInstanceByKey(t.Context(), parentJob.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	// the non-interrupting timer start event (PT1S) fires on its own; the event subprocess
	// would run at depth 2 (> maxNestingDepth) and must be rejected with an incident
	incident := waitForNestingDepthIncident(t, engine, store, 15*time.Second)
	assertNestingDepthIncident(t, engine, store, incident, maxNestingDepth)
	assert.Zero(t, incident.Token.Key, "event-subprocess depth incidents must not resume an unrelated token")

	createdTimers, err := store.FindProcessInstanceTimers(t.Context(), subProcessChild.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, createdTimers, "the rejected event-subprocess timer must not remain eligible for polling")
	triggeredTimers, err := store.FindProcessInstanceTimers(t.Context(), subProcessChild.ProcessInstance().Key, runtime.TimerStateTriggered)
	require.NoError(t, err)
	assert.Len(t, triggeredTimers, 1)

	incidentCount := nestingDepthIncidentCount(t, store, incident.ProcessInstanceKey)
	started := time.Now()
	require.Eventually(t, func() bool {
		return time.Since(started) >= 2500*time.Millisecond &&
			nestingDepthIncidentCount(t, store, incident.ProcessInstanceKey) == incidentCount
	}, 3*time.Second, 100*time.Millisecond, "a consumed timer must not create repeated nesting-depth incidents")

	require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
	createdTimers, err = store.FindProcessInstanceTimers(t.Context(), subProcessChild.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, createdTimers, 1, "resolving the incident must recreate the consumed timer")
	assert.Equal(t, incident.ElementId, createdTimers[0].ElementId)
}

// TestNestedErrorEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident verifies that
// activating an error event subprocess beyond the configured maximum nesting depth fails
// the job with an incident describing the potential infinite loop.
func TestNestedErrorEventSubProcessExceedingMaxProcessInstanceNestingDepthCreatesIncident(t *testing.T) {
	const maxNestingDepth = int64(1)
	engine, store := startEngineWithMaxProcessInstanceNestingDepth(t, maxNestingDepth)

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/error_event_subprocess/error-event-subprocess-nested-in-subprocess.bpmn")
	require.NoError(t, err)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	subProcessChild := waitForChildInstanceOfTypeInStore(t, engine, store, instance.ProcessInstance().Key, runtime.ProcessTypeSubProcess, 5*time.Second)
	assert.Equal(t, int64(1), subProcessChild.ProcessInstance().NestingDepth)

	job := waitForPendingJobInStore(t, store, subProcessChild.ProcessInstance().Key)
	// the error event subprocess would run at depth 2 (> maxNestingDepth); the job fail must succeed
	require.NoError(t, engine.JobFailByKey(t.Context(), job.Key, "boom", new("42"), nil))

	incident := waitForNestingDepthIncident(t, engine, store, 5*time.Second)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed process instance nesting depth of %d", maxNestingDepth))

	// the job carrying the incident must have been failed instead of activating the event subprocess
	failedJob, err := store.FindJobByJobKey(t.Context(), job.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, failedJob.State)
}

// startEngineWithMaxProcessInstanceNestingDepth starts a dedicated engine backed by a fresh in-memory
// storage with the given maximum nesting depth. The engine is stopped on test cleanup.
func startEngineWithMaxProcessInstanceNestingDepth(t *testing.T, maxNestingDepth int64, extraOptions ...EngineOption) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	options := append([]EngineOption{
		EngineWithStorage(store),
		EngineWithMaxProcessInstanceNestingDepth(maxNestingDepth),
	}, extraOptions...)
	engine := NewEngine(options...)
	require.NoError(t, engine.Start(t.Context()))
	t.Cleanup(engine.Stop)
	return &engine, store
}

// waitForNestingDepthIncident waits for and returns the first unresolved incident in the
// given store whose message reports a potential infinite loop (max nesting depth breach).
func waitForNestingDepthIncident(t *testing.T, engine *Engine, store *inmemory.Storage, timeout time.Duration) runtime.Incident {
	t.Helper()
	var incident runtime.Incident
	require.Eventually(t, func() bool {
		for _, pi := range processInstancesSnapshot(t, engine, store) {
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
	}, timeout, 50*time.Millisecond, "expected an nesting depth incident to be created")
	return incident
}

func nestingDepthIncidentCount(t *testing.T, store *inmemory.Storage, processInstanceKey int64) int {
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

// assertNestingDepthIncident asserts that the given incident reports the configured maximum
// nesting depth and that the instance carrying it sits at exactly maxNestingDepth. Token-bound
// incidents fail that instance; recoverable event-subprocess subscription incidents do not.
func assertNestingDepthIncident(t *testing.T, engine *Engine, store *inmemory.Storage, incident runtime.Incident, maxNestingDepth int64) {
	t.Helper()
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed process instance nesting depth of %d", maxNestingDepth))
	failedInstance, err := processInstanceSnapshot(t, engine, store, incident.ProcessInstanceKey)
	require.NoError(t, err)
	assert.Equal(t, maxNestingDepth, failedInstance.ProcessInstance().NestingDepth)
	if incident.Token.Key == 0 {
		assert.NotEqual(t, runtime.ActivityStateFailed, failedInstance.ProcessInstance().State)
	} else {
		assert.Equal(t, runtime.ActivityStateFailed, failedInstance.ProcessInstance().State)
	}
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
	return waitForChildInstanceOfTypeInStore(t, &bpmnEngine, engineStorage, parentInstanceKey, processType, timeout)
}

// waitForChildInstanceOfTypeInStore waits for and returns a child process instance of the given
// type in the given store whose parent execution token belongs to the given parent process instance.
func waitForChildInstanceOfTypeInStore(t *testing.T, engine *Engine, store *inmemory.Storage, parentInstanceKey int64, processType runtime.ProcessType, timeout time.Duration) runtime.ProcessInstance {
	t.Helper()
	var found runtime.ProcessInstance
	require.Eventually(t, func() bool {
		for _, pi := range processInstancesSnapshot(t, engine, store) {
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

func processInstancesSnapshot(t *testing.T, engine *Engine, store *inmemory.Storage) []runtime.ProcessInstance {
	t.Helper()
	liveInstances := store.ProcessInstancesSnapshot()
	snapshots := make([]runtime.ProcessInstance, 0, len(liveInstances))
	for _, live := range liveInstances {
		snapshot, err := processInstanceSnapshot(t, engine, store, live.ProcessInstance().Key)
		if err == nil {
			snapshots = append(snapshots, snapshot)
		}
	}
	return snapshots
}

func processInstanceSnapshot(t *testing.T, engine *Engine, store *inmemory.Storage, key int64) (runtime.ProcessInstance, error) {
	t.Helper()
	engine.runningInstances.lockInstance(key)
	defer engine.runningInstances.unlockInstance(key)

	live, err := store.FindProcessInstanceByKey(t.Context(), key)
	if err != nil {
		return nil, err
	}
	snapshot, err := processInstanceWithState(live, live.ProcessInstance().State)
	if err != nil {
		return nil, err
	}
	snapshot.ProcessInstance().VariableHolder = runtime.NewVariableHolder(nil, maps.Clone(live.ProcessInstance().VariableHolder.LocalVariables()))
	return snapshot, nil
}
