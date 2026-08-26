package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecutionDepthCallActivity verifies that a call activity child runs at execution depth 1
// and an embedded subprocess inside the called process runs at execution depth 2.
func TestExecutionDepthCallActivity(t *testing.T) {
	definition, err := deployGetDefinition(t, "call-activity/call-activity-with-simple-subprocess.bpmn", "Simple_CallActivity_Process")
	require.NoError(t, err)
	_, err = deployGetDefinition(t, "simple-simple-sub-process.bpmn", "empty-sub-process")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{"testVar": 123})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	calledProcess := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, calledProcess.Key, 1)

	embeddedSubProcess := waitForDirectChildProcessInstanceWithin(t, calledProcess.Key, 15*time.Second)
	assertExecutionDepth(t, embeddedSubProcess.Key, 2)

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthPlainSubProcess verifies that plain (embedded) subprocesses nested three
// levels deep get an execution depth incremented by one per nesting level.
func TestExecutionDepthPlainSubProcess(t *testing.T) {
	definition, err := deployGetDefinition(t, "error_events/sub_process/subprocess_nested_with_error_boundery_event.bpmn", "nested_subprocess_with_error_boundery_event")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	outerSubProcess := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, outerSubProcess.Key, 1)

	middleSubProcess := waitForDirectChildProcessInstanceWithin(t, outerSubProcess.Key, 15*time.Second)
	assertExecutionDepth(t, middleSubProcess.Key, 2)

	innerSubProcess := waitForDirectChildProcessInstanceWithin(t, middleSubProcess.Key, 15*time.Second)
	assertExecutionDepth(t, innerSubProcess.Key, 3)

	completeJobForElementId(t, innerSubProcess.Key, "service_task", nil)
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthMultiInstance verifies that a multi-instance subprocess creates the
// multi-instance child at execution depth 1 and the subprocess body at execution depth 2.
func TestExecutionDepthMultiInstance(t *testing.T) {
	definition, err := deployGetDefinition(t, "multi_instance_sub_process_task.bpmn", "MultiInstance_Sub_Process_Task_Process")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{
		"testInputCollection": []string{"item-1"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	multiInstanceChild := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, multiInstanceChild.Key, 1)

	subProcessBody := waitForDirectChildProcessInstanceWithin(t, multiInstanceChild.Key, 15*time.Second)
	assertExecutionDepth(t, subProcessBody.Key, 2)

	completeJobForElementId(t, subProcessBody.Key, "id", map[string]any{"testJobOutput": "done"})
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthMessageEventSubprocess verifies that a message event subprocess nested
// inside an embedded subprocess runs at execution depth 2. It uses a dedicated copy of the
// nested message BPMN (unique message name and correlation key) because active message
// subscriptions are unique per message + correlation key, so sharing them with other tests
// would make instance creations collide.
func TestExecutionDepthMessageEventSubprocess(t *testing.T) {
	definition, err := deployGetDefinition(t, "message_event_subprocess/message-event-subprocess-non-interrupting-nested-depth.bpmn", "Process_msgNestedDepth1")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	subProcessChild := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, subProcessChild.Key, 1)

	err = publishMessage(t, "messageNestedDepthRef", "correlation-key-msg-nested-depth", &map[string]any{})
	require.NoError(t, err)

	eventSubProcessChild := waitForDirectChildProcessInstanceWithin(t, subProcessChild.Key, 15*time.Second)
	assertExecutionDepth(t, eventSubProcessChild.Key, 2)

	completeJobForElementId(t, subProcessChild.Key, "ServiceTaskA_1dv8s8j", nil)
	completeJobForElementId(t, eventSubProcessChild.Key, "ServiceTaskB_00m7f5d", nil)
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthTimerEventSubprocess verifies that a timer event subprocess nested inside
// an embedded subprocess runs at execution depth 2 once the timer (PT1S) fires.
func TestExecutionDepthTimerEventSubprocess(t *testing.T) {
	definition, err := deployGetDefinition(t, "timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn", "Process_1c1lgem")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	subProcessChild := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, subProcessChild.Key, 1)

	// the non-interrupting timer start event fires on its own and starts the event subprocess
	eventSubProcessChild := waitForDirectChildProcessInstanceWithin(t, subProcessChild.Key, 20*time.Second)
	assertExecutionDepth(t, eventSubProcessChild.Key, 2)

	completeJobForElementId(t, subProcessChild.Key, "ServiceTaskA_1dv8s8j", nil)
	completeJobForElementId(t, eventSubProcessChild.Key, "ServiceTaskB_00m7f5d", nil)
	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthErrorEventSubprocess verifies that an error event subprocess declared inside
// an embedded subprocess runs at execution depth 2 when a job error is caught.
func TestExecutionDepthErrorEventSubprocess(t *testing.T) {
	definition, err := deployGetDefinition(t, "error_event_subprocess/error-event-subprocess-nested-in-subprocess.bpmn", "error-event-subprocess-nested-in-subprocess")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	assertExecutionDepth(t, instance.Key, 0)

	subProcessChild := waitForDirectChildProcessInstanceWithin(t, instance.Key, 15*time.Second)
	assertExecutionDepth(t, subProcessChild.Key, 1)

	failJobForElementId(t, subProcessChild.Key, "outer-sub-task", new("42"), nil)

	eventSubProcessChild := waitForDirectChildProcessInstanceWithin(t, subProcessChild.Key, 15*time.Second)
	assertExecutionDepth(t, eventSubProcessChild.Key, 2)

	waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
}

// TestExecutionDepthExceededCreatesIncident verifies that a recursive call activity chain stops
// at the configured maximum execution depth (e2eMaxExecutionDepth, set in TestMain) and that the
// instance attempting to spawn the too-deep child fails with an incident describing the loop.
func TestExecutionDepthExceededCreatesIncident(t *testing.T) {
	definition, err := deployGetDefinition(t, "call-activity/call-activity-recursive.bpmn", "Recursive_CallActivity_Process")
	require.NoError(t, err)

	instance, err := createProcessInstance(t, &definition.Key, map[string]any{})
	require.NoError(t, err)
	require.NotEmpty(t, instance.Key)
	t.Cleanup(func() { cleanProcessInstances(t) })
	assertExecutionDepth(t, instance.Key, 0)

	// walk the recursive chain down to the depth limit; each level must track its depth
	deepestKey := instance.Key
	for depth := int64(1); depth <= e2eMaxExecutionDepth; depth++ {
		child := waitForDirectChildProcessInstanceWithin(t, deepestKey, 15*time.Second)
		assertExecutionDepth(t, child.Key, depth)
		deepestKey = child.Key
	}

	// the deepest instance must fail instead of spawning another child ...
	waitForProcessInstanceState(t, deepestKey, zenclient.ProcessInstanceStateFailed)

	// ... and carry an unresolved incident reporting the potential infinite loop
	incident := waitForExecutionDepthIncidentOnInstance(t, deepestKey)
	assert.Contains(t, incident.Message, fmt.Sprintf("maximum allowed execution depth of %d", e2eMaxExecutionDepth))
	assert.Equal(t, "recursiveCallActivity", incident.ElementId)
}

// assertExecutionDepth reads the process instance from its partition store and asserts its
// persisted execution depth. Execution depth is not exposed through the public REST API,
// so the assertion goes directly against the storage layer.
func assertExecutionDepth(t testing.TB, processInstanceKey int64, expectedDepth int64) {
	t.Helper()

	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(processInstanceKey))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		processInstance, findErr := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
		if !assert.NoError(collect, findErr) {
			return
		}
		assert.Equal(collect, expectedDepth, processInstance.ProcessInstance().ExecutionDepth,
			"process instance %d should have execution depth %d", processInstanceKey, expectedDepth)
	}, 5*time.Second, 100*time.Millisecond,
		"process instance %d should have execution depth %d", processInstanceKey, expectedDepth)
}

// waitForDirectChildProcessInstanceWithin waits (up to the given timeout) for a process
// instance whose direct parent is the given process instance and returns it.
func waitForDirectChildProcessInstanceWithin(t testing.TB, parentProcessInstanceKey int64, timeout time.Duration) zenclient.ProcessInstancesSimple {
	t.Helper()

	var child zenclient.ProcessInstancesSimple
	require.Eventually(t, func() bool {
		page, err := getChildInstances(t, parentProcessInstanceKey)
		if err != nil || len(page.Partitions) == 0 {
			return false
		}
		for _, item := range page.Partitions[0].Items {
			if item.ParentProcessInstanceKey != nil && *item.ParentProcessInstanceKey == parentProcessInstanceKey {
				child = item
				return true
			}
		}
		return false
	}, timeout, 100*time.Millisecond, "process instance %d should create a direct child process instance", parentProcessInstanceKey)
	return child
}

// waitForExecutionDepthIncidentOnInstance waits for and returns the first unresolved incident on
// the given process instance whose message reports a potential infinite loop (execution depth breach).
func waitForExecutionDepthIncidentOnInstance(t testing.TB, processInstanceKey int64) public.Incident {
	t.Helper()

	var incident public.Incident
	require.Eventually(t, func() bool {
		incidents, err := getProcessInstanceIncidents(t, processInstanceKey)
		if err != nil {
			return false
		}
		for _, candidate := range incidents {
			if candidate.ResolvedAt == nil && strings.Contains(candidate.Message, "potential infinite loop detected") {
				incident = candidate
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "process instance %d should have an execution depth incident", processInstanceKey)
	return incident
}
