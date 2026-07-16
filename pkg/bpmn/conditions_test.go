package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExclusiveGatewayWithExpressionsSelectsOneAndNotTheOther(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -50,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-b", cp.CallPath)
}

// Test of corrupted process (no outgoing conditional flows resolved positively nor default flow defined). Incident has to be raised
func TestExclusiveGatewayWithExpressionsFailsNoDefault(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition-and-default.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -1,
	}

	// when
	// Run the engine and check that Gateway haven't triggered any task
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.ErrorContains(t, err, "No default flow, nor matching expressions found, for flow elements")

	// no tasks called
	assert.Equal(t, "", cp.CallPath)
}

func TestExclusiveGatewayExecutesJustOneMatchingPath(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-multiple-tasks.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	defH := bpmnEngine.NewTaskHandler().Id("task-default").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(defH)
	variables := map[string]interface{}{
		"price": 0,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-a", cp.CallPath)
}

func TestExclusiveGatewayExecutesJustNoMatchingPathDefaultIsUsed(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-multiple-tasks.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	defH := bpmnEngine.NewTaskHandler().Id("task-default").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(defH)
	variables := map[string]interface{}{
		"price": -99,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-default", cp.CallPath)
}

func TestExclusiveGatewayExecutesJustNoMatchingNoDefaultErrorThrown(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-multiple-tasks-no-default.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	defH := bpmnEngine.NewTaskHandler().Id("task-default").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(defH)
	variables := map[string]interface{}{
		"price": -99,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)

	// then
	assert.NotNil(t, err)
	assert.Equal(t, "", cp.CallPath)
}

func TestBooleanExpressionWithoutEqualShouldBeTreatedAsConstant(t *testing.T) {
	variables := map[string]interface{}{
		"aValue": 3,
	}

	result, err := bpmnEngine.evaluateExpression("aValue > 1", variables)
	assert.NoError(t, err)

	assert.Equal(t, "aValue > 1", result)
}

func TestBooleanExpressionWithEqualSignEvaluates(t *testing.T) {
	variables := map[string]interface{}{
		"aValue": 3,
	}

	result, err := bpmnEngine.evaluateExpression("= aValue > 1", variables)
	assert.NoError(t, err)

	assert.True(t, result.(bool))
}

func TestMathematicalExpressionEvaluates(t *testing.T) {
	variables := map[string]interface{}{
		"foo": 3,
		"bar": 7,
		"sum": 10,
	}

	result, err := bpmnEngine.evaluateExpression("=sum >= foo + bar", variables)
	assert.NoError(t, err)

	assert.True(t, result.(bool))
}

func TestEvaluationErrorPercolatesUp(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)

	// when
	// don't provide variables, for execution to get an evaluation error
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Equal(t, runtime.ActivityStateFailed, instance.ProcessInstance().State)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "did not evaluate to a boolean")
}

func TestInclusiveGatewayWithExpressionsSelectsOneAndNotTheOther(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/inclusive-gateway-with-condition.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -50,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, cp.CallPath, "task-b")
}

// Test of corrupted process (no outgoing conditional flows resolved positively nor default flow defined). Incident has to be raised
func TestInclusiveGatewayWithExpressionsSelectsDefault(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/inclusive-gateway-with-condition-and-default.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -1,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.Error(t, err)

	// process stuck with no further advance after inclusive gateway
	assert.Equal(t, cp.CallPath, "")
}

func TestInclusiveGatewayExecutesAllPositiveResolvedNoDefaults(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/inclusive-gateway-multiple-tasks.bpmn")
	assert.NoError(t, err)
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	defH := bpmnEngine.NewTaskHandler().Id("task-default").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(defH)
	variables := map[string]interface{}{
		"price": 0,
	}

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-a,task-b", cp.CallPath)
}

// TestInclusiveGatewayJoinSingleHistoryEntry verifies that a single synchronization
// cycle of a multi-incoming inclusive gateway produces exactly one flow element history
// entry, with CompletedAt set at the moment the join fires. The bug it catches: every
// token arrival at the join used to create its own history row, and the first arrival's
// row was prematurely marked CompletedAt before the merge.
func TestInclusiveGatewayJoinSingleHistoryEntry(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/inclusive-gateway-split-join.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"routeA": true,
		"routeB": true,
		"routeC": false,
	})
	require.NoError(t, err)

	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	jobsByElementID := make(map[string]runtime.Job, len(jobs))
	for _, job := range jobs {
		jobsByElementID[job.Token.ElementId] = job
	}

	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), jobsByElementID["service_task_a"].Key, nil))
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), jobsByElementID["service_task_b"].Key, nil))

	jobs, err = bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "service_task_after_join", jobs[0].Token.ElementId)
	require.NoError(t, bpmnEngine.JobCompleteByKey(t.Context(), jobs[0].Key, nil))

	persisted, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, persisted.ProcessInstance().State)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	require.NoError(t, err)

	joinHistory := []runtime.FlowElementInstance{}
	for _, fe := range flowElements {
		if fe.ElementId == "inclusive_gateway_join" {
			joinHistory = append(joinHistory, fe)
		}
	}
	assert.Len(t, joinHistory, 1, "one join cycle should create one inclusive gateway history instance, got: %d", len(joinHistory))
	assert.NotNil(t, joinHistory[0].CompletedAt, "inclusive gateway history must be completed exactly when the join fires")
}

// TestInclusiveGatewayJoinWaitsForQueuedGatewayToken verifies that an arrival
// already queued at the join still makes the first token wait. Internal task
// handlers can queue both arrivals before either gateway activation runs.
func TestInclusiveGatewayJoinWaitsForQueuedGatewayToken(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/inclusive-gateway-split-join.bpmn")
	require.NoError(t, err)

	for _, taskType := range []string{"inclusive-a", "inclusive-b", "inclusive-after"} {
		taskType := taskType
		h := bpmnEngine.NewTaskHandler().Type(taskType).Handler(cp.TaskHandler)
		defer bpmnEngine.RemoveHandler(h)
	}

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"routeA": true,
		"routeB": true,
		"routeC": false,
	})
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
	assert.Equal(t, "service_task_a,service_task_b,service_task_after_join", cp.CallPath)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	require.NoError(t, err)
	joinHistory := []runtime.FlowElementInstance{}
	for _, fe := range flowElements {
		if fe.ElementId == "inclusive_gateway_join" {
			joinHistory = append(joinHistory, fe)
		}
	}
	assert.Len(t, joinHistory, 1, "a queued token must join the existing gateway history instance")
}
