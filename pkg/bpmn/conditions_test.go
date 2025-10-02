package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func Test_exclusive_gateway_with_expressions_selects_one_and_not_the_other(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-with-condition.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -50,
	}

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-b", cp.CallPath)
}

// Test of corrupted process (no outgoing conditional flows resolved positively nor default flow defined). Incident has to be raised
func Test_exclusive_gateway_with_expressions_fails_no_default(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-with-condition-and-default.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -1,
	}

	// when
	// Run the engine and check that Gateway haven't triggered any task
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.ErrorContains(t, err, "No default flow, nor matching expressions found, for flow elements")

	// no tasks called
	assert.Equal(t, "", cp.CallPath)
}

func Test_exclusive_gateway_executes_just_one_matching_path(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-multiple-tasks.bpmn")
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
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-a", cp.CallPath)
}

func Test_exclusive_gateway_executes_just_no_matching_path_default_is_used(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-multiple-tasks.bpmn")
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
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-default", cp.CallPath)
}

func Test_exclusive_gateway_executes_just_no_matching_no_default_error_thrown(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-multiple-tasks-no-default.bpmn")
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
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)

	// then
	assert.NotNil(t, err)
	assert.Equal(t, "", cp.CallPath)
}

func Test_boolean_expression_evaluates(t *testing.T) {
	variables := map[string]interface{}{
		"aValue": 3,
	}

	result, err := evaluateExpression("aValue > 1", variables)
	assert.NoError(t, err)

	assert.True(t, result.(bool))
}

func Test_boolean_expression_with_equal_sign_evaluates(t *testing.T) {
	variables := map[string]interface{}{
		"aValue": 3,
	}

	result, err := evaluateExpression("= aValue > 1", variables)
	assert.NoError(t, err)

	assert.True(t, result.(bool))
}

func Test_mathematical_expression_evaluates(t *testing.T) {
	variables := map[string]interface{}{
		"foo": 3,
		"bar": 7,
		"sum": 10,
	}

	result, err := evaluateExpression("sum >= foo + bar", variables)
	assert.NoError(t, err)

	assert.True(t, result.(bool))
}

func Test_evaluation_error_percolates_up(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/exclusive-gateway-with-condition.bpmn")

	// when
	// don't provide variables, for execution to get an evaluation error
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "Error evaluating expression")
}

func Test_inclusive_gateway_with_expressions_selects_one_and_not_the_other(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/inclusive-gateway-with-condition.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -50,
	}

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, cp.CallPath, "task-b")
}

// Test of corrupted process (no outgoing conditional flows resolved positively nor default flow defined). Incident has to be raised
func Test_inclusive_gateway_with_expressions_selects_default(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/inclusive-gateway-with-condition-and-default.bpmn")
	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)
	variables := map[string]interface{}{
		"price": -1,
	}

	// when
	bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)

	// process stuck with no further advance after inclusive gateway
	assert.Equal(t, cp.CallPath, "")
}

func Test_inclusive_gateway_executes_all_positive_resolved_no_defaults(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/inclusive-gateway-multiple-tasks.bpmn")
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
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variables)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task-a,task-b", cp.CallPath)
}
