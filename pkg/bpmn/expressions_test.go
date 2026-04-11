package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestEvaluateExpressionWithSingleVariableContext(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	variables := map[string]interface{}{
		"foo": 10,
		"bar": 5,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= foo + bar", variables)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

func TestEvaluateExpressionWithMultipleVariableContexts(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	context1 := map[string]interface{}{
		"foo": 10,
	}
	context2 := map[string]interface{}{
		"bar": 5,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= foo + bar", context1, context2)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

func TestEvaluateExpressionMergeContextsWithOverride(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	context1 := map[string]interface{}{
		"value": 10,
	}
	context2 := map[string]interface{}{
		"value": 20, // This should override the first one
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= value", context1, context2)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(20), result)
}

func TestEvaluateExpressionWithoutEqualSign(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	variables := map[string]interface{}{
		"foo": 10,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("plain text", variables)

	// then
	assert.NoError(t, err)
	assert.Equal(t, "plain text", result)
}

func TestEvaluateExpressionWithWhitespace(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	variables := map[string]interface{}{
		"value": 42,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("  = value  ", variables)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

func TestEvaluateExpressionBooleanComparison(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given
	variables := map[string]interface{}{
		"price": 100,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= price > 50", variables)

	// then
	assert.NoError(t, err)
	assert.True(t, result.(bool))
}

func TestEvaluateExpressionEmptyContext(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given - no variable contexts provided

	// when
	result, err := bpmnEngine.evaluateExpression("= 2 + 2")

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(4), result)
}

func TestEvaluateExpressionMergedContextVariables(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given - multiple contexts with different variables
	context1 := map[string]interface{}{
		"x": 100,
	}
	context2 := map[string]interface{}{
		"y": 50,
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= x - y", context1, context2)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(50), result)
}

func TestEvaluateExpressionMultipleContextsAllVariablesAccessible(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// given - multiple contexts with overlapping and unique variables
	context1 := map[string]interface{}{
		"a": 5,
		"b": 10,
	}
	context2 := map[string]interface{}{
		"c": 3,
		"b": 20, // Override b from context1
	}

	// when
	result, err := bpmnEngine.evaluateExpression("= a + b + c", context1, context2)

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(28), result) // 5 + 20 + 3 = 28 (b is overridden to 20)
}
