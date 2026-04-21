package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestEvaluateExpressionArithmetic(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	variables := map[string]interface{}{
		"foo": 10,
		"bar": 5,
	}

	result, err := bpmnEngine.evaluateExpression("= foo + bar", variables)

	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

func TestEvaluateExpressionWithoutEqualSign(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	variables := map[string]interface{}{
		"foo": 10,
	}

	result, err := bpmnEngine.evaluateExpression("plain text", variables)

	assert.NoError(t, err)
	assert.Equal(t, "plain text", result)
}

func TestEvaluateExpressionWithWhitespace(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	variables := map[string]interface{}{
		"value": 42,
	}

	result, err := bpmnEngine.evaluateExpression("  = value  ", variables)

	assert.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

func TestEvaluateExpressionBooleanComparison(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	variables := map[string]interface{}{
		"price": 100,
	}

	result, err := bpmnEngine.evaluateExpression("= price > 50", variables)

	assert.NoError(t, err)
	assert.True(t, result.(bool))
}

func TestEvaluateExpressionEmptyContext(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	result, err := bpmnEngine.evaluateExpression("= 2 + 2", nil)

	assert.NoError(t, err)
	assert.Equal(t, int64(4), result)
}

func TestEvaluateExpressionSubtraction(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	context := map[string]interface{}{
		"x": 100,
		"y": 50,
	}

	result, err := bpmnEngine.evaluateExpression("= x - y", context)

	assert.NoError(t, err)
	assert.Equal(t, int64(50), result)
}

func TestEvaluateExpressionUndefinedVariableReturnsNil(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// FEEL runtime is lenient: undefined variables evaluate to nil, not an error
	result, err := bpmnEngine.evaluateExpression("= undefined_variable", nil)

	assert.NoError(t, err)
	assert.Nil(t, result)
}
