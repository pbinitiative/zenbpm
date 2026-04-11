package runtime

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariableHolderNewVariableHolder(t *testing.T) {
	tests := []struct {
		name      string
		parent    *VariableHolder
		variables map[string]interface{}
		wantVars  map[string]interface{}
	}{
		{
			name:      "with nil parent and variables",
			parent:    nil,
			variables: map[string]interface{}{"key1": "value1"},
			wantVars:  map[string]interface{}{"key1": "value1"},
		},
		{
			name:      "with parent and no variables copies parent vars",
			parent:    &VariableHolder{localVariables: map[string]interface{}{"parentKey": "parentValue"}},
			variables: nil,
			wantVars:  map[string]interface{}{"parentKey": "parentValue"},
		},
		{
			name:      "with both parent and variables uses provided variables",
			parent:    &VariableHolder{localVariables: map[string]interface{}{"parentKey": "parentValue"}},
			variables: map[string]interface{}{"key1": "value1"},
			wantVars:  map[string]interface{}{"key1": "value1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NewVariableHolder(tt.parent, tt.variables)
			assert.Equal(t, tt.wantVars, result.LocalVariables())
		})
	}
}

func TestVariableHolderGetSetLocalVariable(t *testing.T) {
	vh := NewVariableHolder(nil, nil)

	t.Run("get non-existent variable returns nil", func(t *testing.T) {
		result := vh.GetLocalVariable("nonexistent")
		assert.Nil(t, result)
	})

	t.Run("set and get variable", func(t *testing.T) {
		vh.SetLocalVariable("key", "value")
		result := vh.GetLocalVariable("key")
		assert.Equal(t, "value", result)
	})

	t.Run("delete local variable", func(t *testing.T) {
		vh.SetLocalVariable("deleteMe", "tempValue")
		vh.DeleteLocalVariable("deleteMe")
		result := vh.GetLocalVariable("deleteMe")
		assert.Nil(t, result)
	})
}

func TestVariableHolderPropagateVariable(t *testing.T) {
	parentVH := NewVariableHolder(nil, nil)
	childVH := NewVariableHolder(&parentVH, nil)

	childVH.PropagateVariable("key", "value")
	assert.Equal(t, "value", parentVH.GetLocalVariable("key"))
}

func TestVariableHolderPropagateVariables(t *testing.T) {
	parentVH := NewVariableHolder(nil, nil)
	childVH := NewVariableHolder(&parentVH, nil)

	variables := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}
	childVH.PropagateVariables(variables)

	assert.Equal(t, "value1", parentVH.GetLocalVariable("key1"))
	assert.Equal(t, "value2", parentVH.GetLocalVariable("key2"))
}

func TestVariableHolderEvaluateAndSetMappingsToLocalVariables(t *testing.T) {
	tests := []struct {
		name                      string
		parentVariables           map[string]interface{}
		additionalVariableContext map[string]interface{}
		mappings                  []extensions.TIoMapping
		evaluateExpressionFunc    func(expression string, variableContext ...map[string]interface{}) (interface{}, error)
		expectedLocalVariables    map[string]interface{}
		expectedError             bool
	}{
		{
			name:                      "with nil parent returns nil",
			parentVariables:           nil,
			additionalVariableContext: nil,
			mappings:                  []extensions.TIoMapping{{Source: "test", Target: "target"}},
			expectedLocalVariables:    map[string]interface{}{},
			expectedError:             false,
		},
		{
			name:                      "successfully map with parent variables only",
			parentVariables:           map[string]interface{}{"parentKey": "parentValue"},
			additionalVariableContext: nil,
			mappings: []extensions.TIoMapping{
				{Source: "parentKey", Target: "target"},
			},
			evaluateExpressionFunc: func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
				if len(variableContext) > 0 {
					if val, ok := variableContext[0]["parentKey"]; ok {
						return val, nil
					}
				}
				return nil, errors.New("key not found")
			},
			expectedLocalVariables: map[string]interface{}{"parentKey": "parentValue", "target": "parentValue"},
			expectedError:          false,
		},
		{
			name:                      "successfully map with merged context (parent + additional)",
			parentVariables:           map[string]interface{}{"parentKey": "parentValue"},
			additionalVariableContext: map[string]interface{}{"additionalKey": "additionalValue"},
			mappings: []extensions.TIoMapping{
				{Source: "parentKey", Target: "targetParent"},
				{Source: "additionalKey", Target: "targetAdditional"},
			},
			evaluateExpressionFunc: func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
				if len(variableContext) > 0 {
					if val, ok := variableContext[0][expression]; ok {
						return val, nil
					}
				}
				return nil, errors.New("key not found")
			},
			expectedLocalVariables: map[string]interface{}{
				"parentKey":        "parentValue",
				"targetParent":     "parentValue",
				"targetAdditional": "additionalValue",
			},
			expectedError: false,
		},
		{
			name:                      "additional context overrides parent context",
			parentVariables:           map[string]interface{}{"key": "parentValue"},
			additionalVariableContext: map[string]interface{}{"key": "additionalValue"},
			mappings: []extensions.TIoMapping{
				{Source: "key", Target: "target"},
			},
			evaluateExpressionFunc: func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
				if len(variableContext) > 0 {
					if val, ok := variableContext[0]["key"]; ok {
						return val, nil
					}
				}
				return nil, errors.New("key not found")
			},
			expectedLocalVariables: map[string]interface{}{"key": "parentValue", "target": "additionalValue"},
			expectedError:          false,
		},
		{
			name:                      "evaluation error returns error",
			parentVariables:           map[string]interface{}{"key": "value"},
			additionalVariableContext: nil,
			mappings: []extensions.TIoMapping{
				{Source: "nonexistent", Target: "target"},
			},
			evaluateExpressionFunc: func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
				return nil, errors.New("evaluation failed")
			},
			expectedLocalVariables: map[string]interface{}{},
			expectedError:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parentVH *VariableHolder
			if tt.parentVariables != nil {
				parent := NewVariableHolder(nil, tt.parentVariables)
				parentVH = &parent
			}

			childVH := NewVariableHolder(parentVH, nil)

			var contextPtr *map[string]interface{}
			if tt.additionalVariableContext != nil {
				contextPtr = &tt.additionalVariableContext
			}

			err := childVH.EvaluateAndSetMappingsToLocalVariables(
				tt.mappings,
				tt.evaluateExpressionFunc,
				contextPtr,
			)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedLocalVariables, childVH.LocalVariables())
			}
		})
	}
}

func TestVariableHolderPropagateOutputVariablesToParent(t *testing.T) {
	tests := []struct {
		name                   string
		parentVariables        map[string]interface{}
		childVariables         map[string]interface{}
		outputVariables        map[string]interface{}
		mappings               []extensions.TIoMapping
		evaluateExpressionFunc func(expression string, variableContext ...map[string]interface{}) (interface{}, error)
		expectedParentVars     map[string]interface{}
		expectedResult         map[string]interface{}
		expectedError          bool
	}{
		{
			name:                   "with nil parent returns nil",
			parentVariables:        nil,
			childVariables:         nil,
			outputVariables:        map[string]interface{}{"key": "value"},
			mappings:               []extensions.TIoMapping{},
			evaluateExpressionFunc: nil,
			expectedParentVars:     nil,
			expectedResult:         nil,
			expectedError:          false,
		},
		{
			name:                   "no mappings propagates all output variables",
			parentVariables:        map[string]interface{}{},
			childVariables:         map[string]interface{}{},
			outputVariables:        map[string]interface{}{"key1": "value1", "key2": "value2"},
			mappings:               []extensions.TIoMapping{},
			evaluateExpressionFunc: nil,
			expectedParentVars:     map[string]interface{}{"key1": "value1", "key2": "value2"},
			expectedResult:         map[string]interface{}{"key1": "value1", "key2": "value2"},
			expectedError:          false,
		},
		{
			name:            "with mappings evaluates expressions",
			parentVariables: map[string]interface{}{},
			childVariables:  map[string]interface{}{"childKey": "childValue"},
			outputVariables: map[string]interface{}{"outputKey": "outputValue"},
			mappings: []extensions.TIoMapping{
				{Source: "childKey", Target: "targetChild"},
				{Source: "outputKey", Target: "targetOutput"},
			},
			evaluateExpressionFunc: func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
				if len(variableContext) > 0 {
					if val, ok := variableContext[0][expression]; ok {
						return val, nil
					}
				}
				return nil, errors.New("key not found")
			},
			expectedParentVars: map[string]interface{}{
				"targetChild":  "childValue",
				"targetOutput": "outputValue",
			},
			expectedResult: map[string]interface{}{
				"targetChild":  "childValue",
				"targetOutput": "outputValue",
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parentVH *VariableHolder
			if tt.parentVariables != nil {
				parent := NewVariableHolder(nil, tt.parentVariables)
				parentVH = &parent
			}

			childVH := NewVariableHolder(parentVH, tt.childVariables)

			result, err := childVH.PropagateOutputVariablesToParent(
				tt.mappings,
				tt.outputVariables,
				tt.evaluateExpressionFunc,
			)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
				if parentVH != nil {
					assert.Equal(t, tt.expectedParentVars, parentVH.LocalVariables())
				}
			}
		})
	}
}

func TestMergeLocalVariablesWithOutputVariables(t *testing.T) {
	tests := []struct {
		name            string
		localVariables  map[string]interface{}
		outputVariables map[string]interface{}
		expectedMerged  map[string]interface{}
	}{
		{
			name:            "both maps empty",
			localVariables:  map[string]interface{}{},
			outputVariables: map[string]interface{}{},
			expectedMerged:  map[string]interface{}{},
		},
		{
			name:            "only local variables",
			localVariables:  map[string]interface{}{"key1": "value1"},
			outputVariables: map[string]interface{}{},
			expectedMerged:  map[string]interface{}{"key1": "value1"},
		},
		{
			name:            "only output variables",
			localVariables:  map[string]interface{}{},
			outputVariables: map[string]interface{}{"key2": "value2"},
			expectedMerged:  map[string]interface{}{"key2": "value2"},
		},
		{
			name:            "both maps with different keys",
			localVariables:  map[string]interface{}{"key1": "value1"},
			outputVariables: map[string]interface{}{"key2": "value2"},
			expectedMerged:  map[string]interface{}{"key1": "value1", "key2": "value2"},
		},
		{
			name:            "output variables override local variables",
			localVariables:  map[string]interface{}{"key": "localValue"},
			outputVariables: map[string]interface{}{"key": "outputValue"},
			expectedMerged:  map[string]interface{}{"key": "outputValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeLocalVariablesWithOutputVariables(tt.localVariables, tt.outputVariables)
			assert.Equal(t, tt.expectedMerged, result)
		})
	}
}

func TestVariableHolderSetLocalVariables(t *testing.T) {
	vh := NewVariableHolder(nil, nil)

	variables := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": 123,
	}

	vh.SetLocalVariables(variables)

	for key, value := range variables {
		assert.Equal(t, value, vh.GetLocalVariable(key))
	}
}

func TestEvaluateAndSetMappingsToLocalVariablesWithMergedContext(t *testing.T) {
	// This is a specific test to verify that the merged context is used correctly
	parentVH := NewVariableHolder(nil, map[string]interface{}{
		"parentVar": "parentValue",
	})

	childVH := NewVariableHolder(&parentVH, nil)

	additionalContext := map[string]interface{}{
		"additionalVar": "additionalValue",
	}

	mappings := []extensions.TIoMapping{
		{Source: "parentVar", Target: "mappedParent"},
		{Source: "additionalVar", Target: "mappedAdditional"},
	}

	// Mock evaluation function that checks the merged context
	evaluateExpressionFunc := func(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
		require.Len(t, variableContext, 1, "expected exactly one context parameter")

		context := variableContext[0]

		// Verify both parent and additional variables are available in the context
		if val, ok := context[expression]; ok {
			return val, nil
		}

		return nil, errors.New("expression not found in context")
	}

	err := childVH.EvaluateAndSetMappingsToLocalVariables(
		mappings,
		evaluateExpressionFunc,
		&additionalContext,
	)

	require.NoError(t, err)

	// Verify the mapped values are set correctly
	assert.Equal(t, "parentValue", childVH.GetLocalVariable("mappedParent"))
	assert.Equal(t, "additionalValue", childVH.GetLocalVariable("mappedAdditional"))
}
