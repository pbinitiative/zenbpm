package dmn

import (
	"testing"

	dmnModel "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateAnyOutputAcceptsEquivalentNumericTypes(t *testing.T) {
	decisionTable := &dmnModel.TDecisionTable{
		Outputs: []dmnModel.TOutput{{Name: "result"}},
	}
	matchedRules := []EvaluatedRule{
		{EvaluatedOutputs: []EvaluatedOutput{{OutputJsonName: "result", OutputValue: int(10)}}},
		{EvaluatedOutputs: []EvaluatedOutput{{OutputJsonName: "result", OutputValue: float64(10)}}},
	}

	result, err := evaluateAnyOutput(decisionTable, "decision", matchedRules)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"decision": map[string]interface{}{"result": int(10)},
	}, result)
}

func TestEvaluatedOutputsEqualNormalizesNumericTypes(t *testing.T) {
	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		equal bool
	}{
		{
			name:  "equal signed integer and float",
			left:  int(10),
			right: float64(10),
			equal: true,
		},
		{
			name:  "equal unsigned and signed integers",
			left:  uint16(10),
			right: int64(10),
			equal: true,
		},
		{
			name:  "different numeric values",
			left:  int(10),
			right: float64(10.5),
			equal: false,
		},
		{
			name:  "negative signed integer and unsigned integer",
			left:  int64(-1),
			right: uint64(1),
			equal: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := []EvaluatedOutput{{OutputValue: test.left}}
			right := []EvaluatedOutput{{OutputValue: test.right}}

			assert.Equal(t, test.equal, evaluatedOutputsEqual(left, right))
		})
	}
}

func TestEvaluatedOutputsEqualNormalizesNestedNumericTypes(t *testing.T) {
	left := []EvaluatedOutput{{
		OutputValue: map[string]interface{}{
			"items": []interface{}{
				int(10),
				map[string]interface{}{"score": uint8(2)},
			},
		},
	}}
	right := []EvaluatedOutput{{
		OutputValue: map[string]interface{}{
			"items": []interface{}{
				float64(10),
				map[string]interface{}{"score": float32(2)},
			},
		},
	}}

	assert.True(t, evaluatedOutputsEqual(left, right))
}

func TestEvaluatedOutputsEqualDetectsDifferentNestedNumericValues(t *testing.T) {
	left := []EvaluatedOutput{{
		OutputValue: map[string]interface{}{
			"items": []interface{}{int(10)},
		},
	}}
	right := []EvaluatedOutput{{
		OutputValue: map[string]interface{}{
			"items": []interface{}{float64(11)},
		},
	}}

	assert.False(t, evaluatedOutputsEqual(left, right))
}
