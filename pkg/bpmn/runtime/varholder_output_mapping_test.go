package runtime

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/stretchr/testify/require"
)

func TestVariableHolderOutputMappings(t *testing.T) {
	t.Run("Does not propagate partial values when a later mapping fails", func(t *testing.T) {
		parent := NewVariableHolder(nil, map[string]interface{}{
			"value": "initial",
		})
		child := NewVariableHolder(&parent, nil)
		evaluationError := errors.New("invalid expression")

		mappedVariables, err := child.PropagateMappedOutputsOrAll(
			[]extensions.TIoMapping{
				{Source: "valid", Target: "value"},
				{Source: "invalid", Target: "unused"},
			},
			nil,
			func(expression string, variables map[string]interface{}) (interface{}, error) {
				switch expression {
				case "valid":
					return variables["value"].(string) + "-mapped", nil
				case "invalid":
					require.Equal(t, "initial", parent.GetLocalVariable("value"))
					return nil, evaluationError
				default:
					return nil, errors.New("unexpected expression")
				}
			},
		)

		require.ErrorIs(t, err, evaluationError)
		require.Nil(t, mappedVariables)
		require.Equal(t, map[string]interface{}{
			"value": "initial",
		}, parent.LocalVariables())
	})

	t.Run("Propagates all values after every mapping succeeds", func(t *testing.T) {
		parent := NewVariableHolder(nil, map[string]interface{}{
			"source": "input",
		})
		child := NewVariableHolder(&parent, nil)

		mappedVariables, err := child.PropagateMappedOutputsOrAll(
			[]extensions.TIoMapping{
				{Source: "first", Target: "first_output"},
				{Source: "second", Target: "second_output"},
			},
			nil,
			func(expression string, _ map[string]interface{}) (interface{}, error) {
				return expression + "-value", nil
			},
		)

		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{
			"first_output":  "first-value",
			"second_output": "second-value",
		}, mappedVariables)
		require.Equal(t, map[string]interface{}{
			"source":        "input",
			"first_output":  "first-value",
			"second_output": "second-value",
		}, parent.LocalVariables())
	})
}
