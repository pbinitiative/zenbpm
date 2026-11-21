package runtime

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestANewElementScopeIsActivatingAndEmpty(t *testing.T) {
	scope := NewElementScope(ExecutionToken{ElementInstanceKey: 7, ProcessInstanceKey: 3})

	assert.Equal(t, ElementScope{
		ElementInstanceKey: 7, ProcessInstanceKey: 3, Phase: ElementPhaseActivating,
		InputVariables: map[string]any{}, LocalVariables: map[string]any{},
	}, scope)
}

func TestInputMappingsAreEvaluatedAgainstTheParentOnly(t *testing.T) {
	parent := NewVariableHolder(nil, map[string]any{"order": "o-1", "untouched": true})
	scope := NewElementScope(ExecutionToken{})
	scope.MergeLocalVariables(map[string]any{"order": "from-a-listener"})

	err := scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=order", Target: "mapped"}}, lookUpVariable)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"mapped": "o-1"}, scope.InputVariables, "only the targets of the mappings, read from the parent")
	assert.Equal(t, map[string]any{"order": "o-1", "untouched": true}, parent.LocalVariables(), "the parent is left alone")
}

func TestInputMappingsEvaluatedAgainReplaceTheEarlierResults(t *testing.T) {
	parent := NewVariableHolder(nil, map[string]any{"order": "o-1"})
	scope := NewElementScope(ExecutionToken{})
	require.NoError(t, scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=order", Target: "mapped"}}, lookUpVariable))
	scope.MergeLocalVariables(map[string]any{"fromListener": 1})

	parent.SetLocalVariable("order", "o-2")
	require.NoError(t, scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=order", Target: "mapped"}}, lookUpVariable))

	assert.Equal(t, map[string]any{"mapped": "o-2"}, scope.InputVariables)
	assert.Equal(t, map[string]any{"fromListener": 1}, scope.LocalVariables, "what the jobs added stays")
}

func TestAFailingInputMappingKeepsTheEarlierResults(t *testing.T) {
	parent := NewVariableHolder(nil, map[string]any{"order": "o-1"})
	scope := NewElementScope(ExecutionToken{})
	require.NoError(t, scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=order", Target: "mapped"}}, lookUpVariable))

	err := scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=missing", Target: "mapped"}}, lookUpVariable)

	require.Error(t, err)
	assert.Equal(t, map[string]any{"mapped": "o-1"}, scope.InputVariables)
}

func TestAJobOfTheElementSeesTheParentNowUnderInputsUnderLocals(t *testing.T) {
	parent := NewVariableHolder(nil, map[string]any{"a": "parent", "b": "parent", "c": "parent"})
	scope := NewElementScope(ExecutionToken{})
	require.NoError(t, scope.ApplyInputMappings(&parent, []extensions.TIoMapping{{Source: "=a", Target: "b"}, {Source: "=a", Target: "c"}}, lookUpVariable))
	scope.MergeLocalVariables(map[string]any{"c": "first job"})
	scope.MergeLocalVariables(map[string]any{"c": "second job", "d": "second job"})
	parent.SetLocalVariable("a", "changed while the token waits")
	parent.SetLocalVariable("e", "added while the token waits")

	holder := scope.VariableHolder(&parent)

	assert.Equal(t, map[string]any{
		"a": "changed while the token waits",
		"b": "parent",
		"c": "second job",
		"d": "second job",
		"e": "added while the token waits",
	}, holder.LocalVariables())
	holder.SetLocalVariable("a", "changed in the job's copy")
	assert.Equal(t, "changed while the token waits", parent.GetLocalVariable("a"), "the job's variables are a copy")
}

func TestMergingIntoAScopeReadWithoutLocalsWorks(t *testing.T) {
	scope := ElementScope{}

	scope.MergeLocalVariables(map[string]any{"a": 1})

	assert.Equal(t, map[string]any{"a": 1}, scope.LocalVariables)
}

// lookUpVariable evaluates "=name" to the variable of that name, and fails
// where there is none.
func lookUpVariable(expression string, variables map[string]any) (any, error) {
	value, ok := variables[expression[1:]]
	if !ok {
		return nil, errors.New("no variable " + expression[1:])
	}
	return value, nil
}
