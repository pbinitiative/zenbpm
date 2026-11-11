package runtime

import (
	"maps"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

// ElementPhase is where an element instance stands between the moment a token
// enters the element and the moment it leaves.
type ElementPhase int

const (
	// ElementPhaseActivating: the element is entered, its input mappings are
	// evaluated, and its start listeners run.
	ElementPhaseActivating ElementPhase = iota + 1
	// ElementPhaseActive: the element's own work runs, such as the job of a task.
	ElementPhaseActive
	// ElementPhaseCompleting: the element's own work is done, its output
	// mappings are applied, and its end listeners run.
	ElementPhaseCompleting
)

// ElementScope is what an element instance keeps while a token stays in it
// across several jobs: where it stands, and the variables local to it. The
// engine stores one only for an element which carries execution listeners;
// every other element is entered and left without one.
//
// The local variables are kept apart from the instance's: a job of the
// element is handed a copy of the instance's variables as they are then, under
// the element's own (VariableHolder), so a variable the instance gains while
// the token waits in the element is seen by the element's next job.
type ElementScope struct {
	ElementInstanceKey int64
	ProcessInstanceKey int64
	Phase              ElementPhase
	// ListenerIndex is the position, among the listeners of the phase, of the
	// listener whose job is created next or waits; it equals their number
	// once all of them ran.
	ListenerIndex int
	// InputVariables holds the results of the element's input mappings, by
	// target. They are evaluated again when an incident of the element is
	// resolved.
	InputVariables map[string]any
	// LocalVariables holds the variables the jobs of the element were
	// completed with: those of its listeners, and of its own job once it is
	// done. They are never evaluated again, and they hide an input of the same
	// name.
	LocalVariables map[string]any
}

// NewElementScope answers the scope of the element instance the token is at,
// before anything happened in it.
func NewElementScope(token ExecutionToken) ElementScope {
	return ElementScope{
		ElementInstanceKey: token.ElementInstanceKey,
		ProcessInstanceKey: token.ProcessInstanceKey,
		Phase:              ElementPhaseActivating,
		InputVariables:     map[string]any{},
		LocalVariables:     map[string]any{},
	}
}

// ApplyInputMappings evaluates the element's input mappings against the
// parent's variables, and keeps their results in place of earlier ones. The
// local variables are no part of what a mapping reads: input mappings come
// before every listener.
func (scope *ElementScope) ApplyInputMappings(
	parent *VariableHolder,
	mappings []extensions.TIoMapping,
	evaluateExpression func(expression string, variableContext map[string]interface{}) (interface{}, error),
) error {
	mapped := NewVariableHolder(parent, nil)
	if err := mapped.EvaluateAndSetMappingsToLocalVariables(mappings, evaluateExpression); err != nil {
		return err
	}
	scope.InputVariables = make(map[string]any, len(mappings))
	for _, mapping := range mappings {
		scope.InputVariables[mapping.Target] = mapped.GetLocalVariable(mapping.Target)
	}
	return nil
}

// MergeLocalVariables adds the variables a job of the element was completed
// with; a later job's variable replaces an earlier one of the same name.
func (scope *ElementScope) MergeLocalVariables(variables map[string]any) {
	if scope.LocalVariables == nil {
		scope.LocalVariables = make(map[string]any, len(variables))
	}
	maps.Copy(scope.LocalVariables, variables)
}

// VariableHolder answers the variables a job of the element is handed: a copy
// of the parent's as they are now, under the results of the input mappings,
// under the local variables.
func (scope ElementScope) VariableHolder(parent *VariableHolder) VariableHolder {
	holder := NewVariableHolder(parent, nil)
	holder.SetLocalVariables(scope.InputVariables)
	holder.SetLocalVariables(scope.LocalVariables)
	return holder
}
