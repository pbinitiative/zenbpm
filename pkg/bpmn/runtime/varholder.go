package runtime

import (
	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"maps"
	"strings"
)

type VariableHolder struct {
	parent         *VariableHolder
	localVariables map[string]interface{}
}

// NewVariableHolder creates a new VariableHolder with a given parent and localVariables map.
// All localVariables from parent holder are copied into current localVariables.
// (hint: the copy is necessary, due to the fact we need to have all localVariables for expression evaluation in one map)
func NewVariableHolder(parent *VariableHolder, variables map[string]interface{}) VariableHolder {
	if variables == nil {
		variables = make(map[string]interface{})
	}
	if parent != nil {
		maps.Copy(variables, parent.localVariables)
	}
	return VariableHolder{
		parent:         parent,
		localVariables: variables,
	}
}

func (vh *VariableHolder) LocalVariables() map[string]interface{} {
	return vh.localVariables
}

func (vh *VariableHolder) GetLocalVariable(key string) interface{} {
	if v, ok := vh.localVariables[key]; ok {
		return v
	}
	return nil
}

func (vh *VariableHolder) SetLocalVariable(key string, val interface{}) {
	vh.localVariables[key] = val
}

func (vh *VariableHolder) SetLocalVariables(variables map[string]interface{}) {
	for k, v := range variables {
		vh.localVariables[k] = v
	}
}

// PropagateVariable set a value with given key to the parent VariableHolder
func (vh *VariableHolder) PropagateVariable(key string, value interface{}) {
	if vh.parent != nil {
		vh.parent.SetLocalVariable(key, value)
	}
}

// PropagateLocalVariables propagates local variables to the parent VariableHolder according to mappings
func (vh *VariableHolder) PropagateLocalVariables(mappings []extensions.TIoMapping) error {
	if len(mappings) == 0 {
		vh.parent.SetLocalVariables(vh.localVariables)
	}

	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, vh.LocalVariables())
		if err != nil {
			return err
		}
		vh.parent.SetLocalVariable(mapping.Target, evalResult)
	}
	return nil
}

// EvaluateInputMappings sets local variables according to mappings
func (vh *VariableHolder) EvaluateInputMappings(mappings []extensions.TIoMapping) error {
	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, vh.LocalVariables())
		if err != nil {
			return err
		}
		vh.SetLocalVariable(mapping.Target, evalResult)
	}
	return nil
}

func evaluateExpression(expression string, variableContext map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	expression = strings.TrimPrefix(expression, "=") // FIXME: this is just for convenience, but should be removed
	res, err := feel.EvalStringWithScope(expression, variableContext)
	if err == nil {
		if num, ok := res.(*feel.Number); ok {
			// TODO: tbc: what about smart conversion to int, in case of integer value?
			return num.Float64(), nil
		}
		if b, ok := res.(bool); ok {
			return b, nil
		}
	}
	return res, err
}
