package runtime

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

type VariableHolder struct {
	parent         *VariableHolder
	localVariables map[string]interface{}
}

// NewVariableHolder creates a new VariableHolder with a given parent and localVariables map.
// If localVariables are not specified all parent.localVariables holder are copied into current localVariables.
func NewVariableHolder(parent *VariableHolder, localVariables map[string]interface{}) VariableHolder {
	if localVariables == nil {
		localVariables = make(map[string]interface{})
		if parent != nil {
			for k, v := range parent.localVariables {
				localVariables[k] = v
			}
		}
	}

	return VariableHolder{
		parent:         parent,
		localVariables: localVariables,
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
// uses a replaceable evaluateExpression() function eg. engine.evaluateExpression()
func (vh *VariableHolder) PropagateLocalVariables(mappings []extensions.TIoMapping, evaluateExpression func(expression string, variableContext map[string]interface{}) (interface{}, error)) error {
	if vh.parent == nil {
		return nil
	}

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

// EvaluateAndSetInputMappings sets local variables according to mappings
// uses a replaceable evaluateExpression() function eg. engine.evaluateExpression()
func (vh *VariableHolder) EvaluateAndSetInputMappings(mappings []extensions.TIoMapping, evaluateExpression func(expression string, variableContext map[string]interface{}) (interface{}, error)) error {
	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, vh.LocalVariables())
		if err != nil {
			return err
		}
		vh.SetLocalVariable(mapping.Target, evalResult)
	}
	return nil
}
