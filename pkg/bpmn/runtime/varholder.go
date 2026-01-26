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

func (vh *VariableHolder) DeleteLocalVariable(key string) {
	delete(vh.localVariables, key)
}

func (vh *VariableHolder) SetLocalVariables(variables map[string]interface{}) {
	for k, v := range variables {
		vh.localVariables[k] = v
	}
}

// EvaluateAndSetMappingsToLocalVariables sets local variables according to mappings
// uses a replaceable evaluateExpression() function eg. engine.evaluateExpression()
func (vh *VariableHolder) EvaluateAndSetMappingsToLocalVariables(mappings []extensions.TIoMapping, evaluateExpression func(expression string, variableContext map[string]interface{}) (interface{}, error)) error {
	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, vh.parent.LocalVariables())
		if err != nil {
			return err
		}
		vh.SetLocalVariable(mapping.Target, evalResult)
	}
	return nil
}

// PropagateVariable set a value with given key to the parent VariableHolder
func (vh *VariableHolder) PropagateVariable(key string, value interface{}) {
	if vh.parent != nil {
		vh.parent.SetLocalVariable(key, value)
	}
}

// PropagateVariables set a values with given keys to the parent VariableHolder
func (vh *VariableHolder) PropagateVariables(variables map[string]interface{}) {
	if vh.parent != nil {
		for k, v := range variables {
			vh.parent.SetLocalVariable(k, v)
		}
	}
}

// PropagateOutputVariablesToParent propagates local variables to the parent VariableHolder according to mappings
// uses a replaceable evaluateExpression() function eg. engine.evaluateExpression()
func (vh *VariableHolder) PropagateOutputVariablesToParent(mappings []extensions.TIoMapping, outputVariables map[string]any, evaluateExpression func(expression string, variableContext map[string]interface{}) (interface{}, error)) (map[string]any, error) {
	if vh.parent == nil {
		return nil, nil
	}

	if len(mappings) == 0 {
		vh.parent.SetLocalVariables(outputVariables)
		return outputVariables, nil
	}

	localScope := mergeLocalVariablesWithOutputVariables(vh.LocalVariables(), outputVariables)
	outputVariablesWithOutputMappings := make(map[string]interface{})

	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, localScope)
		if err != nil {
			return nil, err
		}
		outputVariablesWithOutputMappings[mapping.Target] = evalResult
		vh.parent.SetLocalVariable(mapping.Target, evalResult)
	}
	return outputVariablesWithOutputMappings, nil
}

func mergeLocalVariablesWithOutputVariables(localVariables map[string]any, outputVariables map[string]any) map[string]any {
	localScope := make(map[string]any)
	for k, v := range localVariables {
		localScope[k] = v
	}
	for k, v := range outputVariables {
		localScope[k] = v
	}
	return localScope
}
