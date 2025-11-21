package bpmn

import (
	"strings"

	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

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

func evaluateMappings(variables map[string]interface{}, mappings []extensions.TIoMapping) (map[string]interface{}, error) {
	resultVariables := make(map[string]interface{}, len(mappings))
	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, variables)
		if err != nil {
			return nil, err
		}
		resultVariables[mapping.Target] = evalResult
	}
	return resultVariables, nil
}

func evaluateLocalVariables(varHolder *runtime.VariableHolder, mappings []extensions.TIoMapping) error {
	return mapVariables(varHolder, mappings, func(key string, value interface{}) {
		varHolder.SetVariable(key, value)
	})
}

func propagateProcessInstanceVariables(varHolder *runtime.VariableHolder, mappings []extensions.TIoMapping) error {
	if len(mappings) == 0 {
		for k, v := range varHolder.Variables() {
			varHolder.PropagateVariable(k, v)
		}
	}
	return mapVariables(varHolder, mappings, func(key string, value interface{}) {
		varHolder.PropagateVariable(key, value)
	})
}
