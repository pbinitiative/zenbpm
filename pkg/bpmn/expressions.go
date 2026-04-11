package bpmn

import (
	"fmt"
	"strings"
)

func (engine *Engine) evaluateExpression(expression string, variableContext ...map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	// if does not start with then no need to evaluate
	if !strings.HasPrefix(expression, "=") {
		return expression, nil
	}
	expression = strings.TrimPrefix(expression, "=") // FIXME: this is just for convenience, but should be removed

	// Merge all variableContext maps into a single combined context
	mergedContext := make(map[string]interface{})
	for _, ctx := range variableContext {
		for k, v := range ctx {
			mergedContext[k] = v
		}
	}

	res, err := engine.feelRuntime.Evaluate(expression, mergedContext)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression %s with variables %s : %w", expression, mergedContext, err)
	}
	return res, nil
}
