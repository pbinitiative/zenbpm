package bpmn

import (
	"fmt"
	"strings"
)

func (engine *Engine) evaluateExpression(expression string, variableContext map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	expression = strings.TrimPrefix(expression, "=") // FIXME: this is just for convenience, but should be removed
	res, err := engine.feelRuntime.Evaluate(expression, variableContext)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression %s with variables %s : %w", expression, variableContext, err)
	}
	return res, nil
}
