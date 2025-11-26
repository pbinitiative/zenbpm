package bpmn

import (
	"strings"

	"github.com/pbinitiative/feel"
)

func (engine *Engine) evaluateExpression(expression string, variableContext map[string]interface{}) (interface{}, error) {
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
