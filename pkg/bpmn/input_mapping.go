package bpmn

import "github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"

// Applies input mappings against a frozen copy of `scope` and writes each result
// back. Mappings must be deterministic over `scope` (no now()/external state —
// replayed values must match the original execution) and must not see each other's
// targets; the freeze is intentional, do not read from `scope` inside the loop.
func (engine *Engine) evaluateInputMappingsAgainstScope(scope map[string]any, mappings []extensions.TIoMapping) error {
	evaluationContext := make(map[string]any, len(scope))
	for key, value := range scope {
		evaluationContext[key] = value
	}

	for _, mapping := range mappings {
		value, err := engine.evaluateExpression(mapping.Source, evaluationContext)
		if err != nil {
			return err
		}
		scope[mapping.Target] = value
	}
	return nil
}
