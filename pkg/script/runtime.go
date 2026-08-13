package script

type FeelRuntime interface {
	UnaryTest(expression string, variableContext map[string]any) (bool, error)
	Evaluate(expression string, variableContext map[string]any) (any, error)
	Stop()
}

// StrictFeelRuntime can report references to variables that are missing from
// the runtime context instead of evaluating them as null.
type StrictFeelRuntime interface {
	UnaryTestStrict(expression string, variableContext map[string]any) (bool, error)
}

// FeelExpressionValidator can validate FEEL expression syntax without
// evaluating the expression against a runtime context.
type FeelExpressionValidator interface {
	ValidateExpression(expression string) error
}

type JsRuntime interface {
	RunScript(script string) (any, error)
	Stop()
}
