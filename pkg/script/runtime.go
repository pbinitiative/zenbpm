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

// FeelUnaryTestValidator can validate FEEL unary test syntax without
// evaluating the unary test against a runtime context.
type FeelUnaryTestValidator interface {
	ValidateUnaryTest(expression string) error
}

// DmnFeelRuntime is the complete FEEL runtime contract required to validate
// and evaluate DMN decision tables safely. Keeping these capabilities separate
// from FeelRuntime preserves source compatibility for runtimes used only for
// expression evaluation, while DMN engines can reject incomplete runtimes
// instead of silently falling back to non-strict or execution-based behavior.
type DmnFeelRuntime interface {
	FeelRuntime
	StrictFeelRuntime
	FeelExpressionValidator
	FeelUnaryTestValidator
}

type JsRuntime interface {
	RunScript(script string) (any, error)
	Stop()
}
