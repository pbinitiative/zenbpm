package script

type FeelRuntime interface {
	UnaryTest(expression string, variableContext map[string]any) (bool, error)
	Evaluate(expression string, variableContext map[string]any) (any, error)
	Stop()
}

type JsRuntime interface {
	RunScript(script string) (any, error)
	Stop()
}
