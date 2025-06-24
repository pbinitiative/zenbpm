package dmn

type EvaluatedDRDResult struct {
	EvaluatedDecisions []EvaluatedDecisionResult
	DecisionOutput     interface{}
}

type EvaluatedDecisionResult struct {
	tenantId                  string
	decisionId                string
	decisionName              string
	decisionType              string
	decisionDefinitionVersion int64
	decisionDefinitionKey     int64
	decisionDefinitionId      string
	matchedRules              []EvaluatedRule
	decisionOutput            interface{}
	evaluatedInputs           []EvaluatedInput
}

type EvaluatedRule struct {
	ruleId           string
	ruleIndex        int
	evaluatedOutputs []EvaluatedOutput
}

type EvaluatedOutput struct {
	outputId       string
	outputName     string
	outputJsonName string
	outputValue    interface{}
}

type EvaluatedInput struct {
	inputId         string
	inputName       string
	inputExpression string
	inputValue      interface{}
}
