package dmn

type EvaluatedDRDResult struct {
	EvaluatedDecisions  []EvaluatedDecisionResult
	DecisionOutput      interface{}
	DecisionInstanceKey int64
}

type EvaluatedDecisionResult struct {
	DecisionId                string
	DecisionName              string
	DecisionType              string
	DecisionDefinitionVersion int64
	DecisionDefinitionKey     int64
	DmnResourceDefinitionId   string
	MatchedRules              []EvaluatedRule
	DecisionOutput            map[string]interface{}
	EvaluatedInputs           []EvaluatedInput
}

type EvaluatedRule struct {
	RuleId           string
	RuleIndex        int
	EvaluatedOutputs []EvaluatedOutput
}

type EvaluatedOutput struct {
	OutputId       string
	OutputName     string
	OutputJsonName string
	OutputValue    interface{}
}

type EvaluatedInput struct {
	InputId         string
	InputName       string
	InputExpression string
	InputValue      interface{}
}
