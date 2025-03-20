package dmn

import "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"

type DecisionDefinition struct {
	definitions dmn.TDefinitions // parsed file content
	rawData     string           // the raw source data, compressed and encoded via ascii85
	checksum    [16]byte         // internal checksum to identify different versions
}

type EvaluatedDRDResult struct {
	EvaluatedDecisions []EvaluatedDecisionResult
	DecisionOutput     interface{}
}

type EvaluatedDecisionResult struct {
	tenantId        string
	decisionId      string
	decisionKey     string
	decisionName    string
	decisionType    string
	decisionVersion int
	matchedRules    []EvaluatedRule
	decisionOutput  interface{}
	evaluatedInputs []EvaluatedInput
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
