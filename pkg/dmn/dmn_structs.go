// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package dmn

type EvaluatedDRDResult struct {
	EvaluatedDecisions []EvaluatedDecisionResult
	DecisionOutput     map[string]interface{}
}

type EvaluatedDecisionResult struct {
	DecisionId                string
	DecisionName              string
	DecisionType              string
	DecisionDefinitionVersion int64
	DecisionDefinitionKey     int64
	DecisionDefinitionId      string
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
