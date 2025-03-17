package dmn

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"os"
	"strings"
)

type DmnEngine interface {
	LoadFromFile(ctx context.Context, filename string) (*DecisionDefinition, error)
	EvaluateDRD(ctx context.Context, dmnDefinition *DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error)
	EvaluateDecision(ctx context.Context, dmnDefinition *DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error)
	Validate(ctx context.Context, dmnDefinition *DecisionDefinition) error
}

type ZenDmnEngine struct {
}

// New creates a new instance of the BPMN Engine;
func New() DmnEngine {
	return &ZenDmnEngine{}
}

func (engine *ZenDmnEngine) LoadFromFile(ctx context.Context, filename string) (*DecisionDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load dmn definition from file: %v, %w", filename, err)
	}
	return engine.load(ctx, xmlData, filename)
}

func (engine *ZenDmnEngine) load(ctx context.Context, xmlData []byte, resourceName string) (*DecisionDefinition, error) {
	md5sum := md5.Sum(xmlData)
	var definitions dmn.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dmn definition from file: %v, %w", resourceName, err)
	}

	dmnDefinition := DecisionDefinition{
		definitions: definitions,
		checksum:    md5sum,
	}

	return &dmnDefinition, engine.Validate(ctx, &dmnDefinition)
}

func (engine *ZenDmnEngine) Validate(ctx context.Context, dmnDefinition *DecisionDefinition) error {
	// TODO: Implement validation - Cyclic Requirements, unique ids, etc.
	return nil
}

func (engine *ZenDmnEngine) EvaluateDRD(ctx context.Context, dmnDefinition *DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error) {
	result, dependencies, err := engine.EvaluateDecision(ctx, dmnDefinition, decisionId, inputVariableContext)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate decision: %v, %w", decisionId, err)
	}

	evaluatedDecisions := append([]EvaluatedDecisionResult{result}, dependencies...)

	return &EvaluatedDRDResult{
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     result.decisionOutput,
	}, nil
}

func (engine *ZenDmnEngine) EvaluateDecision(ctx context.Context, dmnDefinition *DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error) {
	foundDecision := findDecision(dmnDefinition, decisionId)
	if foundDecision == nil {
		return EvaluatedDecisionResult{}, nil, &DecisionNotFoundError{DecisionID: decisionId}
	}

	evaluatedDependencies := make([]EvaluatedDecisionResult, 0)

	// Create localVariableContext and copy values from variableContext
	localVariableContext := make(map[string]interface{})
	for key, value := range inputVariableContext {
		localVariableContext[key] = value
	}

	for _, requirement := range foundDecision.InformationRequirement {
		requiredDecisionRef := requirement.RequiredDecision.Href
		var requiredDecision string
		requiredDecision = strings.TrimPrefix(requiredDecisionRef, "#")

		result, dependencies, err := engine.EvaluateDecision(ctx, dmnDefinition, requiredDecision, inputVariableContext)

		if err != nil {
			return result, dependencies, err
		}

		localVariableContext[result.decisionId] = result.decisionOutput
		evaluatedDependencies = append(evaluatedDependencies, result)
		evaluatedDependencies = append(evaluatedDependencies, dependencies...)
	}

	decisionTable := foundDecision.DecisionTable
	evaluatedInputs := make([]EvaluatedInput, len(decisionTable.Inputs))

	for i, input := range decisionTable.Inputs {

		value, _ := feel.EvalStringWithScope(input.InputExpression.Text, localVariableContext)
		evaluatedInputs[i] = EvaluatedInput{
			inputId:         input.Id,
			inputName:       input.Label,
			inputExpression: input.InputExpression.Text,
			inputValue:      value,
		}
	}

	matchedRules := make([]EvaluatedRule, 0)

	for ruleIndex, rule := range decisionTable.Rules {
		allColumnsMatch := true
		for i, inputEntry := range rule.InputEntry {
			inputInstance := evaluatedInputs[i]
			match, _ := EvaluateCellMatch(inputInstance.inputExpression, inputEntry.Text, localVariableContext)

			if !match {
				allColumnsMatch = false
				break
			}
		}

		if allColumnsMatch {
			evaluatedOutputs := make([]EvaluatedOutput, len(decisionTable.Outputs))
			for i, output := range decisionTable.Outputs {
				value, expressionError := feel.EvalStringWithScope(rule.OutputEntry[i].Text, localVariableContext)

				if expressionError != nil {
					return EvaluatedDecisionResult{}, nil, expressionError
				}

				evaluatedOutputs[i] = EvaluatedOutput{
					outputId:       output.Id,
					outputName:     output.Label,
					outputJsonName: output.Name,
					outputValue:    value,
				}
			}
			matchedRules = append(matchedRules, EvaluatedRule{
				ruleId:           rule.Id,
				ruleIndex:        ruleIndex + 1,
				evaluatedOutputs: evaluatedOutputs,
			})
			if foundDecision.DecisionTable.HitPolicy == dmn.HitPolicyFirst {
				break
			}
		}
	}

	return EvaluatedDecisionResult{
		tenantId:        "<default>", //TODO: Fill out tenantId. TenantId makes sense only in
		decisionId:      decisionId,
		decisionKey:     "<default>", //TODO: Fill out decisionKey. Will get implemented with persistence
		decisionName:    "<default>", //TODO: Fill out decisionName. Will get implemented with persistence
		decisionType:    "<default>", //TODO: Fill out decisionType. Will get implemented with persistence
		decisionVersion: 0,           //TODO: Fill out decisionVersion. Will get implemented with persistence
		matchedRules:    matchedRules,
		evaluatedInputs: evaluatedInputs,
		decisionOutput:  EvaluateHitPolicyOutput(foundDecision.DecisionTable.HitPolicy, foundDecision.DecisionTable.HitPolicyAggregation, matchedRules),
	}, evaluatedDependencies, nil

}
