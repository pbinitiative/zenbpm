package dmn

import (
	"crypto/md5"
	"encoding/xml"
	"errors"
	"github.com/antonmedv/expr"
	"github.com/pbinitiative/zenbpm/pkg/storage/dmn"
	"os"
	"strings"
)

type DmnEngine interface {
	LoadFromFile(filename string) (*DmnDefinition, error)
	EvaluateDRD(dmnDefinition *DmnDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error)
	EvaluateDecision(dmnDefinition *DmnDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDecisionResult, *[]EvaluatedDecisionResult, error)
	Validate(dmnDefinition *DmnDefinition) error
}

type ZenDmnEngine struct {
}

// New creates a new instance of the BPMN Engine;
func New() DmnEngine {
	return &ZenDmnEngine{}
}

func (engine *ZenDmnEngine) LoadFromFile(filename string) (*DmnDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return engine.load(xmlData, filename)
}

func (engine *ZenDmnEngine) load(xmlData []byte, resourceName string) (*DmnDefinition, error) {
	md5sum := md5.Sum(xmlData)
	var definitions dmn.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, err
	}

	dmnDefinition := DmnDefinition{
		definitions: definitions,
		checksum:    md5sum,
	}

	return &dmnDefinition, engine.Validate(&dmnDefinition)
}

func (engine *ZenDmnEngine) Validate(dmnDefinition *DmnDefinition) error {
	// TODO: Implement validation - Cyclic Requirements, unique ids, etc.
	return nil
}

func (engine *ZenDmnEngine) EvaluateDRD(dmnDefinition *DmnDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error) {
	result, dependencies, err := engine.EvaluateDecision(dmnDefinition, decisionId, inputVariableContext)
	if err != nil {
		return nil, err
	}

	evaluatedDecisions := append([]EvaluatedDecisionResult{*result}, *dependencies...)

	return &EvaluatedDRDResult{
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     result.decisionOutput,
	}, nil
}

func (engine *ZenDmnEngine) EvaluateDecision(dmnDefinition *DmnDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDecisionResult, *[]EvaluatedDecisionResult, error) {
	foundDecision := findDecision(dmnDefinition, decisionId)
	if foundDecision == nil {
		return nil, nil, errors.New("Can't evaluate decision. Decision [" + decisionId + "] does not exist.")
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
		if strings.HasPrefix(requiredDecisionRef, "#") {
			requiredDecision = requiredDecisionRef[1:]
		} else {
			requiredDecision = requiredDecisionRef
		}

		result, dependencies, err := engine.EvaluateDecision(dmnDefinition, requiredDecision, inputVariableContext)

		if err != nil {
			return result, dependencies, err
		}

		localVariableContext[result.decisionId] = result.decisionOutput
		evaluatedDependencies = append(evaluatedDependencies, *result)
		evaluatedDependencies = append(evaluatedDependencies, *dependencies...)
	}

	decisionTable := foundDecision.DecisionTable
	evaluatedInputs := make([]EvaluatedInput, len(decisionTable.Inputs))

	for i, input := range decisionTable.Inputs {
		value, _ := expr.Eval(input.InputExpression.Text, localVariableContext)
		evaluatedInputs[i] = EvaluatedInput{
			inputId:    input.Id,
			inputName:  input.Label,
			inputValue: value,
		}
	}

	matchedRules := make([]EvaluatedRule, 0)

	for ruleIndex, rule := range decisionTable.Rules {
		allColumnsMatch := true
		for i, inputEntry := range rule.InputEntry {
			inputInstance := evaluatedInputs[i]
			if inputEntry.Text == "" {
				// If the text is empty, it means any value is accepted
				continue
			}
			value, _ := expr.Eval(inputEntry.Text, localVariableContext)
			if value != inputInstance.inputValue {
				allColumnsMatch = false
				break
			}
		}

		if allColumnsMatch {
			evaluatedOutputs := make([]EvaluatedOutput, len(decisionTable.Outputs))
			for i, output := range decisionTable.Outputs {
				value, expressionError := expr.Eval(rule.OutputEntry[i].Text, localVariableContext)

				if expressionError != nil {
					return nil, nil, expressionError
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

	return &EvaluatedDecisionResult{
		tenantId:        "<default>", //TODO: Fill out tenantId
		decisionId:      decisionId,
		decisionKey:     "<default>", //TODO: Fill out decisionKey
		decisionName:    "<default>", //TODO: Fill out decisionName
		decisionType:    "<default>", //TODO: Fill out decisionType
		decisionVersion: 0,           //TODO: Fill out decisionVersion
		matchedRules:    matchedRules,
		evaluatedInputs: evaluatedInputs,
		decisionOutput:  EvaluateHitPolicyOutput(foundDecision.DecisionTable.HitPolicy, foundDecision.DecisionTable.HitPolicyAggregation, matchedRules),
	}, &evaluatedDependencies, nil

}
