package dmn

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"os"
	"strings"
)

type ZenDmnEngine struct {
	persistence storage.DecisionStorage
}

type EngineOption = func(*ZenDmnEngine)

// NewEngine creates a new instance of the BPMN Engine;
func NewEngine(options ...EngineOption) *ZenDmnEngine {
	engine := ZenDmnEngine{
		persistence: nil,
	}

	for _, option := range options {
		option(&engine)
	}

	return &engine
}

func EngineWithStorage(persistence storage.DecisionStorage) EngineOption {
	return func(engine *ZenDmnEngine) {
		engine.persistence = persistence
	}
}

func (engine *ZenDmnEngine) LoadFromFile(ctx context.Context, filename string) (*runtime.DecisionDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load dmn definition from file: %v, %w", filename, err)
	}
	return engine.load(ctx, xmlData, filename)
}

func (engine *ZenDmnEngine) LoadFromBytes(ctx context.Context, xmlData []byte) (*runtime.DecisionDefinition, error) {
	return engine.load(ctx, xmlData, "")
}

func (engine *ZenDmnEngine) load(ctx context.Context, xmlData []byte, resourceName string) (*runtime.DecisionDefinition, error) {
	md5sum := md5.Sum(xmlData)
	var definitions dmn.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse decision definition from file: %v, %w", resourceName, err)
	}

	dmnDefinition := runtime.DecisionDefinition{
		Version:         1,
		Id:              definitions.Id,
		Key:             engine.generateKey(),
		Definitions:     definitions,
		RawData:         xmlData,
		DmnChecksum:     md5sum,
		DmnResourceName: resourceName,
	}

	decisionDefinitions, err := engine.persistence.FindDecisionDefinitionsById(context.TODO(), definitions.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to load decision definition by id %s: %w", definitions.Id, err)
	}
	if len(decisionDefinitions) > 0 {
		latestIndex := len(decisionDefinitions) - 1
		if decisionDefinitions[latestIndex].DmnChecksum == md5sum {
			return &decisionDefinitions[latestIndex], nil
		}
		dmnDefinition.Version = decisionDefinitions[latestIndex].Version + 1
	}
	err = engine.persistence.SaveDecisionDefinition(context.TODO(), dmnDefinition)
	if err != nil {
		return nil, fmt.Errorf("failed to save decision definition by id %s: %w", definitions.Id, err)
	}

	return &dmnDefinition, engine.Validate(ctx, &dmnDefinition)
}

func (engine *ZenDmnEngine) generateKey() int64 {
	return engine.persistence.GenerateId()
}

func (engine *ZenDmnEngine) Validate(ctx context.Context, dmnDefinition *runtime.DecisionDefinition) error {
	// TODO: Implement validation - Cyclic Requirements, unique ids, etc.
	return nil
}

func (engine *ZenDmnEngine) EvaluateDRD(ctx context.Context, dmnDefinition *runtime.DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error) {
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

func (engine *ZenDmnEngine) EvaluateDecision(ctx context.Context, dmnDefinition *runtime.DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error) {
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
		decisionId:                foundDecision.Id,
		decisionName:              foundDecision.Name,
		decisionType:              "<default>",
		decisionDefinitionVersion: dmnDefinition.Version,
		decisionDefinitionKey:     dmnDefinition.Key,
		decisionDefinitionId:      dmnDefinition.Id,
		matchedRules:              matchedRules,
		evaluatedInputs:           evaluatedInputs,
		decisionOutput:            EvaluateHitPolicyOutput(foundDecision.DecisionTable.HitPolicy, foundDecision.DecisionTable.HitPolicyAggregation, matchedRules),
	}, evaluatedDependencies, nil

}
