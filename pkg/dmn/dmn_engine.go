package dmn

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"os"
	"strings"

	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
)

type ZenDmnEngine struct {
	persistence storage.DecisionStorage
}

type EngineOption = func(*ZenDmnEngine)

// NewEngine creates a new instance of the DMN Engine;
func NewEngine(options ...EngineOption) *ZenDmnEngine {
	engine := ZenDmnEngine{
		persistence: inmemory.NewStorage(),
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

func (engine *ZenDmnEngine) ParseDmnFromFile(filename string) (*dmn.TDefinitions, []byte, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load dmn definition from file: %v, %w", filename, err)
	}
	definition, err := engine.ParseDmnFromBytes("", xmlData)
	if err != nil {
		return nil, xmlData, err
	}
	return definition, xmlData, nil
}

func (engine *ZenDmnEngine) ParseDmnFromBytes(resourceName string, xmlData []byte) (*dmn.TDefinitions, error) {
	var definition dmn.TDefinitions
	err := xml.Unmarshal(xmlData, &definition)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dmn resource definition from bytes: %v, %w", resourceName, err)
	}
	return &definition, nil
}

func (engine *ZenDmnEngine) SaveDmnResourceDefinition(
	ctx context.Context,
	resourceName string,
	definition dmn.TDefinitions,
	xmlData []byte,
	key int64,
) (*runtime.DmnResourceDefinition, []runtime.DecisionDefinition, error) {
	md5sum := md5.Sum(xmlData)
	if resourceName == "" {
		resourceName = definition.Name
	}
	dmnResourceDefinition := runtime.DmnResourceDefinition{
		Version:         1,
		Id:              definition.Id,
		Key:             key,
		Definitions:     definition,
		DmnData:         xmlData,
		DmnChecksum:     md5sum,
		DmnResourceName: resourceName,
	}
	decisionDefinitions := make([]runtime.DecisionDefinition, 0)
	for _, definition := range definition.Decisions {
		decisionDefinition := runtime.DecisionDefinition{
			Version:                  1,
			Id:                       definition.Id,
			VersionTag:               definition.VersionTag.Value,
			DecisionDefinitionId:     dmnResourceDefinition.Id,
			DmnResourceDefinitionKey: dmnResourceDefinition.Key,
		}
		decisionDefinitions = append(decisionDefinitions, decisionDefinition)
	}
	return engine.saveDmnResourceDefinition(ctx, dmnResourceDefinition, decisionDefinitions)
}

func (engine *ZenDmnEngine) saveDmnResourceDefinition(
	ctx context.Context,
	dmnResourceDefinition runtime.DmnResourceDefinition,
	decisionDefinitions []runtime.DecisionDefinition,
) (*runtime.DmnResourceDefinition, []runtime.DecisionDefinition, error) {
	dmnResourceDefinitions, err := engine.persistence.FindDmnResourceDefinitionsById(ctx, dmnResourceDefinition.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to saveDmnResourceDefinition dmnResourceDefinition by id %s: %w", dmnResourceDefinition.Id, err)
	}
	if len(dmnResourceDefinitions) > 0 {
		latestIndex := len(dmnResourceDefinitions) - 1
		if dmnResourceDefinitions[latestIndex].DmnChecksum == dmnResourceDefinition.DmnChecksum {
			return &dmnResourceDefinitions[latestIndex], nil, nil
		}
		dmnResourceDefinition.Version = dmnResourceDefinitions[latestIndex].Version + 1
	}
	err = engine.persistence.SaveDmnResourceDefinition(ctx, dmnResourceDefinition)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to save decisionDefinition definition by id %s: %w", dmnResourceDefinition.Id, err)
	}

	resultDecisions := make([]runtime.DecisionDefinition, 0)
	for _, decisionDefinition := range decisionDefinitions {
		decisionDefinitions, err := engine.persistence.GetDecisionDefinitionsById(ctx, decisionDefinition.Id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to save decisionDefinition in dmn resource definition %s by id %s: %w", dmnResourceDefinition.Id, decisionDefinition.Id, err)
		}
		if len(decisionDefinitions) > 0 {
			latestIndex := len(decisionDefinitions) - 1
			decisionDefinition.Version = decisionDefinitions[latestIndex].Version + 1
		}
		err = engine.persistence.SaveDecisionDefinition(ctx, decisionDefinition)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to save decisionDefinition in decisionDefinition definition %s by id %s: %w", dmnResourceDefinition.Id, decisionDefinition.Id, err)
		}
		resultDecisions = append(resultDecisions, decisionDefinition)
	}

	return &dmnResourceDefinition, resultDecisions, engine.Validate(ctx, &dmnResourceDefinition)
}

func (engine *ZenDmnEngine) generateKey() int64 {
	return engine.persistence.GenerateId()
}

func (engine *ZenDmnEngine) Validate(ctx context.Context, dmnDefinition *runtime.DmnResourceDefinition) error {
	// TODO: Implement validation - Cyclic Requirements, unique ids, etc.
	return nil
}

// TODO: improve tests
func (engine *ZenDmnEngine) FindAndEvaluateDRD(
	ctx context.Context,
	bindingType string,
	decisionId string,
	versionTag string,
	inputVariableContext map[string]interface{},
) (*EvaluatedDRDResult, error) {
	var decisionDefinition runtime.DecisionDefinition
	var dmnResourceDefinition runtime.DmnResourceDefinition
	var err error

	switch bindingType {
	case "", "latest":
		decisionPath := strings.Split(decisionId, ".")
		switch len(decisionPath) {
		case 1:
			decisionId = decisionPath[0]
			decisionDefinition, err = engine.persistence.GetLatestDecisionDefinitionById(ctx, decisionId)
			if err != nil {
				return nil, fmt.Errorf("failed to find decisionDefinition %s : %w", decisionId, err)
			}
			dmnResourceDefinition, err = engine.persistence.FindDmnResourceDefinitionByKey(ctx, decisionDefinition.DmnResourceDefinitionKey)
			if err != nil {
				return nil, fmt.Errorf("failed to find dmnResourceDefinition %s:%d contaning decisionDefinition %s : %w",
					decisionDefinition.DecisionDefinitionId,
					decisionDefinition.DmnResourceDefinitionKey,
					decisionDefinition.Id,
					err,
				)
			}
		case 2:
			decisionDefinitionId := decisionPath[0]
			decisionId = decisionPath[1]
			decisionDefinition, err = engine.persistence.GetLatestDecisionDefinitionByIdAndDecisionDefinitionId(ctx, decisionId, decisionDefinitionId)
			if err != nil {
				return nil, fmt.Errorf("failed to find decisionDefinition %s stored in decisionDefinition %s : %w", decisionId, decisionDefinitionId, err)
			}
			dmnResourceDefinition, err = engine.persistence.FindDmnResourceDefinitionByKey(ctx, decisionDefinition.DmnResourceDefinitionKey)
			if err != nil {
				return nil, fmt.Errorf("failed to find decisionDefinition %s:%d contaning decisionDefinition %s : %w",
					decisionDefinition.DecisionDefinitionId,
					decisionDefinition.DmnResourceDefinitionKey,
					decisionDefinition.Id,
					err,
				)
			}
		default:
			return nil, fmt.Errorf("failed to process decisionDefinition %s : DecisionId has wrong format", decisionPath)
		}
	case "deployment":
		//TODO: Implement binding type deployment
		return nil, fmt.Errorf("failed to process decisionDefinition %s : bindingType \"deployment\" unsuported", decisionId)
	case "versionTag":
		decisionDefinition, err = engine.persistence.GetLatestDecisionDefinitionByIdAndVersionTag(ctx, decisionId, versionTag)
		if err != nil {
			return nil, fmt.Errorf("failed to find decisionDefinition %s with versionTag %s : %w", decisionId, versionTag, err)
		}
		dmnResourceDefinition, err = engine.persistence.FindDmnResourceDefinitionByKey(ctx, decisionDefinition.DmnResourceDefinitionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to find dmnResourceDefinition %s:%d contaning decisionDefinition %s : %w",
				decisionDefinition.DecisionDefinitionId,
				decisionDefinition.DmnResourceDefinitionKey,
				decisionDefinition.Id,
				err,
			)
		}
	default:
		return nil, fmt.Errorf("failed to process Decision %s: BindingType \"%s\" unsuported", decisionId, bindingType)
	}

	result, err := engine.evaluateDRD(
		ctx,
		&dmnResourceDefinition,
		&decisionDefinition,
		inputVariableContext,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (engine *ZenDmnEngine) evaluateDRD(
	ctx context.Context,
	dmnResourceDefinition *runtime.DmnResourceDefinition,
	decisionDefinition *runtime.DecisionDefinition,
	inputVariableContext map[string]interface{},
) (*EvaluatedDRDResult, error) {
	result, dependencies, err := engine.evaluateDecision(ctx, dmnResourceDefinition, decisionDefinition.Id, inputVariableContext)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate DecisionDefinition %s in DmnResourceDefinition %s:%d: %w",
			decisionDefinition.Id,
			dmnResourceDefinition.Id,
			dmnResourceDefinition.Key,
			err)
	}

	evaluatedDecisions := append([]EvaluatedDecisionResult{result}, dependencies...)

	return &EvaluatedDRDResult{
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     result.DecisionOutput,
	}, nil
}

// EvaluateDecision TODO: better error report
func (engine *ZenDmnEngine) evaluateDecision(
	ctx context.Context,
	dmnResourceDefinition *runtime.DmnResourceDefinition,
	decisionId string,
	inputVariableContext map[string]interface{},
) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error) {
	foundDecision := findDecisionDefinition(dmnResourceDefinition, decisionId)
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

		result, dependencies, err := engine.evaluateDecision(ctx, dmnResourceDefinition, requiredDecision, inputVariableContext)

		if err != nil {
			return result, dependencies, err
		}

		for key, outputVariable := range result.DecisionOutput {
			localVariableContext[key] = outputVariable
		}
		evaluatedDependencies = append(evaluatedDependencies, result)
		evaluatedDependencies = append(evaluatedDependencies, dependencies...)
	}

	decisionTable := foundDecision.DecisionTable
	evaluatedInputs := make([]EvaluatedInput, len(decisionTable.Inputs))

	for i, input := range decisionTable.Inputs {

		value, _ := feel.EvalStringWithScope(input.InputExpression.Text, localVariableContext)
		evaluatedInputs[i] = EvaluatedInput{
			InputId:         input.Id,
			InputName:       input.Label,
			InputExpression: input.InputExpression.Text,
			InputValue:      value,
		}
	}

	matchedRules := make([]EvaluatedRule, 0)

	for ruleIndex, rule := range decisionTable.Rules {
		allColumnsMatch := true
		for i, inputEntry := range rule.InputEntry {
			inputInstance := evaluatedInputs[i]
			match, _ := EvaluateCellMatch(inputInstance.InputExpression, inputEntry.Text, localVariableContext)

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
					OutputId:       output.Id,
					OutputName:     output.Label,
					OutputJsonName: output.Name,
					OutputValue:    value,
				}
			}
			matchedRules = append(matchedRules, EvaluatedRule{
				RuleId:           rule.Id,
				RuleIndex:        ruleIndex + 1,
				EvaluatedOutputs: evaluatedOutputs,
			})
			if foundDecision.DecisionTable.HitPolicy == dmn.HitPolicyFirst {
				break
			}
		}
	}

	return EvaluatedDecisionResult{
		DecisionId:                foundDecision.Id,
		DecisionName:              foundDecision.Name,
		DecisionType:              "<default>",
		DecisionDefinitionVersion: dmnResourceDefinition.Version,
		DecisionDefinitionKey:     dmnResourceDefinition.Key,
		DecisionDefinitionId:      dmnResourceDefinition.Id,
		MatchedRules:              matchedRules,
		EvaluatedInputs:           evaluatedInputs,
		DecisionOutput:            EvaluateHitPolicyOutput(foundDecision.DecisionTable.HitPolicy, foundDecision.DecisionTable.HitPolicyAggregation, matchedRules),
	}, evaluatedDependencies, nil

}
