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
		return nil, fmt.Errorf("failed to parse decision definition from bytes: %v, %w", resourceName, err)
	}
	return &definition, nil
}

func (engine *ZenDmnEngine) SaveDecisionDefinition(
	ctx context.Context,
	resourceName string,
	definition dmn.TDefinitions,
	xmlData []byte,
	key int64,
) (*runtime.DecisionDefinition, []runtime.Decision, error) {
	md5sum := md5.Sum(xmlData)
	decisionDefinition := runtime.DecisionDefinition{
		Version:         1,
		Id:              definition.Id,
		Key:             key,
		Definitions:     definition,
		DmnData:         xmlData,
		DmnChecksum:     md5sum,
		DmnResourceName: resourceName,
	}
	decisions := make([]runtime.Decision, 0)
	for _, decision := range definition.Decisions {
		tdecision := runtime.Decision{
			Version:               1,
			Id:                    decision.Id,
			VersionTag:            decision.VersionTag.Value,
			DecisionDefinitionId:  decisionDefinition.Id,
			DecisionDefinitionKey: decisionDefinition.Key,
		}
		decisions = append(decisions, tdecision)
	}
	return engine.saveDecisionDefinition(ctx, decisionDefinition, decisions)
}

func (engine *ZenDmnEngine) saveDecisionDefinition(
	ctx context.Context,
	decisionDefinition runtime.DecisionDefinition,
	decisions []runtime.Decision,
) (*runtime.DecisionDefinition, []runtime.Decision, error) {
	decisionDefinitions, err := engine.persistence.GetDecisionDefinitionsById(ctx, decisionDefinition.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to saveDecisionDefinition decision definition by id %s: %w", decisionDefinition.Id, err)
	}
	if len(decisionDefinitions) > 0 {
		latestIndex := len(decisionDefinitions) - 1
		if decisionDefinitions[latestIndex].DmnChecksum == decisionDefinition.DmnChecksum {
			return &decisionDefinitions[latestIndex], nil, nil
		}
		decisionDefinition.Version = decisionDefinitions[latestIndex].Version + 1
	}
	err = engine.persistence.SaveDecisionDefinition(ctx, decisionDefinition)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to save decision definition by id %s: %w", decisionDefinition.Id, err)
	}

	resultDecisions := make([]runtime.Decision, 0)
	for _, decision := range decisions {
		tdecisions, err := engine.persistence.GetDecisionsById(ctx, decision.Id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to save decision in decision definition %s by id %s: %w", decisionDefinition.Id, decision.Id, err)
		}
		if len(tdecisions) > 0 {
			latestIndex := len(tdecisions) - 1
			decision.Version = tdecisions[latestIndex].Version + 1
		}
		err = engine.persistence.SaveDecision(ctx, decision)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to save decision in decision definition %s by id %s: %w", decisionDefinition.Id, decision.Id, err)
		}
		resultDecisions = append(resultDecisions, decision)
	}

	return &decisionDefinition, resultDecisions, engine.Validate(ctx, &decisionDefinition)
}

func (engine *ZenDmnEngine) generateKey() int64 {
	return engine.persistence.GenerateId()
}

func (engine *ZenDmnEngine) Validate(ctx context.Context, dmnDefinition *runtime.DecisionDefinition) error {
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
	var decision runtime.Decision
	var decisionDefinition runtime.DecisionDefinition
	var err error

	switch bindingType {
	case "", "latest":
		decisionPath := strings.Split(decisionId, ".")
		switch len(decisionPath) {
		case 1:
			decisionId = decisionPath[0]
			decision, err = engine.persistence.GetLatestDecisionById(ctx, decisionId)
			if err != nil {
				return nil, fmt.Errorf("failed to find decision %s : %w", decisionId, err)
			}
			decisionDefinition, err = engine.persistence.GetDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
			if err != nil {
				return nil, fmt.Errorf("failed to find decisionDefinition %s:%d contaning decision %s : %w",
					decision.DecisionDefinitionId,
					decision.DecisionDefinitionKey,
					decision.Id,
					err,
				)
			}
		case 2:
			decisionDefinitionId := decisionPath[0]
			decisionId = decisionPath[1]
			decision, err = engine.persistence.GetLatestDecisionByIdAndDecisionDefinitionId(ctx, decisionId, decisionDefinitionId)
			if err != nil {
				return nil, fmt.Errorf("failed to find decision %s stored in decisionDefinition %s : %w", decisionId, decisionDefinitionId, err)
			}
			decisionDefinition, err = engine.persistence.GetDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
			if err != nil {
				return nil, fmt.Errorf("failed to find decisionDefinition %s:%d contaning decision %s : %w",
					decision.DecisionDefinitionId,
					decision.DecisionDefinitionKey,
					decision.Id,
					err,
				)
			}
		default:
			return nil, fmt.Errorf("failed to process decision %s : DecisionId has wrong format", decisionPath)
		}
	case "deployment":
		//TODO: Implement binding type deployment
		return nil, fmt.Errorf("failed to process decision %s : bindingType \"deployment\" unsuported", decisionId)
	case "versionTag":
		decision, err = engine.persistence.GetLatestDecisionByIdAndVersionTag(ctx, decisionId, versionTag)
		if err != nil {
			return nil, fmt.Errorf("failed to find decision %s with versionTag %s : %w", decisionId, versionTag, err)
		}
		decisionDefinition, err = engine.persistence.GetDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to find decisionDefinition %s:%d contaning decision %s : %w",
				decision.DecisionDefinitionId,
				decision.DecisionDefinitionKey,
				decision.Id,
				err,
			)
		}
	default:
		return nil, fmt.Errorf("failed to process Decision %s: BindingType \"%s\" unsuported", decisionId, bindingType)
	}

	result, err := engine.evaluateDRD(
		ctx,
		&decisionDefinition,
		&decision,
		inputVariableContext,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (engine *ZenDmnEngine) evaluateDRD(ctx context.Context, decisionDefinition *runtime.DecisionDefinition, decision *runtime.Decision, inputVariableContext map[string]interface{}) (*EvaluatedDRDResult, error) {
	result, dependencies, err := engine.evaluateDecision(ctx, decisionDefinition, decision.Id, inputVariableContext)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate Decision %s in DecisionDefinition %s:%d: %w",
			decision.Id,
			decisionDefinition.Id,
			decisionDefinition.Key,
			err)
	}

	evaluatedDecisions := append([]EvaluatedDecisionResult{result}, dependencies...)

	return &EvaluatedDRDResult{
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     result.DecisionOutput,
	}, nil
}

// EvaluateDecision TODO: better error report
func (engine *ZenDmnEngine) evaluateDecision(ctx context.Context, decisionDefinition *runtime.DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error) {
	foundDecision := findDecision(decisionDefinition, decisionId)
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

		result, dependencies, err := engine.evaluateDecision(ctx, decisionDefinition, requiredDecision, inputVariableContext)

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
		DecisionDefinitionVersion: decisionDefinition.Version,
		DecisionDefinitionKey:     decisionDefinition.Key,
		DecisionDefinitionId:      decisionDefinition.Id,
		MatchedRules:              matchedRules,
		EvaluatedInputs:           evaluatedInputs,
		DecisionOutput:            EvaluateHitPolicyOutput(foundDecision.DecisionTable.HitPolicy, foundDecision.DecisionTable.HitPolicyAggregation, matchedRules),
	}, evaluatedDependencies, nil

}
