package dmn

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"github.com/dop251/goja"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/pbinitiative/zenbpm/pkg/script/feel"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"maps"
	"os"
	"strings"
	"time"
)

type ZenDmnEngine struct {
	persistence storage.DecisionStorage
	feelRuntime script.FeelRuntime
}

type EngineOption = func(*ZenDmnEngine)

// NewEngine creates a new instance of the DMN Engine;
func NewEngine(options ...EngineOption) *ZenDmnEngine {
	engine := ZenDmnEngine{
		persistence: inmemory.NewStorage(),
		feelRuntime: feel.NewFeelinRuntime(context.TODO(), 1, 1),
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

func EngineWithFeel(feel script.FeelRuntime) EngineOption {
	return func(engine *ZenDmnEngine) {
		engine.feelRuntime = feel
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
	definition *dmn.TDefinitions,
	xmlData []byte,
	key int64,
) (*runtime.DecisionDefinition, []runtime.Decision, error) {
	md5sum := md5.Sum(xmlData)
	decisionDefinition := runtime.DecisionDefinition{
		Version:         1,
		Id:              definition.Id,
		Key:             key,
		Definitions:     *definition,
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
	decisionDefinitions, err := engine.persistence.FindDecisionDefinitionsById(ctx, decisionDefinition.Id)
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
			decisionDefinition, err = engine.persistence.FindDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
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
			decisionDefinition, err = engine.persistence.FindDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
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
		decisionDefinition, err = engine.persistence.FindDecisionDefinitionByKey(ctx, decision.DecisionDefinitionKey)
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
	var decisionOutput interface{}
	for _, value := range result.DecisionOutput {
		decisionOutput = value
	}

	return &EvaluatedDRDResult{
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     decisionOutput,
	}, nil
}

// EvaluateDecision TODO: better error report
func (engine *ZenDmnEngine) evaluateDecision(ctx context.Context, decisionDefinition *runtime.DecisionDefinition, decisionId string, inputVariableContext map[string]interface{}) (EvaluatedDecisionResult, []EvaluatedDecisionResult, error) {
	foundDecision := findDecision(decisionDefinition, decisionId)
	if foundDecision == nil {
		return EvaluatedDecisionResult{}, nil, &DecisionNotFoundError{DecisionID: decisionId}
	}

	evaluatedDependencies := make([]EvaluatedDecisionResult, 0)

	localVariableContext := make(map[string]interface{})
	for key, value := range inputVariableContext {
		localVariableContext[key] = value
	}

	for _, requirement := range foundDecision.InformationRequirement {
		switch requirement.RequiredResource.(type) {
		case dmn.TRequiredDecision:
			requiredDecisionRef := requirement.RequiredResource.(dmn.TRequiredDecision).Href

			requiredDecisionId := strings.TrimPrefix(requiredDecisionRef, "#")
			result, dependencies, err := engine.evaluateDecision(ctx, decisionDefinition, requiredDecisionId, inputVariableContext)
			if err != nil {
				return result, dependencies, err
			}

			for key, outputVariable := range result.DecisionOutput {
				localVariableContext[key] = outputVariable
			}
			evaluatedDependencies = append(evaluatedDependencies, result)
			evaluatedDependencies = append(evaluatedDependencies, dependencies...)
		case dmn.TRequiredInput:
			requiredInputRef := requirement.RequiredResource.(dmn.TRequiredInput).Href

			requiredInputId := strings.TrimPrefix(requiredInputRef, "#")

			for _, input := range decisionDefinition.Definitions.InputData {
				if input.Id == requiredInputId {
					if _, ok := inputVariableContext[input.Name]; !ok {
						return EvaluatedDecisionResult{}, nil, fmt.Errorf("required input missing for %s", input.Name)
					}
				}
			}
		default:
			panic(fmt.Sprintf("unsupported TInformationRequirement %+v", requirement))
		}
	}

	var decisionOutput map[string]interface{}
	var matchedRules []EvaluatedRule
	var evaluatedInputs []EvaluatedInput
	var err error
	var decisionName string

	if foundDecision.Name != "" {
		decisionName = foundDecision.Name
	} else {
		decisionName = foundDecision.Id
	}

	if foundDecision.DecisionTable != nil {
		decisionOutput, matchedRules, evaluatedInputs, err = engine.evaluateDecisionTable(foundDecision.DecisionTable, decisionName, localVariableContext)
		if err != nil {
			return EvaluatedDecisionResult{}, nil, err
		}
	} else if foundDecision.LiteralExpression != nil {
		decisionOutput, err = engine.evaluateLiteralExpression(foundDecision.LiteralExpression, foundDecision.Variable, decisionName, localVariableContext)
		if err != nil {
			return EvaluatedDecisionResult{}, nil, err
		}
	} else if foundDecision.Context != nil {
		decisionOutput, err = engine.evaluateContext(foundDecision.Context, decisionName, localVariableContext)
		if err != nil {
			return EvaluatedDecisionResult{}, nil, err
		}
	} else {
		return EvaluatedDecisionResult{}, nil, fmt.Errorf("decision type unsuported on decision id %s", foundDecision.Id)
	}

	return EvaluatedDecisionResult{
		DecisionId:                foundDecision.Id,
		DecisionName:              foundDecision.Name,
		DecisionType:              "<literalExpression>",
		DecisionDefinitionVersion: decisionDefinition.Version,
		DecisionDefinitionKey:     decisionDefinition.Key,
		DecisionDefinitionId:      decisionDefinition.Id,
		MatchedRules:              matchedRules,
		EvaluatedInputs:           evaluatedInputs,
		DecisionOutput:            decisionOutput,
	}, evaluatedDependencies, nil
}

func (engine *ZenDmnEngine) evaluateContext(decisionContext *dmn.TContext, resultVariableName string, inputVariables map[string]interface{}) (map[string]any, error) {
	outputVariableContext := make(map[string]any)
	localVariableContext := map[string]interface{}{}
	maps.Copy(localVariableContext, inputVariables)

	for i, contextEntry := range decisionContext.ContextEntries {
		var output map[string]interface{}
		var err error
		if i == len(decisionContext.ContextEntries)-1 && contextEntry.Variable == nil {
			if contextEntry.DecisionTable != nil {
				output, _, _, err = engine.evaluateDecisionTable(contextEntry.DecisionTable, resultVariableName, localVariableContext)
				if err != nil {
					return nil, err
				}
			} else if contextEntry.LiteralExpression != nil {
				output, err = engine.evaluateLiteralExpression(
					contextEntry.LiteralExpression,
					&dmn.TVariable{
						Id:      resultVariableName,
						Name:    resultVariableName,
						TypeRef: "",
					},
					resultVariableName,
					localVariableContext)
				if err != nil {
					return nil, err
				}
			} else if contextEntry.Context != nil {
				output, err = engine.evaluateContext(contextEntry.Context, resultVariableName, localVariableContext)
				if err != nil {
					return nil, err
				}
			}
			return output, nil
		} else {
			if contextEntry.DecisionTable != nil {
				output, _, _, err = engine.evaluateDecisionTable(contextEntry.DecisionTable, contextEntry.Variable.Name, localVariableContext)
				if err != nil {
					return nil, err
				}
			} else if contextEntry.LiteralExpression != nil {
				output, err = engine.evaluateLiteralExpression(contextEntry.LiteralExpression, contextEntry.Variable, contextEntry.Variable.Name, localVariableContext)
				if err != nil {
					return nil, err
				}
			} else if contextEntry.Context != nil {
				output, err = engine.evaluateContext(contextEntry.Context, contextEntry.Variable.Name, localVariableContext)
				if err != nil {
					return nil, err
				}
			}
		}

		for key, outputVariable := range output {
			localVariableContext[key] = outputVariable
			outputVariableContext[key] = outputVariable
		}
	}

	resultVariable := make(map[string]any)
	resultVariable[resultVariableName] = outputVariableContext
	return resultVariable, nil
}

func (engine *ZenDmnEngine) evaluateLiteralExpression(literalExpression *dmn.TLiteralExpression, variable *dmn.TVariable, decisionId string, localVariableContext map[string]interface{}) (map[string]any, error) {
	var resultValue any
	var err error
	switch literalExpression.ExpressionLanguage {
	case "feel", "":
		resultValue, err = engine.feelRuntime.(*feel.FeelinRuntime).Evaluate(literalExpression.Text.Text, localVariableContext)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluated expression \"%s\" with variables %s: %w", literalExpression.Text.Text, localVariableContext, err)
		}
		if err, ok := resultValue.(goja.Exception); ok {
			return nil, fmt.Errorf("%s", err.String())
		}
	default:
		return nil, fmt.Errorf("unsupported literal expression language %s on decision with Id %s", literalExpression.ExpressionLanguage, decisionId)
	}

	var resultVariable = make(map[string]any)
	switch variable.TypeRef {
	case "":
		resultVariable[variable.Name] = resultValue
	case "string":
		if str, ok := resultValue.(string); ok {
			resultVariable[variable.Name] = str
		} else {
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "boolean":
		switch resultValue.(type) {
		case bool:
			resultVariable[variable.Name] = resultValue.(bool)
		case nil:
			resultVariable[variable.Name] = nil
		default:
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "integer":
		if str, ok := resultValue.(int); ok {
			resultVariable[variable.Name] = str
		} else {
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "long":
		if str, ok := resultValue.(int64); ok {
			resultVariable[variable.Name] = str
		} else {
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "double":
		if str, ok := resultValue.(float64); ok {
			resultVariable[variable.Name] = str
		} else {
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "date":
		//TODO: this is just placeholder
		if str, ok := resultValue.(time.Time); ok {
			resultVariable[variable.Name] = str
		} else {
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	case "number":
		switch resultValue.(type) {
		case float64:
			resultVariable[variable.Name] = resultValue.(float64)
		case int64:
			resultVariable[variable.Name] = resultValue.(int64)
		case nil:
			resultVariable[variable.Name] = nil
		default:
			return nil, fmt.Errorf("literal expression result value %x cannot be cast to %s on decision with Id %s", resultValue, variable.TypeRef, decisionId)
		}
	default:
		resultVariable[variable.Name] = resultValue
	}

	return resultVariable, nil
}

func (engine *ZenDmnEngine) evaluateDecisionTable(decisionTable *dmn.TDecisionTable, decisionId string, localVariableContext map[string]interface{}) (map[string]interface{}, []EvaluatedRule, []EvaluatedInput, error) {
	//TODO: move to parsing checks
	if len(decisionTable.Outputs) > 1 && !isUnique(decisionTable.Outputs) {
		return nil, nil, nil, fmt.Errorf("decision table contains more than one output and all of them have to have unique names, decision id %s", decisionId)
	}

	evaluatedInputs := make([]EvaluatedInput, len(decisionTable.Inputs))
	for i, input := range decisionTable.Inputs {
		value, err := engine.feelRuntime.Evaluate(input.InputExpression.Text, localVariableContext)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error while evaluating epression \"%s\" with variables %s: %v", input.InputExpression.Text, localVariableContext, err)
		}
		evaluatedInputs[i] = EvaluatedInput{
			InputId:         input.Id,
			InputName:       input.Label,
			InputExpression: input.InputExpression.Text,
			InputValue:      value,
		}
	}

	matchedRules := make([]EvaluatedRule, 0)

	//this map is edited each cycle dont use it after this for loop
	cellMatchVariables := map[string]interface{}{}
	maps.Copy(cellMatchVariables, localVariableContext)
	for ruleIndex, rule := range decisionTable.Rules {
		allColumnsMatch := true
		for i, inputEntry := range rule.InputEntry {
			cellMatchVariables["?"] = evaluatedInputs[i].InputValue
			if inputEntry.Text == "" {
				inputEntry.Text = "-"
			}
			match, err := engine.feelRuntime.UnaryTest(inputEntry.Text, cellMatchVariables)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("error while evaluating cell match:  \"%s\" %s ,%v", inputEntry.Text, cellMatchVariables, err)
			}
			if !match {
				allColumnsMatch = false
				break
			}
		}

		if allColumnsMatch {
			evaluatedOutputs := make([]EvaluatedOutput, len(decisionTable.Outputs))
			for i, output := range decisionTable.Outputs {
				value, err := engine.feelRuntime.Evaluate(rule.OutputEntry[i].Text, localVariableContext)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("error while evaluating output \"%s\" with variables %s: %v", rule.OutputEntry[i].Text, localVariableContext, err)
				}

				evaluatedOutputs[i] = EvaluatedOutput{
					OutputId:       output.Id,
					OutputName:     output.Name,
					OutputJsonName: output.Name,
					OutputValue:    value,
				}
			}
			matchedRules = append(matchedRules, EvaluatedRule{
				RuleId:           rule.Id,
				RuleIndex:        ruleIndex + 1,
				EvaluatedOutputs: evaluatedOutputs,
			})
			if decisionTable.HitPolicy == dmn.HitPolicyFirst {
				break
			}
		}
	}

	return EvaluateHitPolicyOutput(decisionTable, decisionId, decisionTable.HitPolicy, decisionTable.HitPolicyAggregation, matchedRules),
		matchedRules,
		evaluatedInputs,
		nil
}

func isUnique(list []dmn.TOutput) bool {
	seen := make(map[string]bool)

	for _, s := range list {
		if seen[s.Name] {
			// duplicate found
			return false
		}
		seen[s.Name] = true
	}
	return true
}
