package dmn

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
)

func EvaluateHitPolicyOutput(
	decisionTable *dmn.TDecisionTable,
	decisionID string,
	hitPolicy dmn.HitPolicy,
	hitPolicyAggregation dmn.HitPolicyAggregation,
	matchedRules []EvaluatedRule,
) (map[string]interface{}, error) {
	switch hitPolicy {
	case dmn.HitPolicyCollect:
		switch hitPolicyAggregation {
		case dmn.HitPolicyAggregationSum:
			return evaluateCollectSumOutput(decisionTable, decisionID, matchedRules)
		case dmn.HitPolicyAggregationMin:
			return evaluateCollectMinOutput(decisionTable, decisionID, matchedRules)
		case dmn.HitPolicyAggregationMax:
			return evaluateCollectMaxOutput(decisionTable, decisionID, matchedRules)
		case dmn.HitPolicyAggregationCount:
			return evaluateCollectCountOutput(decisionTable, decisionID, matchedRules)
		default:
			return evaluateCollectOutput(decisionTable, decisionID, matchedRules), nil
		}
	case dmn.HitPolicyFirst:
		return evaluateFirstRuleOutput(decisionTable, decisionID, matchedRules), nil
	case dmn.HitPolicyPriority:
		return nil, fmt.Errorf("priority hit policy is not supported: %s", decisionID)
	case dmn.HitPolicyAny:
		return evaluateAnyOutput(decisionTable, decisionID, matchedRules)
	case dmn.HitPolicyRuleOrder:
		return evaluateRuleOrderOutput(decisionTable, decisionID, matchedRules), nil
	case dmn.HitPolicyOutputOrder:
		return nil, fmt.Errorf("output order hit policy is not supported: %s", decisionID)
	default:
		return evaluateUniqueOutput(decisionTable, decisionID, matchedRules)
	}
}

func evaluateCollectOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) map[string]interface{} {
	resultList := make([]interface{}, 0, len(matchedRules))
	if len(decisionTable.Outputs) == 1 && decisionTable.Outputs[0].Name == "" {
		for _, rule := range matchedRules {
			resultList = append(resultList, rule.EvaluatedOutputs[0].OutputValue)
		}
	} else {
		for _, rule := range matchedRules {
			result := make(map[string]interface{})
			for _, evaluatedOutput := range rule.EvaluatedOutputs {
				result[evaluatedOutput.OutputJsonName] = evaluatedOutput.OutputValue
			}
			resultList = append(resultList, result)
		}
	}

	finalResult := make(map[string]interface{})
	finalResult[decisionID] = resultList
	return finalResult
}

func evaluateCollectSumOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	values, err := collectNumericOutputValues(decisionTable, decisionID, dmn.HitPolicyAggregationSum, matchedRules)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return collectAggregationOutput(decisionTable, decisionID, nil), nil
	}

	var result float64
	for _, value := range values {
		result += value
	}
	return collectAggregationOutput(decisionTable, decisionID, result), nil
}

func evaluateCollectMinOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	values, err := collectNumericOutputValues(decisionTable, decisionID, dmn.HitPolicyAggregationMin, matchedRules)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return collectAggregationOutput(decisionTable, decisionID, nil), nil
	}

	return collectAggregationOutput(decisionTable, decisionID, slices.Min(values)), nil
}

func evaluateCollectMaxOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	values, err := collectNumericOutputValues(decisionTable, decisionID, dmn.HitPolicyAggregationMax, matchedRules)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return collectAggregationOutput(decisionTable, decisionID, nil), nil
	}

	return collectAggregationOutput(decisionTable, decisionID, slices.Max(values)), nil
}

func evaluateCollectCountOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	if err := validateCollectAggregationOutput(decisionTable, decisionID, dmn.HitPolicyAggregationCount); err != nil {
		return nil, err
	}
	return collectAggregationOutput(decisionTable, decisionID, float64(len(matchedRules))), nil
}

func collectNumericOutputValues(
	decisionTable *dmn.TDecisionTable,
	decisionID string,
	aggregation dmn.HitPolicyAggregation,
	matchedRules []EvaluatedRule,
) ([]float64, error) {
	if err := validateCollectAggregationOutput(decisionTable, decisionID, aggregation); err != nil {
		return nil, err
	}

	values := make([]float64, 0, len(matchedRules))
	for _, rule := range matchedRules {
		if len(rule.EvaluatedOutputs) != 1 {
			return nil, fmt.Errorf("COLLECT %s hit policy aggregation requires exactly one evaluated output for decision %s", aggregation, decisionID)
		}

		value, err := numericOutputValue(rule.EvaluatedOutputs[0].OutputValue)
		if err != nil {
			return nil, fmt.Errorf("COLLECT %s hit policy aggregation requires numeric output values for decision %s: %w", aggregation, decisionID, err)
		}
		values = append(values, value)
	}

	return values, nil
}

func validateCollectAggregationOutput(decisionTable *dmn.TDecisionTable, decisionID string, aggregation dmn.HitPolicyAggregation) error {
	if len(decisionTable.Outputs) != 1 {
		return fmt.Errorf("COLLECT %s hit policy aggregation requires exactly one output for decision %s", aggregation, decisionID)
	}
	return nil
}

func collectAggregationOutput(decisionTable *dmn.TDecisionTable, decisionID string, value interface{}) map[string]interface{} {
	finalResult := make(map[string]interface{})

	if decisionTable.Outputs[0].Name == "" {
		finalResult[decisionID] = value
		return finalResult
	}

	finalResult[decisionID] = map[string]interface{}{decisionTable.Outputs[0].Name: value}
	return finalResult
}

func numericOutputValue(value interface{}) (float64, error) {
	switch typedValue := value.(type) {
	case int:
		return float64(typedValue), nil
	case int8:
		return float64(typedValue), nil
	case int16:
		return float64(typedValue), nil
	case int32:
		return float64(typedValue), nil
	case int64:
		return float64(typedValue), nil
	case uint:
		return float64(typedValue), nil
	case uint8:
		return float64(typedValue), nil
	case uint16:
		return float64(typedValue), nil
	case uint32:
		return float64(typedValue), nil
	case uint64:
		return float64(typedValue), nil
	case float32:
		return float64(typedValue), nil
	case float64:
		return typedValue, nil
	default:
		return 0, fmt.Errorf("value %v has type %T", value, value)
	}
}

func evaluateFirstRuleOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) map[string]interface{} {
	finalResult := make(map[string]interface{})

	if len(matchedRules) == 0 {
		finalResult[decisionID] = nil
		return finalResult
	}

	if len(decisionTable.Outputs) == 1 && decisionTable.Outputs[0].Name == "" {
		finalResult[decisionID] = matchedRules[0].EvaluatedOutputs[0].OutputValue
		return finalResult
	}

	rule := matchedRules[0]
	result := make(map[string]interface{})
	for _, evaluatedOutput := range rule.EvaluatedOutputs {
		result[evaluatedOutput.OutputJsonName] = evaluatedOutput.OutputValue
	}

	finalResult[decisionID] = result
	return finalResult
}

func evaluateAnyOutput(decision *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	if len(matchedRules) > 1 {
		outputs := matchedRules[0].EvaluatedOutputs

		for _, rule := range matchedRules {
			if !evaluatedOutputsEqual(outputs, rule.EvaluatedOutputs) {
				return nil, fmt.Errorf("ANY hit policy violation: multiple matched rules returned different output values for decision %s", decisionID)
			}
		}
	}
	return evaluateFirstRuleOutput(decision, decisionID, matchedRules), nil
}

func evaluatedOutputsEqual(left []EvaluatedOutput, right []EvaluatedOutput) bool {
	if len(left) != len(right) {
		return false
	}

	for index, leftOutput := range left {
		if !feelOutputValuesEqual(leftOutput.OutputValue, right[index].OutputValue) {
			return false
		}
	}

	return true
}

func feelOutputValuesEqual(left interface{}, right interface{}) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}

	if leftNum, leftErr := numericOutputValue(left); leftErr == nil {
		rightNum, rightErr := numericOutputValue(right)
		return rightErr == nil && leftNum == rightNum
	}

	switch leftValue := left.(type) {
	case []interface{}:
		rightValue, ok := right.([]interface{})
		return ok && feelSequencesEqual(leftValue, rightValue)
	case map[string]interface{}:
		rightValue, ok := right.(map[string]interface{})
		return ok && feelContextsEqual(leftValue, rightValue)
	}

	return false
}

func feelSequencesEqual(left []interface{}, right []interface{}) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !feelOutputValuesEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func feelContextsEqual(left map[string]interface{}, right map[string]interface{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		rightEntry, exists := right[key]
		if !exists || !feelOutputValuesEqual(value, rightEntry) {
			return false
		}
	}
	return true
}

func evaluateRuleOrderOutput(decisionTable *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) map[string]interface{} {
	return evaluateCollectOutput(decisionTable, decisionID, matchedRules)
}

func evaluateUniqueOutput(decision *dmn.TDecisionTable, decisionID string, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	if len(matchedRules) > 1 {
		return nil, fmt.Errorf("unique hit policy violation: multiple rules matched for decision %s", decisionID)
	}
	return evaluateFirstRuleOutput(decision, decisionID, matchedRules), nil
}
