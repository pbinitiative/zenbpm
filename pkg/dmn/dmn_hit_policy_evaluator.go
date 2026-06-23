package dmn

import (
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
)

func EvaluateHitPolicyOutput(decisionTable *dmn.TDecisionTable, decisionId string, hitPolicy dmn.HitPolicy, hitPolicyAggregation dmn.HitPolicyAggregation, matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	switch hitPolicy {
	case dmn.HitPolicyCollect:
		switch hitPolicyAggregation {
		case dmn.HitPolicyAggregationSum:
			return evaluateCollectSumOutput(matchedRules)
		case dmn.HitPolicyAggregationMin:
			return evaluateCollectMinOutput(matchedRules)
		case dmn.HitPolicyAggregationMax:
			return evaluateCollectMaxOutput(matchedRules)
		case dmn.HitPolicyAggregationCount:
			return evaluateCollectCountOutput(matchedRules)
		default:
			return evaluateCollectOutput(decisionTable, decisionId, matchedRules), nil
		}
	case dmn.HitPolicyFirst:
		return evaluateFirstRuleOutput(decisionTable, decisionId, matchedRules), nil
	case dmn.HitPolicyPriority:
		return evaluateFirstRuleOutput(decisionTable, decisionId, matchedRules), nil
	case dmn.HitPolicyAny:
		return evaluateFirstRuleOutput(decisionTable, decisionId, matchedRules), nil
	case dmn.HitPolicyRuleOrder:
		return evaluateFirstRuleOutput(decisionTable, decisionId, matchedRules), nil
	case dmn.HitPolicyOutputOrder:
		return evaluateFirstRuleOutput(decisionTable, decisionId, matchedRules), nil
	default:
		return evaluateUniqueOutput(decisionTable, decisionId, matchedRules), nil
	}
}

func evaluateCollectOutput(decisionTable *dmn.TDecisionTable, decisionId string, matchedRules []EvaluatedRule) map[string]interface{} {
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
	finalResult[decisionId] = resultList
	return finalResult
}

func evaluateCollectSumOutput(matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	return nil, fmt.Errorf("collect sum hit policy aggregation is not implemented")
}

func evaluateCollectMinOutput(matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	return nil, fmt.Errorf("collect min hit policy aggregation is not implemented")
}

func evaluateCollectMaxOutput(matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	return nil, fmt.Errorf("collect max hit policy aggregation is not implemented")
}

func evaluateCollectCountOutput(matchedRules []EvaluatedRule) (map[string]interface{}, error) {
	return nil, fmt.Errorf("collect count hit policy aggregation is not implemented")
}

func evaluateFirstRuleOutput(decisionTable *dmn.TDecisionTable, decisionId string, matchedRules []EvaluatedRule) map[string]interface{} {
	finalResult := make(map[string]interface{})

	if len(matchedRules) == 0 {
		finalResult[decisionId] = nil
		return finalResult
	}

	if len(decisionTable.Outputs) == 1 && decisionTable.Outputs[0].Name == "" {
		finalResult[decisionId] = matchedRules[0].EvaluatedOutputs[0].OutputValue
		return finalResult
	}

	rule := matchedRules[0]
	result := make(map[string]interface{})
	for _, evaluatedOutput := range rule.EvaluatedOutputs {
		result[evaluatedOutput.OutputJsonName] = evaluatedOutput.OutputValue
	}

	finalResult[decisionId] = result
	return finalResult
}

func evaluateUniqueOutput(decision *dmn.TDecisionTable, decisionId string, matchedRules []EvaluatedRule) map[string]interface{} {
	if len(matchedRules) > 1 {
		return nil
	}
	return evaluateFirstRuleOutput(decision, decisionId, matchedRules)
}
