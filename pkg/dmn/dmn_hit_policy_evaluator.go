package dmn

import (
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
)

func EvaluateHitPolicyOutput(decision *dmn.TDecision, hitPolicy dmn.HitPolicy, hitPolicyAggregation dmn.HitPolicyAggregation, matchedRules []EvaluatedRule) map[string]interface{} {
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
			return evaluateCollectOutput(decision, matchedRules)
		}
	case dmn.HitPolicyFirst:
		return evaluateFirstRuleOutput(decision, matchedRules)
	case dmn.HitPolicyPriority:
		return evaluateFirstRuleOutput(decision, matchedRules)
	case dmn.HitPolicyAny:
		return evaluateFirstRuleOutput(decision, matchedRules)
	case dmn.HitPolicyRuleOrder:
		return evaluateFirstRuleOutput(decision, matchedRules)
	case dmn.HitPolicyOutputOrder:
		return evaluateFirstRuleOutput(decision, matchedRules)
	default:
		return evaluateUniqueOutput(decision, matchedRules)
	}
}

func evaluateCollectOutput(decision *dmn.TDecision, matchedRules []EvaluatedRule) map[string]interface{} {
	resultList := make([]interface{}, 0, len(matchedRules))
	if len(decision.DecisionTable.Outputs) == 1 && decision.DecisionTable.Outputs[0].Name == "" {
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
	finalResult[decision.Id] = resultList
	return finalResult
}

func evaluateCollectSumOutput(matchedRules []EvaluatedRule) map[string]interface{} {
	panic("Not implemented")
}

func evaluateCollectMinOutput(matchedRules []EvaluatedRule) map[string]interface{} {
	panic("Not implemented")
}

func evaluateCollectMaxOutput(matchedRules []EvaluatedRule) map[string]interface{} {
	panic("Not implemented")
}

func evaluateCollectCountOutput(matchedRules []EvaluatedRule) map[string]interface{} {
	panic("Not implemented")
}

func evaluateFirstRuleOutput(decision *dmn.TDecision, matchedRules []EvaluatedRule) map[string]interface{} {
	finalResult := make(map[string]interface{})

	if len(matchedRules) == 0 {
		finalResult[decision.Id] = nil
		return finalResult
	}

	if len(decision.DecisionTable.Outputs) == 1 && decision.DecisionTable.Outputs[0].Name == "" {
		finalResult[decision.Id] = matchedRules[0].EvaluatedOutputs[0].OutputValue
		return finalResult
	}

	rule := matchedRules[0]
	result := make(map[string]interface{})
	for _, evaluatedOutput := range rule.EvaluatedOutputs {
		result[evaluatedOutput.OutputJsonName] = evaluatedOutput.OutputValue
	}

	finalResult[decision.Id] = result
	return finalResult
}

func evaluatePriorityOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateAnyOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateRuleOrderOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateOutputOrderOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateUniqueOutput(decision *dmn.TDecision, matchedRules []EvaluatedRule) map[string]interface{} {
	if len(matchedRules) > 1 {
		return nil
	}
	return evaluateFirstRuleOutput(decision, matchedRules)
}
