package dmn

import (
	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"strings"
)

func EvaluateCellMatch(columnExpression string, cellExpression string, variables map[string]interface{}) (bool, error) {
	if cellExpression == "" {
		// If the text is empty, it means any value is accepted
		return true, nil
	}

	var resultExpression string

	if strings.HasPrefix(cellExpression, "=") || strings.HasPrefix(cellExpression, "<") || strings.HasPrefix(cellExpression, ">") {
		resultExpression = columnExpression + " " + cellExpression
	} else {
		resultExpression = columnExpression + " = " + cellExpression
	}

	result, err := feel.EvalStringWithScope(resultExpression, variables)

	return result.(bool), err
}

func EvaluateHitPolicyOutput(hitPolicy dmn.HitPolicy, hitPolicyAggregation dmn.HitPolicyAggregation, matchedRules []EvaluatedRule) interface{} {
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
			return evaluateCollectOutput(matchedRules)
		}
	case dmn.HitPolicyFirst:
		return evaluateFirstOutput(matchedRules)
	case dmn.HitPolicyPriority:
		return evaluateFirstOutput(matchedRules)
	case dmn.HitPolicyAny:
		return evaluateFirstOutput(matchedRules)
	case dmn.HitPolicyRuleOrder:
		return evaluateFirstOutput(matchedRules)
	case dmn.HitPolicyOutputOrder:
		return evaluateFirstOutput(matchedRules)
	default:
		return evaluateUniqueOutput(matchedRules)
	}
}

func evaluateCollectOutput(matchedRules []EvaluatedRule) interface{} {
	result := make([]interface{}, len(matchedRules))
	for i, rule := range matchedRules {
		if len(rule.evaluatedOutputs) > 1 {
			result[i] = ruleOutputToMap(rule)
		} else {
			result[i] = rule.evaluatedOutputs[0].outputValue
		}
	}

	return result
}

func evaluateCollectSumOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateCollectMinOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateCollectMaxOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateCollectCountOutput(matchedRules []EvaluatedRule) interface{} {
	panic("Not implemented")
}

func evaluateFirstOutput(matchedRules []EvaluatedRule) interface{} {
	if len(matchedRules) > 0 {
		rule := matchedRules[0]
		if len(rule.evaluatedOutputs) > 1 {
			return ruleOutputToMap(rule)
		} else {
			return rule.evaluatedOutputs[0].outputValue
		}
	}
	return nil
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

func evaluateUniqueOutput(matchedRules []EvaluatedRule) interface{} {
	if len(matchedRules) > 1 {
		return nil
	}
	return evaluateFirstOutput(matchedRules)
}

func ruleOutputToMap(rule EvaluatedRule) map[string]interface{} {
	outputMap := make(map[string]interface{})
	for _, evaluatedOutput := range rule.evaluatedOutputs {
		outputMap[evaluatedOutput.outputJsonName] = evaluatedOutput.outputValue
	}

	return outputMap
}
