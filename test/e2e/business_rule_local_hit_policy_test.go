package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalHitPolicyUnique(t *testing.T) {

	t.Run("UNIQUE hit policy creates an incident when multiple rules match", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_unique.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_unique", map[string]any{
			"amount":        1500,
			"customer_type": "VIP",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1500), "customer_type": "VIP"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
		assertNoDecisionInstance(t, processInstance.Key)
	})

	t.Run("UNIQUE hit policy returns multiple named outputs", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_unique_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_multi_output.bpmn", "dmn_hit_policy_unique_multi_output", map[string]any{
			"score": 2,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(20), "reason": "bonus", "score": float64(2)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalScoreDecisionInputs("unique_multi_output", float64(2)), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_unique_multi_output_score_2",
				Outputs: []ExpectedDecisionOutput{
					expectedAdditionalDiscountDecisionOutput("unique_multi_output", float64(20)),
					expectedAdditionalReasonDecisionOutput("unique_multi_output", "bonus"),
				},
			},
		})
	})

	t.Run("UNIQUE hit policy returns the matching rule when only one rule matches", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_unique.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_unique", map[string]any{
			"amount":        10,
			"customer_type": nil,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(0), "amount": float64(10), "customer_type": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedUniqueOrFirstDecisionInputs(float64(10), nil), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_low_amount",
				Outputs: []ExpectedDecisionOutput{
					expectedDiscountDecisionOutput(float64(0)),
				},
			},
		})
	})

	t.Run("UNIQUE hit policy returns nil output when no rule matches", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_unique.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_unique", map[string]any{
			"amount":        500,
			"customer_type": "UNKNOWN",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": interface{}(nil), "amount": float64(500), "customer_type": "UNKNOWN"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedUniqueOrFirstDecisionInputs(float64(500), "UNKNOWN"), nil)
	})
}

func TestBusinessRuleLocalHitPolicyFirst(t *testing.T) {

	t.Run("FIRST hit policy returns the first matching rule when multiple rules match", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_first", map[string]any{
			"amount":        1500,
			"customer_type": "VIP",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(20), "amount": float64(1500), "customer_type": "VIP"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedUniqueOrFirstDecisionInputs(float64(1500), "VIP"), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_vip_customer",
				Outputs: []ExpectedDecisionOutput{
					expectedDiscountDecisionOutput(float64(20)),
				},
			},
		})
	})

	t.Run("FIRST hit policy returns multiple named outputs", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_first_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_multi_output.bpmn", "dmn_hit_policy_first_multi_output", map[string]any{
			"score": 2,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(20), "reason": "high", "score": float64(2)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalScoreDecisionInputs("first_multi_output", float64(2)), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_first_multi_output_score_at_least_2",
				Outputs: []ExpectedDecisionOutput{
					expectedAdditionalDiscountDecisionOutput("first_multi_output", float64(20)),
					expectedAdditionalReasonDecisionOutput("first_multi_output", "high"),
				},
			},
		})
	})

	t.Run("FIRST hit policy returns the matching rule when only one rule matches", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_first", map[string]any{
			"amount":        10,
			"customer_type": nil,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(0), "amount": float64(10), "customer_type": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedUniqueOrFirstDecisionInputs(float64(10), nil), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_low_amount",
				Outputs: []ExpectedDecisionOutput{
					expectedDiscountDecisionOutput(float64(0)),
				},
			},
		})
	})

	t.Run("FIRST hit policy returns nil output when no rule matches", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_first", map[string]any{
			"amount":        500,
			"customer_type": "UNKNOWN",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": interface{}(nil), "amount": float64(500), "customer_type": "UNKNOWN"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedUniqueOrFirstDecisionInputs(float64(500), "UNKNOWN"), nil)
	})

	t.Run("FIRST hit policy returns raw value for unnamed single output", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_first_unnamed_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_result_variable.bpmn", "dmn_hit_policy_first_unnamed_output", map[string]any{
			"score": 1,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"result": float64(42), "score": float64(1)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalScoreDecisionInputs("first_unnamed_output", float64(1)), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_first_unnamed_output_score_at_least_1",
				Outputs: []ExpectedDecisionOutput{
					{
						OutputId:    "output_first_unnamed_output_discount",
						OutputName:  "",
						OutputValue: float64(42),
					},
				},
			},
		})
	})
}

func TestBusinessRuleLocalHitPolicyAny(t *testing.T) {

	t.Run("ANY hit policy creates an incident when matched rules return different outputs", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_any.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_any", map[string]any{
			"amount":        1500,
			"customer_type": "CONFLICT",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1500), "customer_type": "CONFLICT"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
		assertNoDecisionInstance(t, processInstance.Key)
	})

	t.Run("ANY hit policy returns the shared output when multiple rules match", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_any.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_any", map[string]any{
			"amount":        1500,
			"customer_type": "VIP",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(20), "amount": float64(1500), "customer_type": "VIP"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)

		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAnyDecisionInputs(float64(1500), "VIP"), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_vip_high_amount",
				Outputs: []ExpectedDecisionOutput{
					expectedDiscountDecisionOutput(float64(20)),
				},
			},
			{
				RuleId: "rule_vip_medium_amount",
				Outputs: []ExpectedDecisionOutput{
					expectedDiscountDecisionOutput(float64(20)),
				},
			},
		})
	})

	t.Run("ANY hit policy returns nil output when no rule matches", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_any.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", "dmn_hit_policy_any", map[string]any{
			"amount":        500,
			"customer_type": "UNKNOWN",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": interface{}(nil), "amount": float64(500), "customer_type": "UNKNOWN"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAnyDecisionInputs(float64(500), "UNKNOWN"), nil)
	})

	t.Run("ANY hit policy compares equal list and context outputs without panic", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_any_non_scalar.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_result_variable.bpmn", "dmn_hit_policy_any_non_scalar", map[string]any{
			"customer_type": "NON_SCALAR_OK",
		})

		expectedResult := map[string]interface{}{
			"items":   []interface{}{"alpha", "beta"},
			"details": map[string]interface{}{"code": "same"},
		}
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"result": expectedResult, "customer_type": "NON_SCALAR_OK"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("ANY hit policy creates an incident when list outputs differ", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyAnyNonScalarIncident(t, "LIST_CONFLICT")
	})

	t.Run("ANY hit policy creates an incident when context outputs differ", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyAnyNonScalarIncident(t, "CONTEXT_CONFLICT")
	})
	t.Run("ANY hit policy returns shared multiple outputs when all matched rules agree", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_any_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_multi_output.bpmn", "dmn_hit_policy_any_multi_output", map[string]any{
			"amount":        1500,
			"customer_type": "VIP",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": float64(20), "reason": "vip", "amount": float64(1500), "customer_type": "VIP"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalAnyDecisionInputs(float64(1500), "VIP"), []ExpectedDecisionMatchedRule{
			{
				RuleId: "rule_any_multi_output_vip_high",
				Outputs: []ExpectedDecisionOutput{
					expectedAdditionalDiscountDecisionOutput("any_multi_output", float64(20)),
					expectedAdditionalReasonDecisionOutput("any_multi_output", "vip"),
				},
			},
			{
				RuleId: "rule_any_multi_output_vip_medium",
				Outputs: []ExpectedDecisionOutput{
					expectedAdditionalDiscountDecisionOutput("any_multi_output", float64(20)),
					expectedAdditionalReasonDecisionOutput("any_multi_output", "vip"),
				},
			},
		})
	})

	t.Run("ANY hit policy creates an incident when later output columns differ", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_any_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_multi_output.bpmn", "dmn_hit_policy_any_multi_output", map[string]any{
			"amount":        1500,
			"customer_type": "CONFLICT",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1500), "customer_type": "CONFLICT"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
		assertNoDecisionInstance(t, processInstance.Key)
	})
}

func TestBusinessRuleLocalHitPolicyRuleOrder(t *testing.T) {
	t.Run("RULE ORDER hit policy returns all matching outputs in rule order", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyRuleOrder(t, 3, []interface{}{
			map[string]interface{}{"discount": float64(20)},
			map[string]interface{}{"discount": float64(5)},
			map[string]interface{}{"discount": float64(15)},
		}, expectedRuleOrderMatchedRules(float64(20), float64(5), float64(15)))
	})

	t.Run("RULE ORDER hit policy returns one output when one rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyRuleOrder(t, 1, []interface{}{
			map[string]interface{}{"discount": float64(20)},
		}, expectedRuleOrderMatchedRules(float64(20)))
	})

	t.Run("RULE ORDER hit policy returns an empty output list when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyRuleOrder(t, 0, []interface{}{}, nil)
	})

	t.Run("RULE ORDER hit policy returns multiple named outputs in rule order", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_rule_order_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id_result.bpmn",
			"dmn_hit_policy_rule_order_multi_output",
			map[string]any{"score": 3},
		)

		expectedDiscounts := []interface{}{
			map[string]interface{}{"discount": float64(20), "reason": "first"},
			map[string]interface{}{"discount": float64(5), "reason": "second"},
			map[string]interface{}{"discount": float64(15), "reason": "third"},
		}
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discounts": expectedDiscounts, "score": float64(3)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalScoreDecisionInputs("rule_order_multi_output", float64(3)), expectedAdditionalRuleOrderMultiOutputRules(
			additionalMultiOutputRule{suffix: "at_least_1", discount: float64(20), reason: "first"},
			additionalMultiOutputRule{suffix: "at_least_2", discount: float64(5), reason: "second"},
			additionalMultiOutputRule{suffix: "at_least_3", discount: float64(15), reason: "third"},
		))
	})
}

func TestBusinessRuleLocalHitPolicyCollect(t *testing.T) {
	t.Run("COLLECT hit policy returns all matching named outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollect(t, 3, []interface{}{
			map[string]interface{}{"discount": float64(10)},
			map[string]interface{}{"discount": float64(2.5)},
			map[string]interface{}{"discount": float64(-4)},
		}, expectedPlainCollectMatchedRules(float64(10), float64(2.5), float64(-4)))
	})

	t.Run("COLLECT hit policy returns one output when one rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollect(t, 1, []interface{}{
			map[string]interface{}{"discount": float64(10)},
		}, expectedPlainCollectMatchedRules(float64(10)))
	})

	t.Run("COLLECT hit policy returns an empty output list when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollect(t, 0, []interface{}{}, nil)
	})

	t.Run("COLLECT hit policy returns multiple named outputs", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_collect_multi_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id_result.bpmn",
			"dmn_hit_policy_collect_multi_output",
			map[string]any{"score": 3},
		)

		expectedDiscounts := []interface{}{
			map[string]interface{}{"discount": float64(10), "reason": "base"},
			map[string]interface{}{"discount": float64(2.5), "reason": "bonus"},
			map[string]interface{}{"discount": float64(-4), "reason": "penalty"},
		}
		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discounts": expectedDiscounts, "score": float64(3)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertHitPolicyDecisionInstance(t, processInstance.Key, expectedAdditionalScoreDecisionInputs("collect_multi_output", float64(3)), expectedAdditionalCollectMultiOutputRules(
			additionalMultiOutputRule{suffix: "at_least_1", discount: float64(10), reason: "base"},
			additionalMultiOutputRule{suffix: "at_least_2", discount: float64(2.5), reason: "bonus"},
			additionalMultiOutputRule{suffix: "at_least_3", discount: float64(-4), reason: "penalty"},
		))
	})

	t.Run("COLLECT hit policy returns raw value list for unnamed single output", func(t *testing.T) {
		deployHitPolicyDmn(t, "hit_policy_collect_unnamed_output.dmn")

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_result_variable.bpmn", "dmn_hit_policy_collect_unnamed_output", map[string]any{
			"score": 2,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"result": []interface{}{float64(10), float64(2.5)}, "score": float64(2)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})
}

func TestBusinessRuleLocalHitPolicyUnsupportedAndIncidentPaths(t *testing.T) {
	tests := []struct {
		name       string
		dmnFile    string
		decisionId string
		variables  map[string]any
	}{
		{
			name:       "PRIORITY hit policy creates an incident",
			dmnFile:    "hit_policy_priority.dmn",
			decisionId: "dmn_hit_policy_priority",
			variables:  map[string]any{"score": 1},
		},
		{
			name:       "OUTPUT ORDER hit policy creates an incident",
			dmnFile:    "hit_policy_output_order.dmn",
			decisionId: "dmn_hit_policy_output_order",
			variables:  map[string]any{"score": 1},
		},
		{
			name:       "duplicate output names create an incident",
			dmnFile:    "hit_policy_duplicate_output_names.dmn",
			decisionId: "dmn_duplicate_output_names",
			variables:  map[string]any{"score": 1},
		},
		{
			name:       "unsupported literal expression language creates an incident",
			dmnFile:    "literal_expression_unsupported_language.dmn",
			decisionId: "dmn_unsupported_literal_expression_language",
			variables:  map[string]any{},
		},
		{
			name:       "FEEL output evaluation error creates an incident",
			dmnFile:    "hit_policy_feel_eval_error.dmn",
			decisionId: "dmn_feel_eval_error",
			variables:  map[string]any{"score": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployHitPolicyDmn(t, tt.dmnFile)

			processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_result_variable.bpmn", tt.decisionId, tt.variables)

			waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
			assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
			assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
			assertNoDecisionInstance(t, processInstance.Key)
		})
	}
}

func TestBusinessRuleLocalHitPolicyCollectSumAggregation(t *testing.T) {
	t.Run("COLLECT SUM aggregation creates an incident when matched rule returns non-numeric output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationIncident(t, "SUM", "dmn_hit_policy_collect_sum_incident", 1)
	})

	t.Run("COLLECT SUM aggregation creates an incident when decision table has multiple outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationMultiOutputIncident(t, "SUM", "dmn_hit_policy_collect_sum_multi_output_incident", 1)
	})

	t.Run("COLLECT SUM aggregation returns the sum of all matching rule outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "SUM", "dmn_hit_policy_collect_sum", 3, float64(8.5), expectedCollectMatchedRules("SUM"))
	})

	t.Run("COLLECT SUM aggregation returns the single matching output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "SUM", "dmn_hit_policy_collect_sum", 1, float64(10), expectedCollectMatchedRules("SUM", float64(10)))
	})

	t.Run("COLLECT SUM aggregation returns nil output when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "SUM", "dmn_hit_policy_collect_sum", 0, interface{}(nil), nil)
	})
}

func TestBusinessRuleLocalHitPolicyCollectMinAggregation(t *testing.T) {
	t.Run("COLLECT MIN aggregation creates an incident when matched rule returns non-numeric output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationIncident(t, "MIN", "dmn_hit_policy_collect_min_incident", 1)
	})

	t.Run("COLLECT MIN aggregation creates an incident when decision table has multiple outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationMultiOutputIncident(t, "MIN", "dmn_hit_policy_collect_min_multi_output_incident", 1)
	})

	t.Run("COLLECT MIN aggregation returns the minimum matching rule output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MIN", "dmn_hit_policy_collect_min", 3, float64(-4), expectedCollectMatchedRules("MIN"))
	})

	t.Run("COLLECT MIN aggregation returns the single matching output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MIN", "dmn_hit_policy_collect_min", 1, float64(10), expectedCollectMatchedRules("MIN", float64(10)))
	})

	t.Run("COLLECT MIN aggregation returns nil output when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MIN", "dmn_hit_policy_collect_min", 0, interface{}(nil), nil)
	})
}

func TestBusinessRuleLocalHitPolicyCollectMaxAggregation(t *testing.T) {
	t.Run("COLLECT MAX aggregation creates an incident when matched rule returns non-numeric output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationIncident(t, "MAX", "dmn_hit_policy_collect_max_incident", 1)
	})

	t.Run("COLLECT MAX aggregation creates an incident when decision table has multiple outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationMultiOutputIncident(t, "MAX", "dmn_hit_policy_collect_max_multi_output_incident", 1)
	})

	t.Run("COLLECT MAX aggregation returns the maximum matching rule output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MAX", "dmn_hit_policy_collect_max", 3, float64(10), expectedCollectMatchedRules("MAX"))
	})

	t.Run("COLLECT MAX aggregation returns the single matching output", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MAX", "dmn_hit_policy_collect_max", 1, float64(10), expectedCollectMatchedRules("MAX", float64(10)))
	})

	t.Run("COLLECT MAX aggregation returns nil output when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "MAX", "dmn_hit_policy_collect_max", 0, interface{}(nil), nil)
	})
}

func TestBusinessRuleLocalHitPolicyCollectCountAggregation(t *testing.T) {
	t.Run("COLLECT COUNT aggregation creates an incident when decision table has multiple outputs", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregationIncident(t, "COUNT", "dmn_hit_policy_collect_count_incident", 1)
	})

	t.Run("COLLECT COUNT aggregation returns the number of matching rules", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "COUNT", "dmn_hit_policy_collect_count", 3, float64(3), expectedCollectMatchedRules("COUNT"))
	})

	t.Run("COLLECT COUNT aggregation returns zero when no rule matches", func(t *testing.T) {
		assertBusinessRuleLocalHitPolicyCollectAggregation(t, "COUNT", "dmn_hit_policy_collect_count", 0, float64(0), nil)
	})
}

func assertBusinessRuleLocalHitPolicyCollectAggregation(
	t *testing.T,
	aggregation string,
	decisionId string,
	score int,
	expectedDiscount any,
	expectedRules []ExpectedDecisionMatchedRule,
) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, collectHitPolicyDmnFile(aggregation))
	assert.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", decisionId, map[string]any{
		"score": score,
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discount": expectedDiscount, "score": float64(score)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
	assertHitPolicyDecisionInstance(t, processInstance.Key, expectedCollectDecisionInputs(aggregation, float64(score)), expectedRules)
}

func assertBusinessRuleLocalHitPolicyCollectAggregationIncident(t *testing.T, aggregation string, decisionId string, score int) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, collectHitPolicyIncidentDmnFile(aggregation))
	assert.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", decisionId, map[string]any{
		"score": score,
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"score": float64(score)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
	assertNoDecisionInstance(t, processInstance.Key)
}

func assertBusinessRuleLocalHitPolicyCollectAggregationMultiOutputIncident(t *testing.T, aggregation string, decisionId string, score int) {
	t.Helper()

	deployHitPolicyDmn(t, fmt.Sprintf("hit_policy_collect_%s_multi_output_incident.dmn", strings.ToLower(aggregation)))

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id.bpmn", decisionId, map[string]any{
		"score": score,
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"score": float64(score)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
	assertNoDecisionInstance(t, processInstance.Key)
}

func assertBusinessRuleLocalHitPolicyCollect(
	t *testing.T,
	score int,
	expectedDiscounts []interface{},
	expectedRules []ExpectedDecisionMatchedRule,
) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_collect.dmn")
	assert.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
		t,
		"testdata/dmn/business_rule_local_dynamic_decision_id_result.bpmn",
		"dmn_hit_policy_collect",
		map[string]any{
			"score": score,
		},
	)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discounts": expectedDiscounts, "score": float64(score)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
	assertHitPolicyDecisionInstance(t, processInstance.Key, expectedPlainCollectDecisionInputs(float64(score)), expectedRules)
}

func assertBusinessRuleLocalHitPolicyAnyNonScalarIncident(t *testing.T, customerType string) {
	t.Helper()

	deployHitPolicyDmn(t, "hit_policy_any_non_scalar.dmn")

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t, "testdata/dmn/business_rule_local_dynamic_decision_id_result_variable.bpmn", "dmn_hit_policy_any_non_scalar", map[string]any{
		"customer_type": customerType,
	})

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"customer_type": customerType})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
	assertNoDecisionInstance(t, processInstance.Key)
}

func assertBusinessRuleLocalHitPolicyRuleOrder(
	t *testing.T,
	score int,
	expectedDiscounts []interface{},
	expectedRules []ExpectedDecisionMatchedRule,
) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_rule_order.dmn")
	assert.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
		t,
		"testdata/dmn/business_rule_local_dynamic_decision_id_result.bpmn",
		"dmn_hit_policy_rule_order",
		map[string]any{
			"score": score,
		},
	)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"discounts": expectedDiscounts, "score": float64(score)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
	assertHitPolicyDecisionInstance(t, processInstance.Key, expectedRuleOrderDecisionInputs(float64(score)), expectedRules)
}

func collectHitPolicyDmnFile(aggregation string) string {
	return fmt.Sprintf("testdata/dmn/hit_policy/hit_policy_collect_%s.dmn", strings.ToLower(aggregation))
}

func collectHitPolicyIncidentDmnFile(aggregation string) string {
	return fmt.Sprintf("testdata/dmn/hit_policy/hit_policy_collect_%s_incident.dmn", strings.ToLower(aggregation))
}

func deployHitPolicyDmn(t testing.TB, fileName string) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, fmt.Sprintf("testdata/dmn/hit_policy/%s", fileName))
	require.NoError(t, err)
}

type ExpectedDecisionMatchedRule struct {
	RuleId  string
	Outputs []ExpectedDecisionOutput
}

func assertHitPolicyDecisionInstance(t *testing.T, processInstanceKey int64, expectedInputs []ExpectedDecisionInput, expectedRules []ExpectedDecisionMatchedRule) {
	t.Helper()

	decisionInstance := getDecisionInstance(t, processInstanceKey)
	require.NotNil(t, decisionInstance)
	require.Len(t, decisionInstance.EvaluatedDecisions, 1)

	evaluatedDecision := decisionInstance.EvaluatedDecisions[0]

	assertDecisionInputs(t, evaluatedDecision.Inputs, expectedInputs)
	require.NotNil(t, evaluatedDecision.MatchedRules)
	require.Equal(t, len(expectedRules), len(*evaluatedDecision.MatchedRules))

	for _, expectedRule := range expectedRules {
		assertDecisionMatchedRule(t, *evaluatedDecision.MatchedRules, expectedRule.RuleId, expectedRule.Outputs)
	}
}

func assertNoDecisionInstance(t *testing.T, processInstanceKey int64) {
	t.Helper()

	resp, err := app.restClient.GetDecisionInstancesWithResponse(t.Context(), &zenclient.GetDecisionInstancesParams{
		ProcessInstanceKey: &processInstanceKey,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)

	decisionInstancesCount := 0
	for _, partition := range resp.JSON200.Partitions {
		decisionInstancesCount += len(partition.Items)
	}
	require.Zero(t, decisionInstancesCount)
}

func expectedUniqueOrFirstDecisionInputs(amount any, customerType any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "amount",
			InputId:         "input_amount",
			InputName:       "Amount",
			InputValue:      amount,
		},
		{
			InputExpression: "customer_type",
			InputId:         "input_customer_type",
			InputName:       "Customer type",
			InputValue:      customerType,
		},
	}
}

func expectedAnyDecisionInputs(amount any, customerType any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "amount",
			InputId:         "input_amount",
			InputName:       "Amount",
			InputValue:      amount,
		},
		{
			InputExpression: "customer_type",
			InputId:         "input_customer_type",
			InputName:       "Customer Type",
			InputValue:      customerType,
		},
	}
}

func expectedRuleOrderDecisionInputs(score any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "score",
			InputId:         "input_rule_order_score",
			InputName:       "Score",
			InputValue:      score,
		},
	}
}

func expectedPlainCollectDecisionInputs(score any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "score",
			InputId:         "input_collect_score",
			InputName:       "Score",
			InputValue:      score,
		},
	}
}

func expectedAdditionalScoreDecisionInputs(key string, score any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "score",
			InputId:         fmt.Sprintf("input_%s_score", key),
			InputName:       "Score",
			InputValue:      score,
		},
	}
}

func expectedAdditionalAnyDecisionInputs(amount any, customerType any) []ExpectedDecisionInput {
	return []ExpectedDecisionInput{
		{
			InputExpression: "customer_type",
			InputId:         "input_any_multi_output_customer_type",
			InputName:       "Customer Type",
			InputValue:      customerType,
		},
		{
			InputExpression: "amount",
			InputId:         "input_any_multi_output_amount",
			InputName:       "Amount",
			InputValue:      amount,
		},
	}
}

func expectedDiscountDecisionOutput(value any) ExpectedDecisionOutput {
	return ExpectedDecisionOutput{
		OutputId:    "output_discount",
		OutputName:  "discount",
		OutputValue: value,
	}
}

func expectedAdditionalDiscountDecisionOutput(key string, value any) ExpectedDecisionOutput {
	return ExpectedDecisionOutput{
		OutputId:    fmt.Sprintf("output_%s_discount", key),
		OutputName:  "discount",
		OutputValue: value,
	}
}

func expectedAdditionalReasonDecisionOutput(key string, value any) ExpectedDecisionOutput {
	return ExpectedDecisionOutput{
		OutputId:    fmt.Sprintf("output_%s_reason", key),
		OutputName:  "reason",
		OutputValue: value,
	}
}

func expectedRuleOrderMatchedRules(discounts ...float64) []ExpectedDecisionMatchedRule {
	rules := make([]ExpectedDecisionMatchedRule, 0, len(discounts))
	for index, discount := range discounts {
		ruleNumber := index + 1
		rules = append(rules, ExpectedDecisionMatchedRule{
			RuleId: fmt.Sprintf("rule_rule_order_score_at_least_%d", ruleNumber),
			Outputs: []ExpectedDecisionOutput{
				{
					OutputId:    "output_rule_order_discount",
					OutputName:  "discount",
					OutputValue: discount,
				},
			},
		})
	}
	return rules
}

func expectedPlainCollectMatchedRules(discounts ...float64) []ExpectedDecisionMatchedRule {
	rules := make([]ExpectedDecisionMatchedRule, 0, len(discounts))
	for index, discount := range discounts {
		ruleNumber := index + 1
		rules = append(rules, ExpectedDecisionMatchedRule{
			RuleId: fmt.Sprintf("rule_collect_score_at_least_%d", ruleNumber),
			Outputs: []ExpectedDecisionOutput{
				{
					OutputId:    "output_collect_discount",
					OutputName:  "discount",
					OutputValue: discount,
				},
			},
		})
	}
	return rules
}

type additionalMultiOutputRule struct {
	suffix   string
	discount any
	reason   any
}

func expectedAdditionalCollectMultiOutputRules(rules ...additionalMultiOutputRule) []ExpectedDecisionMatchedRule {
	return expectedAdditionalMultiOutputRules("collect_multi_output", rules...)
}

func expectedAdditionalRuleOrderMultiOutputRules(rules ...additionalMultiOutputRule) []ExpectedDecisionMatchedRule {
	return expectedAdditionalMultiOutputRules("rule_order_multi_output", rules...)
}

func expectedAdditionalMultiOutputRules(key string, rules ...additionalMultiOutputRule) []ExpectedDecisionMatchedRule {
	expected := make([]ExpectedDecisionMatchedRule, 0, len(rules))
	for _, rule := range rules {
		expected = append(expected, ExpectedDecisionMatchedRule{
			RuleId: fmt.Sprintf("rule_%s_score_%s", key, rule.suffix),
			Outputs: []ExpectedDecisionOutput{
				expectedAdditionalDiscountDecisionOutput(key, rule.discount),
				expectedAdditionalReasonDecisionOutput(key, rule.reason),
			},
		})
	}
	return expected
}

func expectedCollectDecisionInputs(aggregation string, score any) []ExpectedDecisionInput {
	aggregationKey := strings.ToLower(aggregation)
	return []ExpectedDecisionInput{
		{
			InputExpression: "score",
			InputId:         fmt.Sprintf("input_collect_%s_score", aggregationKey),
			InputName:       "Score",
			InputValue:      score,
		},
	}
}

func expectedCollectMatchedRules(aggregation string, discounts ...float64) []ExpectedDecisionMatchedRule {
	aggregationKey := strings.ToLower(aggregation)
	if len(discounts) == 0 {
		discounts = []float64{10, 2.5, -4}
	}

	rules := make([]ExpectedDecisionMatchedRule, 0, len(discounts))
	for index, discount := range discounts {
		ruleNumber := index + 1
		rules = append(rules, ExpectedDecisionMatchedRule{
			RuleId: fmt.Sprintf("rule_collect_%s_score_at_least_%d", aggregationKey, ruleNumber),
			Outputs: []ExpectedDecisionOutput{
				expectedCollectDiscountDecisionOutput(aggregationKey, discount),
			},
		})
	}
	return rules
}

func expectedCollectDiscountDecisionOutput(aggregationKey string, value any) ExpectedDecisionOutput {
	return ExpectedDecisionOutput{
		OutputId:    fmt.Sprintf("output_collect_%s_discount", aggregationKey),
		OutputName:  "discount",
		OutputValue: value,
	}
}

func deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(t *testing.T, filePath string, dmnDefinitionId string, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	deployedProcessDefinition := deployUniqueDefinitionWithDmnDefinitionId(t, filePath, dmnDefinitionId)
	processInstance, err := createProcessInstance(t, &deployedProcessDefinition.Key, variables)
	require.NoError(t, err)

	return processInstance
}

func deployUniqueDefinitionWithDmnDefinitionId(t testing.TB, filePath string, dmnDefinitionId string) zenclient.ProcessDefinitionSimple {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	loc := filepath.Join(wd, filePath)

	file, err := os.ReadFile(loc)
	require.NoError(t, err)

	stringFile := string(file)
	oldDefinitionId, found := getStringInBetweenTwoString(stringFile, "bpmn:process id=\"", "\"")
	if !found {
		require.NoError(t, fmt.Errorf("didn't find bpmn process id for filename %v", filePath))
	}

	replacedDefinitionId := new(fmt.Sprintf("%v-%v", oldDefinitionId, time.Now().UnixNano()))
	fileStringWithNewProcessId := strings.ReplaceAll(stringFile, "bpmn:process id=\""+oldDefinitionId+"\"", "bpmn:process id=\""+*replacedDefinitionId+"\"")
	fileStringWithNewProcessIdAndBusinessRuleDmnDefinitionId := strings.ReplaceAll(fileStringWithNewProcessId, "replace_me", dmnDefinitionId)
	deployProcessDefinitionContent(t, filePath, []byte(fileStringWithNewProcessIdAndBusinessRuleDmnDefinitionId))

	definitions, err := listProcessDefinitions(t)
	require.NoError(t, err)

	var processDefinition zenclient.ProcessDefinitionSimple
	for _, def := range definitions {
		if def.BpmnProcessId == *replacedDefinitionId {
			processDefinition = def
			break
		}
	}

	return processDefinition
}
