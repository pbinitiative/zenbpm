package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalDrdFlowAndVariables(t *testing.T) {
	t.Run("Dependency chain evaluates all required decisions", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_dependency_chain.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/drd/drd_dynamic_decision.bpmn",
			"drdChainApproval",
			map[string]any{"amount": 100},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(100), "result.approval_result": "APPROVED"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		decisionInstance := getDecisionInstance(t, processInstance.Key)
		require.NotNil(t, decisionInstance.DecisionOutput)
		assert.JSONEq(t, `{"approval_result":"APPROVED"}`, string(*decisionInstance.DecisionOutput))
		require.Len(t, decisionInstance.EvaluatedDecisions, 3)
	})

	t.Run("Dependency chain passes null outputs", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_dependency_chain.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/drd/drd_dynamic_decision.bpmn",
			"drdChainApproval",
			map[string]any{"amount": 1000},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1000), "result.approval_result": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)

		decisionInstance := getDecisionInstance(t, processInstance.Key)
		assert.Nil(t, decisionInstance.DecisionOutput)
		require.Len(t, decisionInstance.EvaluatedDecisions, 3)

		evaluatedByID := map[string]zenclient.EvaluatedDecision{}
		for _, evaluatedDecision := range decisionInstance.EvaluatedDecisions {
			if evaluatedDecision.DecisionId != nil {
				evaluatedByID[*evaluatedDecision.DecisionId] = evaluatedDecision
			}
		}

		riskDecision, ok := evaluatedByID["drdChainRisk"]
		require.True(t, ok, "drdChainRisk decision should be evaluated")
		require.NotNil(t, riskDecision.MatchedRules)
		assert.Empty(t, *riskDecision.MatchedRules)

		reviewDecision, ok := evaluatedByID["drdChainReview"]
		require.True(t, ok, "drdChainReview decision should be evaluated")
		assertDecisionInputs(t, reviewDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "drdChainRisk.risk_level",
				InputId:         "chain_review_input_risk",
				InputName:       "Risk level",
				InputValue:      nil,
			},
		})
		require.NotNil(t, reviewDecision.MatchedRules)
		assert.Empty(t, *reviewDecision.MatchedRules)

		approvalDecision, ok := evaluatedByID["drdChainApproval"]
		require.True(t, ok, "drdChainApproval decision should be evaluated")
		assertDecisionInputs(t, approvalDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "drdChainReview.review_result",
				InputId:         "chain_approval_input_review",
				InputName:       "Review result",
				InputValue:      nil,
			},
		})
		require.NotNil(t, approvalDecision.MatchedRules)
		assert.Empty(t, *approvalDecision.MatchedRules)
	})

	t.Run("Required decision is referenced by ID when its name differs", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_required_decision_name_differs_from_id.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/drd/drd_dynamic_decision.bpmn",
			"drdApprovalByDecisionId",
			nil,
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"result.approval_result": "APPROVED"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("Acceptable rate follows the standard pricing path", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_risk_decision.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinition(
			t,
			"testdata/dmn/drd/drd_risk_decision.bpmn",
			map[string]any{
				"application": map[string]any{
					"annualIncome": float64(60000),
					"latePayments": float64(0),
				},
			},
		)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"application": map[string]any{
				"annualIncome": float64(60000),
				"latePayments": float64(0),
			},
			"interestRate": float64(4.9),
		})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"EndEvent_StandardPricing"}, []string{"EndEvent_ManualReview"})
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_ApplicationReceived",
			"Flow_Start_To_Decision",
			"BusinessRuleTask_EvaluatePricing",
			"Flow_Decision_To_Gateway",
			"Gateway_RateAcceptable",
			"Flow_StandardPricing",
			"EndEvent_StandardPricing",
		})

		decisionInstance := getDecisionInstance(t, processInstance.Key)
		require.NotNil(t, decisionInstance.DecisionOutput)
		assert.JSONEq(t, `{"interestRate":4.9}`, string(*decisionInstance.DecisionOutput))
		require.Len(t, decisionInstance.EvaluatedDecisions, 2)

		evaluatedByID := map[string]zenclient.EvaluatedDecision{}
		for _, evaluatedDecision := range decisionInstance.EvaluatedDecisions {
			if evaluatedDecision.DecisionId != nil {
				evaluatedByID[*evaluatedDecision.DecisionId] = evaluatedDecision
			}
		}

		riskDecision, ok := evaluatedByID["riskClass"]
		require.True(t, ok, "riskClass decision should be evaluated")
		assertDecisionInputs(t, riskDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "annualIncome",
				InputId:         "riskIncomeInput",
				InputName:       "Annual income",
				InputValue:      float64(60000),
			},
			{
				InputExpression: "latePayments",
				InputId:         "riskLatePaymentsInput",
				InputName:       "Late payments",
				InputValue:      float64(0),
			},
		})
		require.NotNil(t, riskDecision.MatchedRules)
		assertDecisionMatchedRule(t, *riskDecision.MatchedRules, "riskClassLowRule", []ExpectedDecisionOutput{
			{
				OutputId:    "riskClassOutput",
				OutputName:  "riskClass",
				OutputValue: "LOW",
			},
		})

		interestRateDecision, ok := evaluatedByID["interestRate"]
		require.True(t, ok, "interestRate decision should be evaluated")
		assertDecisionInputs(t, interestRateDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "riskClass.riskClass",
				InputId:         "interestRateRiskInput",
				InputName:       "Risk class",
				InputValue:      "LOW",
			},
		})
		require.NotNil(t, interestRateDecision.MatchedRules)
		assertDecisionMatchedRule(t, *interestRateDecision.MatchedRules, "interestRateLowRule", []ExpectedDecisionOutput{
			{
				OutputId:    "interestRateOutput",
				OutputName:  "interestRate",
				OutputValue: float64(4.9),
			},
		})
	})

	t.Run("High rate follows the manual review default path", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_risk_decision.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinition(
			t,
			"testdata/dmn/drd/drd_risk_decision.bpmn",
			map[string]any{
				"application": map[string]any{
					"annualIncome": float64(10000),
					"latePayments": float64(3),
				},
			},
		)
		t.Cleanup(func() {
			cleanupOwnedProcessInstance(t, processInstance.Key)
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"application": map[string]any{
				"annualIncome": float64(10000),
				"latePayments": float64(3),
			},
			"interestRate": float64(11.5),
		})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"EndEvent_ManualReview"}, []string{"EndEvent_StandardPricing"})
		assertExactProcessInstanceHistory(t, processInstance.Key, []string{
			"StartEvent_ApplicationReceived",
			"Flow_Start_To_Decision",
			"BusinessRuleTask_EvaluatePricing",
			"Flow_Decision_To_Gateway",
			"Gateway_RateAcceptable",
			"Flow_ManualReview",
			"EndEvent_ManualReview",
		})

		decisionInstance := getDecisionInstance(t, processInstance.Key)
		require.NotNil(t, decisionInstance.DecisionOutput)
		assert.JSONEq(t, `{"interestRate":11.5}`, string(*decisionInstance.DecisionOutput))
		require.Len(t, decisionInstance.EvaluatedDecisions, 2)

		evaluatedByID := map[string]zenclient.EvaluatedDecision{}
		for _, evaluatedDecision := range decisionInstance.EvaluatedDecisions {
			if evaluatedDecision.DecisionId != nil {
				evaluatedByID[*evaluatedDecision.DecisionId] = evaluatedDecision
			}
		}

		riskDecision, ok := evaluatedByID["riskClass"]
		require.True(t, ok, "riskClass decision should be evaluated")
		assertDecisionInputs(t, riskDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "annualIncome",
				InputId:         "riskIncomeInput",
				InputName:       "Annual income",
				InputValue:      float64(10000),
			},
			{
				InputExpression: "latePayments",
				InputId:         "riskLatePaymentsInput",
				InputName:       "Late payments",
				InputValue:      float64(3),
			},
		})
		require.NotNil(t, riskDecision.MatchedRules)
		assertDecisionMatchedRule(t, *riskDecision.MatchedRules, "riskClassHighRule", []ExpectedDecisionOutput{
			{
				OutputId:    "riskClassOutput",
				OutputName:  "riskClass",
				OutputValue: "HIGH",
			},
		})

		interestRateDecision, ok := evaluatedByID["interestRate"]
		require.True(t, ok, "interestRate decision should be evaluated")
		assertDecisionInputs(t, interestRateDecision.Inputs, []ExpectedDecisionInput{
			{
				InputExpression: "riskClass.riskClass",
				InputId:         "interestRateRiskInput",
				InputName:       "Risk class",
				InputValue:      "HIGH",
			},
		})
		require.NotNil(t, interestRateDecision.MatchedRules)
		assertDecisionMatchedRule(t, *interestRateDecision.MatchedRules, "interestRateHighRule", []ExpectedDecisionOutput{
			{
				OutputId:    "interestRateOutput",
				OutputName:  "interestRate",
				OutputValue: float64(11.5),
			},
		})
	})
}
