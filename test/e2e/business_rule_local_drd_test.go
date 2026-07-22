package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalDrd(t *testing.T) {

	t.Run("DRD", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd.dmn")
		assert.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/dmn/drd/drd.bpmn", map[string]any{
			"amount":        1500,
			"customer_type": "VIP",
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1500), "customer_type": "VIP", "result.approval_result": "APPROVED"})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
		assertDrdDecisionInstance(t, processInstance.Key)
	})
}

func TestBusinessRuleLocalDrdIncidents(t *testing.T) {
	t.Run("DRD creates an incident when a required input is missing", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinition(t, "testdata/dmn/drd/drd.bpmn", map[string]any{
			"amount": 1500,
		})

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(1500)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule_task_determine_approval"}, nil)
		assertNoDecisionInstance(t, processInstance.Key)
	})

	t.Run("DRD creates an incident when a required decision fails", func(t *testing.T) {
		assertBusinessRuleLocalDrdIncident(t, "testdata/dmn/drd/drd_dependency_failure.dmn", "drdDependencyFailureApproval", map[string]any{"amount": 100})
	})

	t.Run("DRD creates an incident when a required decision is missing", func(t *testing.T) {
		assertBusinessRuleLocalDrdIncident(t, "testdata/dmn/drd/drd_missing_dependency.dmn", "drdMissingDependencyApproval", map[string]any{"amount": 100})
	})
}

func TestBusinessRuleLocalDrdDependencyChain(t *testing.T) {
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
}

func assertBusinessRuleLocalDrdIncident(t *testing.T, dmnFilePath string, decisionId string, variables map[string]any) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, dmnFilePath)
	require.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
		t,
		"testdata/dmn/drd/drd_dynamic_decision.bpmn",
		decisionId,
		variables,
	)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
	assertNoDecisionInstance(t, processInstance.Key)
}

func assertDrdDecisionInstance(t testing.TB, processInstanceKey int64) {
	t.Helper()

	decisionInstance := getDecisionInstance(t, processInstanceKey)
	require.NotNil(t, decisionInstance.DecisionOutput)
	assert.JSONEq(t, `{"approval_result":"APPROVED"}`, string(*decisionInstance.DecisionOutput))
	require.Len(t, decisionInstance.EvaluatedDecisions, 2)

	evaluatedByID := map[string]zenclient.EvaluatedDecision{}
	for _, evaluatedDecision := range decisionInstance.EvaluatedDecisions {
		if evaluatedDecision.DecisionId != nil {
			evaluatedByID[*evaluatedDecision.DecisionId] = evaluatedDecision
		}
	}

	riskDecision, ok := evaluatedByID["determineRiskLevel"]
	require.True(t, ok, "determineRiskLevel decision should be evaluated")
	assertDecisionInputs(t, riskDecision.Inputs, []ExpectedDecisionInput{
		{
			InputExpression: "amount",
			InputId:         "risk_input_amount",
			InputName:       "Amount",
			InputValue:      float64(1500),
		},
		{
			InputExpression: "customer_type",
			InputId:         "risk_input_customer_type",
			InputName:       "Customer type",
			InputValue:      "VIP",
		},
	})
	require.NotNil(t, riskDecision.MatchedRules)
	assertDecisionMatchedRule(t, *riskDecision.MatchedRules, "risk_rule_3", []ExpectedDecisionOutput{
		{
			OutputId:    "risk_output",
			OutputName:  "risk_level",
			OutputValue: "LOW",
		},
	})

	approvalDecision, ok := evaluatedByID["determineApprovalResult"]
	require.True(t, ok, "determineApprovalResult decision should be evaluated")
	assertDecisionInputs(t, approvalDecision.Inputs, []ExpectedDecisionInput{
		{
			InputExpression: "determineRiskLevel.risk_level",
			InputId:         "approval_input_riskLevel",
			InputName:       "Risk level",
			InputValue:      "LOW",
		},
	})
	require.NotNil(t, approvalDecision.MatchedRules)
	assertDecisionMatchedRule(t, *approvalDecision.MatchedRules, "approval_rule_1", []ExpectedDecisionOutput{
		{
			OutputId:    "approval_output",
			OutputName:  "approval_result",
			OutputValue: "APPROVED",
		},
	})
}
