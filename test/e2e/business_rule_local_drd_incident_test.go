package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalDrdIncidents(t *testing.T) {
	t.Run("DRD creates an incident when a required input is missing", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/drd/drd_risk_decision.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/drd/drd_dynamic_decision.bpmn",
			"interestRate",
			map[string]any{"annualIncome": 60000},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"annualIncome": float64(60000)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
		assertNoDecisionInstance(t, processInstance.Key)
	})

	t.Run("DRD creates an incident when a required decision fails", func(t *testing.T) {
		assertBusinessRuleLocalDrdIncident(t, "testdata/dmn/drd/drd_dependency_failure.dmn", "drdDependencyFailureApproval", map[string]any{"amount": 100})
	})

	t.Run("DRD creates an incident when a required decision is missing", func(t *testing.T) {
		assertBusinessRuleLocalDrdIncident(t, "testdata/dmn/drd/drd_missing_dependency.dmn", "drdMissingDependencyApproval", map[string]any{"amount": 100})
	})
}

func assertBusinessRuleLocalDrdIncident(t *testing.T, dmnFilePath string, decisionID string, variables map[string]any) {
	t.Helper()

	_, err := deployDmnResourceDefinitionE2e(t, dmnFilePath)
	require.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
		t,
		"testdata/dmn/drd/drd_dynamic_decision.bpmn",
		decisionID,
		variables,
	)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
	assertNoDecisionInstance(t, processInstance.Key)
}
